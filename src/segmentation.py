import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    datediff,
    current_date,
    ntile,
    coalesce,
    lit
)
from delta import configure_spark_with_delta_pip

from config import (
    SPARK_WAREHOUSE_DIR,
    LOG_DIR,
    DELTA_CUSTOMER_360,
    DELTA_SERVING_CUSTOMER_SEGMENTS
)
from logger import get_logger


# ---------------------------------
# Linux / WSL setup
# ---------------------------------
PROJECT_ROOT = "/mnt/c/Customer360"

os.makedirs("/tmp/spark-temp", exist_ok=True)
os.makedirs("/tmp/spark-work", exist_ok=True)

log = get_logger("segmentation", LOG_DIR)


# ---------------------------------
# Spark session
# ---------------------------------
builder = (
    SparkSession.builder
    .appName("customer360_segmentation")
    .master("local[*]")
    .config("spark.local.dir", "/tmp/spark-work")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)

    # stability configs for Airflow execution
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "16")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

log.info("Segmentation job started")


# ---------------------------------
# Read curated customer360 table
# ---------------------------------
customer_360_df = spark.read.format("delta").load(DELTA_CUSTOMER_360)

log.info(f"customer_360 input count = {customer_360_df.count()}")


# ---------------------------------
# Prepare segmentation features
# ---------------------------------
seg_base = (
    customer_360_df
    .withColumn(
        "last_activity_date",
        when(
            col("last_loan_event_time").isNotNull() & col("last_account_event_time").isNotNull(),
            when(col("last_loan_event_time") > col("last_account_event_time"),
                 col("last_loan_event_time")).otherwise(col("last_account_event_time"))
        ).otherwise(
            coalesce(col("last_loan_event_time"), col("last_account_event_time"))
        )
    )
    .withColumn(
        "recency_days",
        when(
            col("last_activity_date").isNotNull(),
            datediff(current_date(), col("last_activity_date").cast("date"))
        ).otherwise(lit(9999))
    )
    .withColumn(
        "frequency_score_raw",
        coalesce(col("account_count"), lit(0)) + coalesce(col("loan_count"), lit(0))
    )
    .withColumn(
        "monetary_score_raw",
        coalesce(col("total_account_balance"), lit(0.0)) +
        coalesce(col("total_loan_amount"), lit(0.0))
    )
)

# keep only active customers if column exists
if "status" in seg_base.columns:
    seg_base = seg_base.filter(
        col("status").isNull() | (col("status") != "DELETED")
    )


# ---------------------------------
# RFM bucket scores
# ---------------------------------
from pyspark.sql.window import Window

seg_scored = (
    seg_base
    .withColumn("r_score", ntile(4).over(Window.orderBy(col("recency_days").asc())))
    .withColumn("f_score", ntile(4).over(Window.orderBy(col("frequency_score_raw").desc())))
    .withColumn("m_score", ntile(4).over(Window.orderBy(col("monetary_score_raw").desc())))
)

seg_scored = seg_scored.withColumn(
    "rfm_total_score",
    col("r_score") + col("f_score") + col("m_score")
)


# ---------------------------------
# Business segmentation
# ---------------------------------
segmented_df = (
    seg_scored
    .withColumn(
        "customer_segment",
        when(col("rfm_total_score") >= 10, lit("Platinum"))
        .when(col("rfm_total_score") >= 7, lit("Gold"))
        .when(col("rfm_total_score") >= 4, lit("Silver"))
        .otherwise(lit("Dormant"))
    )
    .withColumn(
        "engagement_flag",
        when(col("frequency_score_raw") > 0, lit("Engaged"))
        .otherwise(lit("Inactive"))
    )
    .withColumn(
        "value_flag",
        when(col("monetary_score_raw") >= 100000, lit("High Value"))
        .when(col("monetary_score_raw") >= 25000, lit("Medium Value"))
        .otherwise(lit("Low Value"))
    )
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
        "zip",
        "status",
        "account_count",
        "total_account_balance",
        "avg_account_balance",
        "loan_count",
        "total_loan_amount",
        "avg_loan_amount",
        "last_account_event_time",
        "last_loan_event_time",
        "last_activity_date",
        "recency_days",
        "frequency_score_raw",
        "monetary_score_raw",
        "r_score",
        "f_score",
        "m_score",
        "rfm_total_score",
        "customer_segment",
        "engagement_flag",
        "value_flag"
    )
)

log.info(f"segmented output count = {segmented_df.count()}")


# ---------------------------------
# Write serving Delta table
# ---------------------------------
(
    segmented_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DELTA_SERVING_CUSTOMER_SEGMENTS)
)

log.info("Customer segmentation Delta table created successfully")

spark.stop()
