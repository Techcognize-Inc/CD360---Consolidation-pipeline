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
    HADOOP_HOME_DIR,
    SPARK_WAREHOUSE_DIR,
    SPARK_LOCAL_DIR,
    LOG_DIR,
    DELTA_CUSTOMER_360,
    DELTA_SERVING_CUSTOMER_SEGMENTS
)
from logger import get_logger


# ---------------------------------
# Windows / local setup
# ---------------------------------
os.environ["HADOOP_HOME"] = HADOOP_HOME_DIR
os.environ["hadoop.home.dir"] = HADOOP_HOME_DIR
os.environ["PATH"] = f"{HADOOP_HOME_DIR}\\bin;" + os.environ["PATH"]

os.makedirs(r"C:\spark-temp", exist_ok=True)
os.makedirs(r"C:\spark-work", exist_ok=True)

log = get_logger("segmentation", LOG_DIR)


# ---------------------------------
# Spark session
# ---------------------------------
builder = (
    SparkSession.builder
    .appName("customer360_segmentation")
    .master("local[*]")
    .config("spark.local.dir", SPARK_LOCAL_DIR)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
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

# Optional: keep only active/non-deleted customers if your curated table has status
if "customer_status" in seg_base.columns:
    seg_base = seg_base.filter(
        col("customer_status").isNull() | (col("customer_status") != "DELETED")
    )


# ---------------------------------
# RFM bucket scores
# ---------------------------------
# Lower recency_days is better, so ascending
# Higher frequency and monetary are better, so descending
seg_scored = (
    seg_base
    .withColumn("r_score", ntile(4).over(
        __import__("pyspark.sql").sql.Window.orderBy(col("recency_days").asc())
    ))
    .withColumn("f_score", ntile(4).over(
        __import__("pyspark.sql").sql.Window.orderBy(col("frequency_score_raw").desc())
    ))
    .withColumn("m_score", ntile(4).over(
        __import__("pyspark.sql").sql.Window.orderBy(col("monetary_score_raw").desc())
    ))
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
        "customer_status",
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
# Write to serving Delta table
# ---------------------------------
(
    segmented_df.write
    .format("delta")
    .mode("overwrite")
    .save(DELTA_SERVING_CUSTOMER_SEGMENTS)
)

log.info("Customer segmentation Delta table created successfully")

spark.stop()