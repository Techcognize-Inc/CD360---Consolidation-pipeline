import os
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    row_number,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    when,
    coalesce,
    lit,
    to_timestamp
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from config import (
    SPARK_WAREHOUSE_DIR,
    SPARK_LOCAL_DIR,
    LOG_DIR,
    DELTA_STAGING_CUSTOMERS,
    DELTA_STAGING_ACCOUNTS,
    DELTA_STAGING_LOANS,
    DELTA_CUSTOMER_360,
)
from logger import get_logger


# -----------------------------
# Linux / WSL environment
# -----------------------------
PROJECT_ROOT = "/mnt/c/Customer360"

os.makedirs("/tmp/spark-temp", exist_ok=True)
os.makedirs("/tmp/spark-work", exist_ok=True)

log = get_logger("customer360_consolidation", LOG_DIR)

# watermark control table path (LINUX PATH)
DELTA_WATERMARK = "/mnt/c/Customer360/data/control/watermark_config"

JOB_NAME = "customer360_consolidation"


# -----------------------------
# spark session
# -----------------------------
builder = (
    SparkSession.builder
    .appName("customer360_consolidation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
    .config("spark.local.dir", "/tmp/spark-work")

    # memory settings to avoid heap errors
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")

    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "16")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

log.info("Customer360 consolidation job started")


# -----------------------------
# helper functions
# -----------------------------
def ensure_watermark_table() -> None:

    if not DeltaTable.isDeltaTable(spark, DELTA_WATERMARK):

        log.info("Watermark table not found. Creating new watermark table.")

        seed_df = spark.createDataFrame(
            [
                ("customer360_consolidation", "customers", "1900-01-01 00:00:00"),
                ("customer360_consolidation", "accounts", "1900-01-01 00:00:00"),
                ("customer360_consolidation", "loans", "1900-01-01 00:00:00"),
            ],
            ["job_name", "source_name", "last_processed_timestamp"]
        ).withColumn(
            "last_processed_timestamp",
            to_timestamp(col("last_processed_timestamp"))
        )

        (
            seed_df.write
            .format("delta")
            .mode("overwrite")
            .save(DELTA_WATERMARK)
        )


def get_last_watermark(source_name: str) -> datetime:

    ensure_watermark_table()

    wm_df = spark.read.format("delta").load(DELTA_WATERMARK)

    row = (
        wm_df
        .filter((col("job_name") == JOB_NAME) & (col("source_name") == source_name))
        .select("last_processed_timestamp")
        .first()
    )

    if row and row["last_processed_timestamp"] is not None:
        return row["last_processed_timestamp"]

    return datetime(1900, 1, 1, 0, 0, 0)


def update_watermark(source_name: str, new_ts) -> None:

    if new_ts is None:
        log.info(f"No new watermark found for source={source_name}. Skipping watermark update.")
        return

    wm_table = DeltaTable.forPath(spark, DELTA_WATERMARK)

    source_df = spark.createDataFrame(
        [(JOB_NAME, source_name, new_ts)],
        ["job_name", "source_name", "last_processed_timestamp"]
    )

    (
        wm_table.alias("t")
        .merge(
            source_df.alias("s"),
            "t.job_name = s.job_name AND t.source_name = s.source_name"
        )
        .whenMatchedUpdate(set={
            "last_processed_timestamp": col("s.last_processed_timestamp")
        })
        .whenNotMatchedInsert(values={
            "job_name": col("s.job_name"),
            "source_name": col("s.source_name"),
            "last_processed_timestamp": col("s.last_processed_timestamp")
        })
        .execute()
    )

    log.info(f"Updated watermark for source={source_name} to {new_ts}")


# -----------------------------
# read last watermark values
# -----------------------------
customers_wm = get_last_watermark("customers")
accounts_wm = get_last_watermark("accounts")
loans_wm = get_last_watermark("loans")

log.info(f"customers watermark = {customers_wm}")
log.info(f"accounts watermark  = {accounts_wm}")
log.info(f"loans watermark     = {loans_wm}")


# -----------------------------
# read staging delta tables
# -----------------------------
customers_df = spark.read.format("delta").load(DELTA_STAGING_CUSTOMERS)
accounts_df = spark.read.format("delta").load(DELTA_STAGING_ACCOUNTS)
loans_df = spark.read.format("delta").load(DELTA_STAGING_LOANS)


# -----------------------------
# incremental filtering
# -----------------------------
customers_inc = customers_df.filter(col("event_time") > lit(customers_wm))
accounts_inc = accounts_df.filter(col("event_time") > lit(accounts_wm))
loans_inc = loans_df.filter(col("event_time") > lit(loans_wm))

log.info(f"incremental customers count = {customers_inc.count()}")
log.info(f"incremental accounts count  = {accounts_inc.count()}")
log.info(f"incremental loans count     = {loans_inc.count()}")


if customers_inc.rdd.isEmpty() and accounts_inc.rdd.isEmpty() and loans_inc.rdd.isEmpty():
    log.info("No new records found after watermark filtering.")
    spark.stop()
    raise SystemExit(0)


# -----------------------------
# latest records
# -----------------------------
cust_win = Window.partitionBy("customer_id").orderBy(col("event_time").desc_nulls_last())

customers_latest = (
    customers_inc
    .withColumn("rn", row_number().over(cust_win))
    .filter(col("rn") == 1)
    .drop("rn")
)

acct_win = Window.partitionBy("account_id").orderBy(col("event_time").desc_nulls_last())

accounts_latest = (
    accounts_inc
    .withColumn("rn", row_number().over(acct_win))
    .filter(col("rn") == 1)
    .drop("rn")
)

loan_win = Window.partitionBy("loan_id").orderBy(col("event_time").desc_nulls_last())

loans_latest = (
    loans_inc
    .withColumn("rn", row_number().over(loan_win))
    .filter(col("rn") == 1)
    .drop("rn")
)


# -----------------------------
# aggregates
# -----------------------------
account_metrics = (
    accounts_latest
    .groupBy("customer_id")
    .agg(
        count("*").alias("account_count"),
        spark_sum(coalesce(col("balance"), lit(0.0))).alias("total_account_balance"),
        avg(col("balance")).alias("avg_account_balance"),
        spark_max("event_time").alias("last_account_event_time")
    )
)

loan_metrics = (
    loans_latest
    .groupBy("customer_id")
    .agg(
        count("*").alias("loan_count"),
        spark_sum(coalesce(col("loan_amount"), lit(0.0))).alias("total_loan_amount"),
        avg(col("loan_amount")).alias("avg_loan_amount"),
        spark_max("event_time").alias("last_loan_event_time")
    )
)


# -----------------------------
# customer360 join
# -----------------------------
customer_360_df = (
    customers_latest.alias("c")
    .join(account_metrics.alias("a"), on="customer_id", how="left")
    .join(loan_metrics.alias("l"), on="customer_id", how="left")
)

log.info(f"customer_360 count = {customer_360_df.count()}")


# -----------------------------
# write curated delta table
# -----------------------------
(
    customer_360_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(DELTA_CUSTOMER_360)
)

log.info("customer_360 Delta table created successfully")


# -----------------------------
# update watermark
# -----------------------------
max_customer_ts = customers_inc.agg(spark_max("event_time").alias("mx")).first()["mx"]
max_account_ts = accounts_inc.agg(spark_max("event_time").alias("mx")).first()["mx"]
max_loan_ts = loans_inc.agg(spark_max("event_time").alias("mx")).first()["mx"]

update_watermark("customers", max_customer_ts)
update_watermark("accounts", max_account_ts)
update_watermark("loans", max_loan_ts)

log.info("Watermark update completed successfully")

spark.stop()
