import os

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
    lit
)
from delta import configure_spark_with_delta_pip

from config import (
    HADOOP_HOME_DIR,
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
# local windows env
# -----------------------------
os.environ["HADOOP_HOME"] = HADOOP_HOME_DIR
os.environ["hadoop.home.dir"] = HADOOP_HOME_DIR
os.environ["PATH"] = rf"{HADOOP_HOME_DIR}\bin;" + os.environ["PATH"]

os.makedirs(r"C:\spark-temp", exist_ok=True)
os.makedirs(r"C:\spark-work", exist_ok=True)

log = get_logger("customer360_consolidation", LOG_DIR)

# final curated path
DELTA_CUSTOMER_360 = "file:///C:/Customer360/data/curated/customer_360"

builder = (
    SparkSession.builder
    .appName("customer360_consolidation")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
    .config("spark.local.dir", SPARK_LOCAL_DIR)
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

log.info("Customer360 consolidation job started")

# -----------------------------
# read staging delta tables
# -----------------------------
customers_df = spark.read.format("delta").load(DELTA_STAGING_CUSTOMERS)
accounts_df = spark.read.format("delta").load(DELTA_STAGING_ACCOUNTS)
loans_df = spark.read.format("delta").load(DELTA_STAGING_LOANS)

# -----------------------------
# latest customer record
# -----------------------------
cust_win = Window.partitionBy("customer_id").orderBy(col("event_time").desc_nulls_last())

customers_latest = (
    customers_df
    .withColumn("rn", row_number().over(cust_win))
    .filter(col("rn") == 1)
    .drop("rn")
)

# -----------------------------
# latest account record
# -----------------------------
acct_win = Window.partitionBy("account_id").orderBy(col("event_time").desc_nulls_last())

accounts_latest = (
    accounts_df
    .withColumn("rn", row_number().over(acct_win))
    .filter(col("rn") == 1)
    .drop("rn")
)

# keep only active accounts if needed
accounts_latest = accounts_latest.filter(
    (col("account_status").isNull()) |
    (~col("account_status").isin("DELETED")))

# -----------------------------
# latest loan record
# -----------------------------
loan_win = Window.partitionBy("loan_id").orderBy(col("event_time").desc_nulls_last())

loans_latest = (
    loans_df
    .withColumn("rn", row_number().over(loan_win))
    .filter(col("rn") == 1)
    .drop("rn")
)

loans_latest = loans_latest.filter(
    (col("loan_status").isNull()) | (~col("loan_status").isin("DELETED", "CLOSED")))

# -----------------------------
# account aggregates by customer
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

# -----------------------------
# loan aggregates by customer
# -----------------------------
loan_metrics = (
    loans_latest
    .groupBy("customer_id")
    .agg(
        count("*").alias("loan_count"),
        spark_sum(coalesce(col("loan_amount"), lit(0.0))).alias("total_loan_amount"),
        avg(col("loan_amount")).alias("avg_loan_amount"),
        spark_max("event_time").alias("last_loan_event_time"),
    )
)

# -----------------------------
# customer_360 join
# -----------------------------
customer_360_df = (
    customers_latest.alias("c")
    .join(account_metrics.alias("a"), on="customer_id", how="left")
    .join(loan_metrics.alias("l"), on="customer_id", how="left")
    .select(
        col("customer_id"),
        col("c.event_uid").alias("customer_event_uid"),
        col("c.event_time").alias("customer_event_time"),
        col("first_name"),
        col("last_name"),
        col("email"),
        col("phone"),
        col("city"),
        col("state"),
        col("zip"),
        col("status").alias("customer_status") if "status" in customers_latest.columns else lit(None).alias("customer_status"),
        col("source_system"),
        col("account_count"),
        col("total_account_balance"),
        col("avg_account_balance"),
        col("loan_count"),
        col("total_loan_amount"),
        col("avg_loan_amount"),
        col("last_account_event_time"),
        col("last_loan_event_time"),
        when(col("account_count") > 0, lit("Y")).otherwise(lit("N")).alias("has_account"),
        when(col("loan_count") > 0, lit("Y")).otherwise(lit("N")).alias("has_loan")
    )
)

# fill null numeric columns
customer_360_df = (
    customer_360_df
    .fillna({
        "account_count": 0,
        "total_account_balance": 0.0,
        "avg_account_balance": 0.0,
        "loan_count": 0,
        "total_loan_amount": 0.0,
        "avg_loan_amount": 0.0
    })
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

spark.stop()