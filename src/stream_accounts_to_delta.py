import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, to_timestamp
from delta import configure_spark_with_delta_pip

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ACCOUNTS,
    DELTA_STAGING_ACCOUNTS,
    DELTA_DEAD_LETTER_ACCOUNTS,
    CHK_ACCOUNTS,
    CHK_DEAD_LETTER_ACCOUNTS,
    HADOOP_HOME_DIR,
    SPARK_WAREHOUSE_DIR,
    SPARK_LOCAL_DIR,
    LOG_DIR
)

from schemas import account_schema
from logger import get_logger


os.environ["HADOOP_HOME"] = HADOOP_HOME_DIR
os.environ["hadoop.home.dir"] = HADOOP_HOME_DIR
os.environ["PATH"] = rf"{HADOOP_HOME_DIR}\bin;" + os.environ["PATH"]

os.makedirs(r"C:\spark-temp", exist_ok=True)
os.makedirs(r"C:\spark-work", exist_ok=True)


log = get_logger("stream_accounts", LOG_DIR)


builder = (
    SparkSession.builder
    .appName("stream_accounts_to_delta")
    .master("local[*]")
    .config(
        "spark.jars",
        ",".join([
            r"C:\spark-extra-jars\spark-sql-kafka-0-10_2.12-3.5.1.jar",
            r"C:\spark-extra-jars\spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
            r"C:\spark-extra-jars\kafka-clients-3.4.1.jar",
            r"C:\spark-extra-jars\commons-pool2-2.11.1.jar",
            r"C:\spark-extra-jars\jsr305-3.0.0.jar",
        ])
    )
    .config("spark.driver.extraClassPath", r"C:\spark-extra-jars\*")
    .config("spark.executor.extraClassPath", r"C:\spark-extra-jars\*")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DIR)
    .config("spark.local.dir", SPARK_LOCAL_DIR)
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_ACCOUNTS)
    .option("startingOffsets", "latest")
    .load()
)


base = raw.selectExpr(
    "CAST(value AS STRING) as value",
    "timestamp as kafka_ts",
    "topic",
    "partition",
    "offset"
).withColumn("event_uid", expr("concat(topic,'-',partition,'-',offset)"))

parsed = base.withColumn(
    "json_data",
    from_json(col("value"), account_schema)
)


bad = parsed.filter(col("json_data").isNull()).select(
    "value", "kafka_ts", "event_uid"
)


good = (
    parsed.filter(col("json_data").isNotNull())
    .select("event_uid", "kafka_ts", "json_data.*")
    .withColumn("event_time", to_timestamp(col("event_time")))  
)

bad_query = (
    bad.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", DELTA_DEAD_LETTER_ACCOUNTS)
    .option("checkpointLocation", CHK_DEAD_LETTER_ACCOUNTS)
    .start()
)

good_query = (
    good.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", DELTA_STAGING_ACCOUNTS)
    .option("checkpointLocation", CHK_ACCOUNTS)
    .start()
)

log.info("Account stream started.")
spark.streams.awaitAnyTermination()