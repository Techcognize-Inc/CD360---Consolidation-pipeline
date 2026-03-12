from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

customer_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("op", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("status", StringType(), True),
])

account_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("op", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("account_status", StringType(), True),
])

loan_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("op", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("loan_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("loan_type", StringType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("loan_status", StringType(), True),
])