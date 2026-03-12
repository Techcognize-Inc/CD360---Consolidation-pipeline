from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_CUSTOMERS = "cdc.customers"
TOPIC_ACCOUNTS = "cdc.accounts"
TOPIC_LOANS = "cdc.loans"

BASE_DATA_DIR = (PROJECT_ROOT / "data").resolve()
BASE_CHECKPOINT_DIR = (PROJECT_ROOT / "checkpoints").resolve()

DELTA_STAGING_CUSTOMERS = (BASE_DATA_DIR / "delta_staging" / "customers").as_uri()
DELTA_STAGING_ACCOUNTS = (BASE_DATA_DIR / "delta_staging" / "accounts").as_uri()
DELTA_STAGING_LOANS = (BASE_DATA_DIR / "delta_staging" / "loans").as_uri()

DELTA_CUSTOMER_360 = (BASE_DATA_DIR / "curated" / "customer_360").as_uri()

DELTA_SERVING_CUSTOMER_SEGMENTS = (BASE_DATA_DIR / "serving" / "customer_segments").as_uri()

DELTA_DEAD_LETTER_CUSTOMERS = (BASE_DATA_DIR / "delta_staging" / "dead_letter" / "customers").as_uri()
DELTA_DEAD_LETTER_ACCOUNTS = (BASE_DATA_DIR / "delta_staging" / "dead_letter" / "accounts").as_uri()
DELTA_DEAD_LETTER_LOANS = (BASE_DATA_DIR / "delta_staging" / "dead_letter" / "loans").as_uri()

CHK_CUSTOMERS = (BASE_CHECKPOINT_DIR / "customers").as_uri()
CHK_ACCOUNTS = (BASE_CHECKPOINT_DIR / "accounts").as_uri()
CHK_LOANS = (BASE_CHECKPOINT_DIR / "loans").as_uri()

CHK_DEAD_LETTER_CUSTOMERS = (BASE_CHECKPOINT_DIR / "dead_letter" / "customers").as_uri()
CHK_DEAD_LETTER_ACCOUNTS = (BASE_CHECKPOINT_DIR / "dead_letter" / "accounts").as_uri()
CHK_DEAD_LETTER_LOANS = (BASE_CHECKPOINT_DIR / "dead_letter" / "loans").as_uri()

HADOOP_HOME_DIR = r"C:\Hadoop"
SPARK_WAREHOUSE_DIR = "file:///C:/spark-temp"
SPARK_LOCAL_DIR = r"C:\spark-work"

LOG_DIR = str((PROJECT_ROOT / "logs").resolve())