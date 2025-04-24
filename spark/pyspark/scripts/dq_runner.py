import json
from pyspark.sql import SparkSession

import logging
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'Modules'))
import dq_checks

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DQRunner")

# Load Config
config_path = "config/dq_config.json"
with open(config_path) as f:
    config = json.load(f)

# Spark Init
spark = SparkSession.builder.appName("DataQualityFramework").getOrCreate()

# Load Data
dataset = config["dataset"]
format = dataset["format"]
df = None

if format == "csv":
    df = spark.read.option("header", dataset.get("header", True)).csv(dataset["input_path"])
elif format == "parquet":
    df = spark.read.parquet(dataset["input_path"])
elif format == "json":
    df = spark.read.json(dataset["input_path"])
else:
    logger.error("Unsupported format")
    sys.exit(1)

logger.info(f"Data loaded from {dataset['input_path']}")

# Run Checks
results = {}

for check in config["checks"]:
    ctype = check["type"]
    if ctype == "null_check":
        res = dq_checks.null_check(df, check["columns"], check["threshold"])
    elif ctype == "duplicate_check":
        res = dq_checks.duplicate_check(df, check["columns"])
    elif ctype == "value_range":
        res = dq_checks.value_range_check(df, check["column"], check["min"], check["max"])
    elif ctype == "distinct_count":
        res = dq_checks.distinct_count_check(df, check["column"])
    else:
        res = {ctype: {"status": "UNKNOWN"}}
    results.update(res)

# Log Results
logger.info("=== DATA QUALITY RESULTS ===")
for check, res in results.items():
    logger.info(f"{check}: {res}")

spark.stop()
