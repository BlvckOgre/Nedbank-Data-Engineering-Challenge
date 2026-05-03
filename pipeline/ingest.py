import sys
import os
import yaml
import logging
from datetime import datetime

from pipeline.spark_session import get_spark
from pyspark.sql.functions import current_timestamp, lit




# -----------------------------
# Logging Setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# 
# Spark Session
#
#def get_spark():
#    spark = (
#       SparkSession.builder
#        .appName("Bronze Ingestion Layer")
#        .getOrCreate()
#    )
#    return spark


# -----------------------------
# Load Config
# -----------------------------
def load_config(config_path):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


# -----------------------------
# Ingest CSV
# -----------------------------
def ingest_csv(spark, input_path, source_name):
    logger.info(f"Ingesting CSV: {source_name}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)  # IMPORTANT: no inference in Bronze
        .csv(input_path)
    )

    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source", lit(source_name))

    return df


# -----------------------------
# Ingest JSON
# -----------------------------
def ingest_json(spark, input_path, source_name):
    logger.info(f"Ingesting JSON: {source_name}")

    df = (
        spark.read
        .option("multiLine", False)
        .json(input_path)
    )

    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source", lit(source_name))

    return df


# -----------------------------
# Write Bronze Layer
# -----------------------------
def write_bronze(df, output_path, source_name):
    logger.info(f"Writing Bronze data for {source_name}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("source")
        .save(f"{output_path}/{source_name}")
    )


# -----------------------------
# Main Execution
# -----------------------------
#transactions_df = ingest_json(
#    spark,
#    input_paths["transactions"],
#    "transactions"
#)

def resolve_transactions_path(config_path):
    if os.path.exists("/data/input/transactions.jsonl"):
        return "/data/input/transactions.jsonl"
    elif os.path.exists("/data/input/transactions.json"):
        return "/data/input/transactions.json"
    else:
        raise FileNotFoundError(f"Transactions file not found. Checked: {config_path}")

def main():
    try:
        spark = get_spark("Bronze Ingestion Layer")

        config = load_config("/data/config/pipeline_config.yaml")

        input_paths = config["input_paths"]
        logger.info(f"Transactions path from config: {input_paths['transactions']}")
        bronze_output = config["output_paths"]["bronze"]

        # Ingest Accounts
        accounts_df = ingest_csv(
            spark,
            input_paths["accounts"],
            "accounts"
        )
        write_bronze(accounts_df, bronze_output, "accounts")

        # Ingest Customers
        customers_df = ingest_csv(
            spark,
            input_paths["customers"],
            "customers"
        )
        write_bronze(customers_df, bronze_output, "customers")

        # Ingest Transactions
        #transactions_df = ingest_json(
        #    spark,
         #   input_paths["transactions"],
         #   "transactions"
        #)
        resolved_path = resolve_transactions_path(input_paths["transactions"])
        logger.info(f"Resolved transactions path: {resolved_path}")

        transactions_df = ingest_json(
            spark,
            resolved_path,
            "transactions"
        )   
        
        write_bronze(transactions_df, bronze_output, "transactions")

        logger.info("Bronze ingestion completed successfully")

        spark.stop()
        return

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()