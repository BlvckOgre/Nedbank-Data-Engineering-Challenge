import yaml
import logging

from pipeline.spark_session import get_spark
from pyspark.sql.functions import col, to_timestamp, concat_ws, to_date, coalesce
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



# -----------------------------
# Load Config
# -----------------------------
def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


# -----------------------------
# Read Bronze
# -----------------------------
def read_bronze(spark, path):
    return spark.read.format("delta").load(path)


# -----------------------------
# Deduplication Helper
# -----------------------------
def deduplicate(df, key_column):
    window = Window.partitionBy(key_column).orderBy(col("ingestion_timestamp").desc())

    return (
        df.withColumn("row_num", row_number().over(window))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )


# -----------------------------
# Transform Accounts
# -----------------------------
def transform_accounts(df):
    logger.info("Transforming accounts")


    df = df.select(
        col("account_id"),
        col("customer_ref"),
        col("account_type"),
        col("account_status").alias("status"), 
        col("product_tier"),
        col("open_date"),
        col("credit_limit"),
        col("current_balance"),
        col("last_activity_date"),
        col("ingestion_timestamp")
        )
    

    df = df.withColumn("open_date", to_date(col("open_date"))) \
       .withColumn("last_activity_date", to_date(col("last_activity_date"))) \
       .withColumn("credit_limit", col("credit_limit").cast("double")) \
       .withColumn("current_balance", col("current_balance").cast("double"))

    df = deduplicate(df, "account_id")

    return df


# -----------------------------
# Transform Customers
# -----------------------------
def transform_customers(df):
    logger.info("Transforming customers")

    df = df.select(
        col("customer_id"),
        col("segment"),
        col("risk_score").cast("int"),
        col("income_band"),
        col("product_flags"),
        col("kyc_status"),
        col("ingestion_timestamp")
    )

    df = deduplicate(df, "customer_id")

    return df


# -----------------------------
# Transform Transactions
# -----------------------------
def transform_transactions(df):
    logger.info("Transforming transactions")
    

    df = df.select(
        col("transaction_id"),
        col("account_id"),
        col("transaction_date"),
        col("transaction_time"),
        col("transaction_type"),
        col("merchant_category"),
        col("merchant_subcategory"),
        col("amount"),
        col("currency"),
        col("channel"),
        col("location.province").alias("province"),
        col("location.city").alias("city"),
        col("metadata.retry_flag").alias("retry_flag"),
        col("ingestion_timestamp")
        )


    df = df.withColumn(
        "transaction_date",
        coalesce(
            to_date("transaction_date", "yyyy-MM-dd"),
            to_date("transaction_date", "dd/MM/yyyy")
        )
    )

    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(concat_ws(" ", col("transaction_date"), col("transaction_time")))
    )

    # Currency normalisation 
    df = df.withColumn(
        "currency",
        when(col("currency").isin("ZAR", "zar"), "ZAR")
        .when(col("currency").isin("R", "rands", "710"), "ZAR")
        .otherwise("ZAR")
    )

    df = df.withColumn("amount", col("amount").cast("double"))
    df = df.withColumn("retry_flag", col("retry_flag").cast("boolean"))

    df = df.drop("transaction_time")

    return deduplicate(df, "transaction_id")


# -----------------------------
# Write Silver
# -----------------------------
def write_silver(df, path):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") 
        .save(path)
    )


# -----------------------------
# Main
# -----------------------------
def main():
    try:
        spark = get_spark("Silver Layer")
        config = load_config("/data/config/pipeline_config.yaml")

        bronze = config["output_paths"]["bronze"]
        silver = config["output_paths"]["silver"]

        # Load Bronze
        accounts_df = read_bronze(spark, f"{bronze}/accounts")
        customers_df = read_bronze(spark, f"{bronze}/customers")
        transactions_df = read_bronze(spark, f"{bronze}/transactions")

        accounts_df.printSchema()
        customers_df.printSchema()
        transactions_df.printSchema()

        # Transform
        accounts_clean = transform_accounts(accounts_df)
        customers_clean = transform_customers(customers_df)
        transactions_clean = transform_transactions(transactions_df)

        # Write Silver
        write_silver(accounts_clean, f"{silver}/accounts")
        write_silver(customers_clean, f"{silver}/customers")
        write_silver(transactions_clean, f"{silver}/transactions")

        logger.info("Silver transformation completed")

        spark.stop()
        return

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()