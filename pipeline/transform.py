import sys
import yaml
import logging

from pipeline.spark_session import get_spark
from pyspark.sql.functions import greatest

from pyspark.sql.window import Window
from pyspark.sql.functions import count

from pyspark.sql.functions import (
    col, to_timestamp, concat_ws,
    row_number, to_date, when,
    coalesce, from_unixtime
)

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

# ---------------------------
# DQ Function
# ---------------------------
def apply_dq_rules(df, rules):
    from pyspark.sql.functions import col, when, lit

    # NOT NULL checks
    if "not_null" in rules:
        for c in rules["not_null"]:
            df = df.withColumn(
                f"{c}_null_flag",
                when(col(c).isNull(), 1).otherwise(0)
            )

    # VALID VALUES
    if "valid_values" in rules:
        for c, values in rules["valid_values"].items():
            df = df.withColumn(
                f"{c}_invalid_flag",
                when(~col(c).isin(values), 1).otherwise(0)
            )

    # NUMERIC RANGE
    if "numeric_ranges" in rules:
        for c, bounds in rules["numeric_ranges"].items():
            df = df.withColumn(
                f"{c}_out_of_range_flag",
                when(
                    (col(c) < bounds["min"]) | (col(c) > bounds["max"]),
                    1
                ).otherwise(0)
            )

    return df


# -----------------------------
# Transform Accounts
# -----------------------------
def transform_accounts(df, dq_rules):
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
    df = apply_dq_rules(df, dq_rules["accounts"])

    

    return df


# -----------------------------
# Transform Customers
# -----------------------------
def transform_customers(df, dq_rules):
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
    df = apply_dq_rules(df, dq_rules["customers"])

    return df


# -----------------------------
# Transform Transactions
# -----------------------------
def transform_transactions(df, dq_rules):
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

    # Create timestamp

    df = df.withColumn(
        "transaction_date_clean",
        coalesce(
        to_date(col("transaction_date"), "yyyy-MM-dd"),
        to_date(col("transaction_date"), "dd/MM/yyyy"),
        to_date(from_unixtime(col("transaction_date")))
    )
    )

    df = df.withColumn(
        "date_flag",
        when(col("transaction_date_clean").isNull(), 1).otherwise(0)
    )

    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(concat_ws(" ", col("transaction_date_clean"), col("transaction_time")))
    )

    df = df.drop("transaction_date").withColumnRenamed("transaction_date_clean", "transaction_date")


    df = df.withColumn("transaction_date", to_date(col("transaction_date")))
    df = df.drop("transaction_time")
    df = df.withColumn("amount_type_flag", when(col("amount").cast("string").rlike("^[0-9.]+$"), 0).otherwise(1))
    df = df.withColumn("amount", col("amount").cast("double"))
    df = df.withColumn("currency_variant_flag", when(~col("currency").isin("ZAR", "zar"), 1).otherwise(0))

    # Currency normalisation 
    df = df.withColumn(
        "currency",
        when(col("currency").isin("ZAR", "zar"), "ZAR")
        .when(col("currency").isin("R", "rands", "710"), "ZAR")
        .otherwise("ZAR")
    )

    df = df.withColumn(
        "retry_flag",
        when(col("retry_flag").isin(True, "true", "True", 1), True).otherwise(False)
    )

    dup_window = Window.partitionBy("transaction_id")

    df = df.withColumn(
        "duplicate_count",
        count("*").over(dup_window)
    )

    df = df.withColumn(
        "duplicate_flag",
        when(col("duplicate_count") > 1, 1).otherwise(0)
    )

    df = deduplicate(df, "transaction_id")
    df = apply_dq_rules(df, dq_rules["transactions"])
    

    df = df.withColumn(
        "dq_flag",
        when(col("transaction_id_null_flag") == 1, "NULL_REQUIRED")
        .when(col("amount_out_of_range_flag") == 1, "TYPE_MISMATCH")
        .when(col("transaction_type_invalid_flag") == 1, "TYPE_MISMATCH")
        .when(col("amount_type_flag") == 1, "TYPE_MISMATCH")
        .when(col("currency_variant_flag") == 1, "CURRENCY_VARIANT")
        .when(col("date_flag") == 1, "DATE_FORMAT")
        .when(col("duplicate_flag") == 1, "DUPLICATE_DEDUPED")
        .otherwise(None)
    )


    return df


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
        dq_rules = load_config("/data/config/dq_rules.yaml")

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
        accounts_clean = transform_accounts(accounts_df, dq_rules)
        customers_clean = transform_customers(customers_df, dq_rules)
        transactions_clean = transform_transactions(transactions_df, dq_rules)

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
