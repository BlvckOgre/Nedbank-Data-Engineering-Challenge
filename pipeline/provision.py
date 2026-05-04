import sys
import yaml
import logging

from pipeline.spark_session import get_spark
from pyspark.sql.functions import (
    col, month
)
from pyspark.sql.functions import count, sum, avg
from pyspark.sql.functions import when
from dq_report import DQReport

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
# Read Silver
# -----------------------------
def read_silver(spark, path):
    return spark.read.format("delta").load(path)


# -----------------------------
# Build Gold Layer
# -----------------------------
def build_gold(transactions, accounts, customers):
    logger.info("Building Gold layer")

    # Join transactions -> accounts
    df = transactions.join(
        accounts,
        on="account_id",
        how="left"
    )

    df = df.withColumn(
        "dq_flag",
        when(col("account_type").isNull(), "ORPHANED_ACCOUNT")
        .otherwise(col("dq_flag"))
    )

    # Join -> customers
    df = df.join(
        customers,
        accounts.customer_ref == customers.customer_id,
        "left"
    )

    # Aggregation
    customer_metrics = transactions.groupBy("account_id").agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_spent"),
        avg("amount").alias("avg_transaction")
    )

    df = df.join(customer_metrics, on="account_id", how="left")

    # Retry Flag
    df = df.withColumn(
        "high_retry_flag",
        when(col("retry_flag"), 1).otherwise(0)
    )

    # High value transactions
    df = df.withColumn(
        "high_value_txn_flag",
        (col("amount") > 5000).cast("int")
    )
    
    # Potential fraud
    df = df.withColumn(
        "potential_fraud_flag",
        ((col("high_retry_flag") == 1) & (col("amount") > 2000)).cast("int")
    )

    # Composite fraud signal
    df = df.withColumn(
        "fraud_risk_flag",
        (
            (col("high_retry_flag") == 1) &
            (col("high_value_txn_flag") == 1)
        ).cast("int")
    )


    # Derived fields 
    df = df.withColumn( 
        "transaction_month", 
        month(col("transaction_timestamp"))
    )

    

    # Fraud Score 
    df = df.withColumn(
    "fraud_score",
    (
        (col("high_retry_flag") * 30) +
        (col("high_value_txn_flag") * 25) +
        (col("potential_fraud_flag") * 20) +
        (col("transaction_count") / 10) +
        (col("avg_transaction") / 1000)
    )
    )

    # Fraud Risk Level

    df = df.withColumn(
        "fraud_risk_level",
        when(col("fraud_score") >= 70, "HIGH")
        .when(col("fraud_score") >= 40, "MEDIUM")
        .otherwise("LOW")
    )

    # Final selection 
    df = df.select(
        col("transaction_id"),
        col("account_id"),
        col("customer_id"),
        col("transaction_timestamp"),
        col("transaction_month"),
        col("merchant_category"),
        col("merchant_subcategory"),
        col("amount"),
        col("currency"),
        col("transaction_type"),
        col("channel"),
        col("province"),
        col("city"),
        col("account_type"),
        col("segment"),
        col("transaction_count"),
        col("total_spent"),
        col("avg_transaction"),
        col("dq_flag"),
        col("high_retry_flag"),
        col("high_value_txn_flag"),
        col("potential_fraud_flag"),
        col("fraud_risk_flag"),
        col("fraud_score"),
        col("fraud_risk_level"),
        col("ingestion_timestamp")
    )

    return df


# -----------------------------
# Write Gold
# -----------------------------
def write_gold(df, path):
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
        spark = get_spark("Gold Layer")
        config = load_config("/data/config/pipeline_config.yaml")

        silver = config["output_paths"]["silver"]
        gold = config["output_paths"]["gold"]
        dq = DQReport()

        # Load Silver
        transactions_df = read_silver(spark, f"{silver}/transactions")
        accounts_df = read_silver(spark, f"{silver}/accounts")
        customers_df = read_silver(spark, f"{silver}/customers")

        dq.set_source_counts(
            customers_df.count(),
            accounts_df.count(),
            transactions_df.count()
        )

        # Build Gold
        gold_df = build_gold(
            transactions_df,
            accounts_df,
            customers_df
        )

        gold_df.cache() 

        dq.set_gold_count(gold_df.count())

        dq.add_issue("NULL_REQUIRED", gold_df.filter(col("dq_flag") == "NULL_REQUIRED").count(), "FLAGGED")
        dq.add_issue("TYPE_MISMATCH", gold_df.filter(col("dq_flag") == "TYPE_MISMATCH").count(), "FLAGGED")
        dq.add_issue("DATE_FORMAT", gold_df.filter(col("dq_flag") == "DATE_FORMAT").count(), "FLAGGED")
        dq.add_issue("ORPHANED_ACCOUNT", gold_df.filter(col("dq_flag") == "ORPHANED_ACCOUNT").count(), "FLAGGED")
        dq.add_issue("CURRENCY_VARIANT", gold_df.filter(col("currency_variant_flag") == 1).count(),"NORMALISED")
        dq.add_issue("DUPLICATE_DEDUPED", gold_df.filter(col("dq_flag") == "DUPLICATE_DEDUPED").count(),"FLAGGED")

        # Write Gold
        write_gold(gold_df, f"{gold}/fact_transactions")

        logger.info("Gold layer created successfully")

        dq.save()

        spark.stop()
        return
    

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
