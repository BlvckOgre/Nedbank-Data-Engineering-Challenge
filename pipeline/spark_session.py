from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark(app_name: str):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark