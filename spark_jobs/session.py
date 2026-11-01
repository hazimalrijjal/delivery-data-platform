"""Create the local Spark session used by the jobs."""
from __future__ import annotations


KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0"      # must match the Spark and Scala versions of pyspark==4.3.0


def get_spark(app: str = "delivery-platform", delta: bool = False, kafka: bool = False):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName(app)
        .config("spark.sql.session.timeZone", "UTC")        # all timestamps are read and shown as UTC
        .config("spark.sql.shuffle.partitions", "4")        # small data: the default 200 partitions would only add overhead
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
    )
    if kafka:
        # Downloaded from Maven the first time, so this needs internet access.
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    if delta:
        # Delta Lake needs its jars; this downloads them from Maven the first time, so it needs internet access.
        from delta import configure_spark_with_delta_pip
        builder = (builder
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        return configure_spark_with_delta_pip(builder).getOrCreate()
    return builder.getOrCreate()
