"""Local SparkSession factory used by the feature-engineering jobs."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "chainsignal") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
