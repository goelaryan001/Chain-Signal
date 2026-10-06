"""Local SparkSession factory used by the feature-engineering jobs."""
import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "chainsignal") -> SparkSession:
    # Spark starts Python worker processes with whatever "python3" is first on PATH
    # (Python 3.8 on this machine), not the interpreter running the job. Pin both
    # driver and workers to this interpreter.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "4g")
        # the default 200 shuffle partitions is sized for clusters; ~2M rows locally needs few
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
