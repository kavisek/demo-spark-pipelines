from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, round
from pyspark.sql.types import IntegerType, DoubleType


class SparkJob:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )


job = SparkJob()
