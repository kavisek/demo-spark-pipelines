from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round
from pyspark.sql.types import DoubleType, IntegerType


class SparkJob:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )


job = SparkJob()
