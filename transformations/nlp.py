import logging
import os

from pandas.core.frame import DataFrame as PandasDataframe
from pyspark.ml.feature import NGram, RegexTokenizer
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType


class SparkJob:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

        self.input_directory = "datasets/word_count/"
        self.output_directory = "output"


    def remove_special_characters(self, dataframe: DataFrame) -> DataFrame:
        """
        Remove special characters.
        
        Keep alpha numeric characters and ("," & "'"). Remove "-", "+" ,".", etc 
        """
        dataframe = dataframe.select(
            regexp_replace(col("value"),"[^A-Za-z']"," ").alias("value")
            )

        return dataframe

    def tokenized_counts(self, dataframe: DataFrame) -> DataFrame:
        """Tokenize corpos and aggregate word counts"""

        # Tokenizer
        regex_tokenizer = RegexTokenizer(inputCol="value", outputCol="words")
        dataframe = regex_tokenizer.transform(dataframe)

        # GroupBy Aggregration
        dataframe = dataframe.withColumn("word", explode(col("words"))) \
            .groupBy("word") \
            .agg(count("*").alias('count')) \
            .sort('word', ascending=True)

        return dataframe

    def preprocessing_words(self) -> DataFrame:
        """Read in corpus and process the data."""
        df = self.spark.read.text(f'../{self.input_directory}/word_count.txt')
        return df


    def word_count(self) -> DataFrame:
        """Read Text File"""
        df = self.preprocessing_words()
        df = self.remove_special_characters(df)
        df = self.tokenized_counts(df)

        return df


    def ngrams(self) -> None:
        """Compute N-Grams"""
        df = self.preprocessing_words()

        # Split strings into arrays.
        df = df.select(split('value', pattern='').alias('value'))

        # 
        ngram = NGram(n=2, inputCol='value', outputCol='grams')
        ngram.transform(df).head(10)


