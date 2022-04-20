from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, round
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import max, min, sum, mean, when
from pyspark.sql.types import IntegerType, DoubleType


class SparkJob:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )
        
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        
        self.input_directory = "datasets/psycon/solona-usdt-to-20220-4-historical-data"
        
    def stop(self):
        """Stop the Spark Session"""
        self.spark.stop()

    def extract_timeseries(self):
        # Read in dataframe
        dataset = "sol-usdt"
        df = self.spark.read.option("header", True).csv(
            f"../{self.input_directory}/{dataset}.csv"
        )
        # df.printSchema()
        return df

    def preprocessing_timeseries(self):
        df = self.extract_timeseries()

        # Rename columns.
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

        # Fix string datatype into
        double_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "tb_base_volume",
            "tb_quote_volume",
            "ignore",
        ]
        for col in double_cols:
            df = df.withColumn(col, df[col].cast(IntegerType()))

        print("preprocessing completed.")
        # df.printSchema()
        return df

    def solona_head(self):
        """Head of Alias."""
        df = self.preprocessing_timeseries()
        df = df.alias("s")
        df.printSchema()
        df.show(10)

    def solona_aliased(self):
        """Alias the dataset."""
        df = self.preprocessing_timeseries()
        df = df.alias("s")
        df.printSchema()
        df.show(10)

    def solona_counts(self):
        """Provide the shape of the dataset."""
        df = self.preprocessing_timeseries()
        print(f"count: {df.count()}, {len(df.columns)}")

    def solona_describe(self):
        """Describe the dataset."""
        df = self.preprocessing_timeseries()
        df.describe().show()

        
    # GROUP BY
        
    def solona_groupby_avg(self):
        """Groupby and return max value of all columns."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        df = df.groupBy("month").max()
        df.show(4)

    def solona_groupby_mean_single(self):
        """Groupby mean single column."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        # Groupby: mean sinlge column
        df = df.groupBy("month").mean("open")
        df.show(4)

    def solona_groupby_min_multiple(self):
        """Groupby get mins from dataset."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        # Grouby: min multiple columns.
        df = df.groupBy("month").min("open", "close")
        df.show(4)

    def solona_groupby_custom(self):
        """Groupby with custom aggregation."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        # Groupby: Custom
        df = df.groupBy("month").agg(
            max("open").alias("max_open"),
            min("close").alias("min_close"),
            sum("volume").alias("sum_volume"),
            mean("number_of_trades").alias("mean_number_of_trades"),
        )

        # Sort/Orderby
        df = df.sort(df.mean_number_of_trades.desc(), df.max_open.asc())

        df.show(4)
        
        
    # DISTINCT

    def solona_months_distinct(self):
        """Distinct Operation"""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        df = df.select("month").distinct()
        df.show()
    
    
    # OTHERWISE
    
    def solona_when(self):
        """When Operation"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, 1).otherwise(0).alias("case1"))
        return df
    
    
    def solona_multiple_when(self):
        """Multilpe When Operation with different Conditions"""
        df = self.preprocessing_timeseries()
        df = df.select("volume",
               "open",
               when((df.volume > 5000) | (df.volume < 1000) , 1).otherwise(0).alias("case1"),
               when((df.volume > 2500) & (df.open == 3) , 1).otherwise(0).alias("case2")
                )
        return df
    
    
    # LIKE, StartsWith, EndsWith
    
    def solona_like(self):
        """Like Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.like("%not%").alias('likecase1'))
        return df
    
    def solona_startswith(self):
        """StartWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.startswith("not").alias('startswithcase1'))
        return df
    
    def solona_endswith(self):
        """EndsWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.endswith("normal").alias('endswithcase1'))
        return df

    def solona_endswith(self):
        """EndsWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.endswith("normal").alias('endswithcase1'))
        return df
    
    
    # DROP
    
    def solona_drop(self):
        """Drop Columns"""
        df = self.preprocessing_timeseries()
        df = df.drop("open","close")
        return df
    
    # NULLS
    
    def solona_replace(self):
        """Replace values"""
        df = self.preprocessing_timeseries()
        df = df.replace(3,3000)
        return df
    
    def solona_nulls(self):
        """Replace values with nulls"""
        df = self.preprocessing_timeseries()
        # df = df.replace(3,None)
        return df
    
    
        
    # FILTER AND UNION
    
    def solona_split_dataframes(self):
        """Split Dataframes"""
        df = self.preprocessing_timeseries()
        df = df.withColumn("month", month(df["open_time"]))
        df1 = df.filter(df.month == 1).select("open", "high", "low", "close")
        df2 = df.filter(df.month == 2).select("open", "high", "low", "close")
        df3 = df.filter(df.month == 3).select("open", "high", "low", "close")
        print(f"d1: counts: {df1.count()}")
        print(f"d2: counts: {df2.count()}")
        print(f"d3: counts: {df3.count()}")
        return df1, df2, df3

    def solona_union_dataframes(self):
        """Union Dataframes"""
        df1, df2, df3 = self.solona_split_dataframes()
        print(f"d1: counts: {df1.count()}")
        print(f"d2: counts: {df2.count()}")
        print(f"d3: counts: {df3.count()}")
        df123 = df1.union(df2).union(df3)
        print(f"d123: counts: {df123.count()}")
        df123.show(10)
        
        
    # TEMP VIEWS
    
    def solona_temp_view(self):
        """Create a Temp View of the dataset"""
        df = self.extract_timeseries()
        df.createOrReplaceTempView("solona")
        self.spark.sql("select * from solona").show(3)

        
    # SPARK DATAFRAME TO OTHER DATA STRUCTURES
    
    def solona_to_pandas(self):
        """Spark Dataframe to Pandas Dataframe"""
        df = self.extract_timeseries()
        pdf = df.toPandas()
        return pdf
        
        
    def solona_to_rdd(self):
        """Spark Dataframe to RDD"""
        df = self.extract_timeseries()
        rdd = df.rdd
        return rdd
    
    
    def solona_to_json(self):
        """Spark Dataframe to JSON"""
        df = self.extract_timeseries()
        json = df.toJSON().first()
        return json
    
    # EXPLAIN
    
    def solona_explain(self):
        """Explain Spark Plan"""
        df = self.preprocessing_timeseries()
        df.explain()