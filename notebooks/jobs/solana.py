from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, round
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import max, min, sum, mean, when, udf
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame as DataFrame
from pandas.core.frame import DataFrame as PandasDataframe

    
class SparkJob:
    def __init__(self):
        self.spark = (
            SparkSession.builder.appName("Python Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
        )
        
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        
        self.input_directory = "datasets/psycon/solana-usdt-to-20220-4-historical-data"
        
    def stop(self) -> None:
        """Stop the Spark Session"""
        self.spark.stop()

    def extract_timeseries(self) -> DataFrame:
        # Read in dataframe
        dataset = "sol-usdt"
        df = self.spark.read.option("header", True).csv(
            f"../{self.input_directory}/{dataset}.csv"
        )
        # df.printSchema()
        return df

    def preprocessing_timeseries(self) -> DataFrame:
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

    def solana_head(self) -> None:
        """Head of Alias."""
        df = self.preprocessing_timeseries()
        df.printSchema()
        df.show(10)

    def solana_aliased(self) -> None:
        """Alias the dataset."""
        df = self.preprocessing_timeseries()
        df = df.alias("s")
        df.printSchema()
        df.show(10)

    def solana_counts(self) -> None:
        """Provide the shape of the dataset."""
        df = self.preprocessing_timeseries()
        print(f"count: {df.count()}, {len(df.columns)}")

    def solana_describe(self) -> None:
        """Describe the dataset."""
        df = self.preprocessing_timeseries()
        df.describe().show()

        
    # GROUP BY
        
    def solana_groupby_avg(self) -> None:
        """Groupby and return max value of all columns."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        df = df.groupBy("month").max()
        df.show(4)

    def solana_groupby_mean_single(self) -> None:
        """Groupby mean single column."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        # Groupby: mean sinlge column
        df = df.groupBy("month").mean("open")
        df.show(4)

    def solana_groupby_min_multiple(self) -> None:
        """Groupby get mins from dataset."""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        # Grouby: min multiple columns.
        df = df.groupBy("month").min("open", "close")
        df.show(4)

    def solana_groupby_custom(self) -> None:
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

    def solana_months_distinct(self) -> None:
        """Distinct Operation"""
        df = self.preprocessing_timeseries()

        # Create new column.
        df = df.withColumn("month", month(df["open_time"]))

        df = df.select("month").distinct()
        df.show()
    
    
    # OTHERWISE
    
    def solana_when(self) ->  DataFrame:
        """When Operation"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, 1).otherwise(0).alias("case1"))
        return df
    
    
    def solana_multiple_when(self) ->  DataFrame:
        """Multilpe When Operation with different Conditions"""
        df = self.preprocessing_timeseries()
        df = df.select("volume",
               "open",
               when((df.volume > 5000) | (df.volume < 1000) , 1).otherwise(0).alias("case1"),
               when((df.volume > 2500) & (df.open == 3) , 1).otherwise(0).alias("case2")
                )
        return df
    
    
    # Like, StartsWith, EndsWith
    
    def solana_like(self) ->  DataFrame:
        """Like Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.like("%not%").alias('likecase1'))
        return df
    
    def solana_startswith(self) ->  DataFrame:
        """StartWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.startswith("not").alias('startswithcase1'))
        return df
    
    def solana_endswith(self) ->  DataFrame:
        """EndsWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.endswith("normal").alias('endswithcase1'))
        return df

    def solana_endswith(self) ->  DataFrame:
        """EndsWith Operation on String Colunmns"""
        df = self.preprocessing_timeseries()
        df = df.select("open",when(df.open == 3, "normal").otherwise("not normal").alias("case1"))
        df = df.select("open", "case1", df.case1.endswith("normal").alias('endswithcase1'))
        return df
    
    
    # DROP
    
    def solana_drop(self) ->  DataFrame:
        """Drop Columns"""
        df = self.preprocessing_timeseries()
        df = df.drop("open","close")
        return df
    
    # NULLS
    
    def solana_replace(self) ->  DataFrame:
        """Replace values"""
        df = self.preprocessing_timeseries()
        df = df.replace(3,3000)
        return df
    
    def solana_nulls(self):
        """Replace values with nulls."""
        df = self.preprocessing_timeseries()
        df = df.replace(3,None)
        return df
    
    def solana_fill_nulls(self) ->  DataFrame:
        """Replace values with nulls."""
        df = self.solana_nulls()
        df = df.fillna(1000)
        return df
    

    def solana_drop_nulls(self) ->  DataFrame:
        """Drop Rows with Null based on subset."""
        df = self.solana_nulls()
        print(f'row count: {df.count()}')
        df = df.dropna(subset=["open","close"])
        print(f'row count: {df.count()}')
        return df
    
    
        
    # FILTER AND UNION
    
    def solana_split_dataframes(self):
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

    def solana_union_dataframes(self) -> None:
        """Union Dataframes"""
        df1, df2, df3 = self.solana_split_dataframes()
        print(f"d1: counts: {df1.count()}")
        print(f"d2: counts: {df2.count()}")
        print(f"d3: counts: {df3.count()}")
        df123 = df1.union(df2).union(df3)
        print(f"d123: counts: {df123.count()}")
        df123.show(10)
        
        
    # TEMP VIEWS
    
    def solana_temp_view(self) -> None:
        """Create a Temp View of the dataset"""
        df = self.extract_timeseries()
        df.createOrReplaceTempView("solana")
        self.spark.sql("select * from solana").show(3)

    
    # SPARK DATAFRAME TO OTHER DATA STRUCTURES
    
    def solana_to_pandas(self) -> PandasDataframe:
        """Spark Dataframe to Pandas Dataframe"""
        df = self.extract_timeseries()
        pdf = df.toPandas()
        return pdf
        
        
    def solana_to_rdd(self) -> RDD:
        """Spark Dataframe to RDD"""
        df = self.extract_timeseries()
        rdd = df.rdd
        return rdd
    
    
    def solana_to_json(self) -> str:
        """Spark Dataframe to JSON"""
        df = self.extract_timeseries()
        json = df.toJSON().first()
        return json
    
    
    # EXPLAIN
    
    def solana_explain(self) -> None:
        """Explain Spark Plan"""
        df = self.preprocessing_timeseries()
        df.explain()
        
        
    # REPARTITION
    
    def solana_repartition(self) -> int:
        """Repartition Dataframe"""
        df = self.preprocessing_timeseries()
        print(f'partitions = {df.rdd.getNumPartitions()}')
        df = df.repartition(10)
        print(f'partitions = {df.rdd.getNumPartitions()}')
        return df.rdd.getNumPartitions()
        
        
    # JOINS
    
    def solana_alias(self, alias: str) -> DataFrame:
        """Alias dataframe"""
        df = self.preprocessing_timeseries()
        df = df.alias(alias)
        return df
    
    
    def solana_alias_show(self) -> None:
        """Select for aliased Dataframe"""
        df = self.solana_alias(alias='a')
        df.printSchema()
        df.select('open_time').show(10)
        df.select('a.open_time').show(10)
        
        
    def solana_alias_join(self) -> DataFrame:
        """Alias to dataframes"""
        
        # Alias dataframes.
        dfa = self.solana_alias(alias='a')
        dfb = self.solana_alias(alias='b')
        
        # Print counts.
        print(f'count: {dfa.count()}')
        print(f'count: {dfb.count()}')
        
        # Test some selects out.
        dfa = dfa.select('a.open_time','a.open', 'a.close')
        dfb = dfb.select('b.open_time','b.close')

        # Join dataframes.
        dfc = dfa.join(dfb, on='open_time', how='left')
        print(f'count: {dfc.count()}')
        dfc.show(10)

        # Select on final dataframes.
        dfc = dfc.select('a.open_time','a.open','b.close')
        dfc.show(10)
        return dfc
    
    def solana_realias_join(self) -> DataFrame:
        """
        Realias dataFrame.
        
        >> dfc.select('c.open_time','c.open','c.close').show(10)
        """
        
        print('attempting realias')
        dfc = self.solana_alias_join()
        print(f'count: {dfc.count()}')
        dfc = dfc.alias('c')
        print(f'count: {dfc.count()}')
        return dfc
    
    
    # UDFs
    
    def solana_udf(self) -> DataFrame:
        """Creating a UDF and with Spark '.' syntaxs """
        df = self.preprocessing_timeseries()
        
        # Create a UDF
        neg = udf(lambda x: -abs(x), IntegerType())
        
        # Use the UDF.
        df = df.select("open", neg("open"), neg("open").alias('neg'))
        
        return df 
    
    
    def solana_sql_udf(self) -> None:
        """Using UDF on SQL."""
        df = self.preprocessing_timeseries()
        
        def neg(x:int) -> int:
            """ int to negative int """
            return -abs(x)
    
        self.spark.udf.register("neg", neg ,  IntegerType())
        df.createOrReplaceTempView("data")
        self.spark.sql("select open, neg(open) as neg_open from data").show(10)
    
    