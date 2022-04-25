import pytest

from jobs.solana import SparkJob
from pyspark.sql.dataframe import DataFrame as DataFrame
from pyspark.sql.functions import lit, col, round
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import max, min, sum, mean, when, udf, rand, avg, count


# PYTEST EXAMPLES
# THE following are example of test your can have for your pyspark dataframes.
# This is not an extensive list, but a sample of components you can test.
# (i.e columns, udfs, datatype, distributions) and testing concepts
# (i.e dataframe fixtures, and parameterized fixtures)


def test_dummy() -> None:
    """Dummyp Test"""
    pass


# Simple Fixture Test
@pytest.fixture
def sample_df() -> DataFrame:
    """Create dummy dataframe fixture."""
    job = SparkJob()
    df = job.create_dataframe()
    return df


@pytest.fixture
def timeseries_df() -> DataFrame:
    """Create solana timeseries dataframe fixture."""
    job = SparkJob()
    df = job.extract_timeseries()
    return df


def test_sample_df_columns(sample_df: DataFrame) -> None:
    """Test sample df columns"""
    columns = [
        "language",
        "users_count",
    ]
    return sample_df.columns == columns


def test_timeseries_df_columns(timeseries_df: DataFrame) -> None:
    """Test the colunmns within our kaggle dataset have not changed."""

    columns = [
        "Open Time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close Time",
        "Quote Asset Volume",
        "Number of Trades",
        "TB Base Volume",
        "TB Quote Volume",
        "Ignore",
    ]

    assert timeseries_df.columns == columns, "columns don't match"


# Parameterized Fixture Tests
# Take in an parameter and generate a dataframe.


@pytest.fixture(params=[-1, -2, -3])
def select_df(request) -> DataFrame:
    """Parameterized DataFrame"""
    job = SparkJob()
    df = job.create_dataframe()
    df = df.withColumn("user_counts", lit(request.param))
    return df


def test_select_df(select_df: DataFrame) -> None:
    """Check if type is DataFrame"""
    assert type(select_df) == DataFrame, "select_df is not dataframe"


# UDF TEST
