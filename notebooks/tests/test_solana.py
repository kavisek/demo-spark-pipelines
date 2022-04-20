import pytest

from jobs.solana import SparkJob
from pyspark.sql.dataframe import DataFrame as DataFrame

def test_dummy() -> None:
    """Dummyp Test"""
    pass



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


def test_sample_df_columns(sample_df) -> None:
    """ Test sample df columns"""
    columns = [
        "language",
        "users_count",
    ]
    return sample_df.columns == columns


def test_timeseries_df_columns(timeseries_df) -> None:
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

    
    




    