import pytest

from jobs.solana import SparkJob


def test_dummy():
    """Dummyp Test"""
    pass


def test_solana_columns():
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

    job = SparkJob()
    df = job.extract_timeseries()
    assert df.columns == columns
    
