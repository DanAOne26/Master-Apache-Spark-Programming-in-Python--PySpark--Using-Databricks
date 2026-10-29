import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from booking_analysis import read_booking_summary, top_3_revenue

@pytest.fixture(scope="session")
def spark_session():
  return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="session")
def spark():
    # Global Spark session passed as ficsture
    return SparkSession.builder.getOrCreate()

def test_read_booking_summary(spark):
    # Get actual results
    summary_df = read_booking_summary(spark)
    records_loaded = summary_df.count()
    # We know that the expected results are 58, so we check the count
    assert records_loaded == 58

def test_top_3_revenue(spark):
    # Get actual results
    summary_df = read_booking_summary(spark)
    result_df = top_3_revenue(summary_df)
    # Get the expected results
    file_schema = "booked_by STRING, booking_date DATE, revenue DOUBLE"
    expected_df = (
        spark.read.format("csv")
            .option("header", "true")
            .schema(file_schema)
            .load("/Volumes/dev/spark_db/datasets/spark_programming/data/top-3-days-test-data.csv")
    )
    # Assert - Compare the actual and expected results
    assertDataFrameEqual(result_df, expected_df)

