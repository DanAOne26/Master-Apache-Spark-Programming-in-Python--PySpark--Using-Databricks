import pytest
from lib.utils import *
from pyspark.testing.utils import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    #Global Spark Session passed as fixture
    return get_spark_session("local")

@pytest.fixture(scope="session")
def survey_file():
    return "data/sample.csv"

def test_load_survey_df(spark, survey_file):
    #Actual Result
    expected_rows_count = load_survey_df(spark, survey_file).count()
    assert expected_rows_count == 9

def test_count_by_country(spark, survey_file):
    #Expected Results
    data_list = [("United States", 4), ("Canada", 2), ("United Kingdom", 1)]
    data_schema = "Country string, count long"
    expected_df = spark.createDataFrame(data_list, data_schema)

    #Actual Results
    raw_df = load_survey_df(spark, survey_file)
    actual_df = count_by_country(raw_df)

    #Assert
    assertDataFrameEqual(expected_df, actual_df)