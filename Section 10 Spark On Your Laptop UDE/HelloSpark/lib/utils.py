## import required requirements
from pyspark.sql import SparkSession

## Solve the Spark Session problem - local/cluster variable
def get_spark_session(env: str):
    if env == "local":
        return (
            SparkSession.builder
                # create only 2 threats only for local development
                .master("local[2]")
                .getOrCreate()
        )
    else:
        # create threats allocated by the cluster
        return SparkSession.builder.getOrCreate()

## Read data
def load_survey_df(spark, data_file):
    # simpel file sample for the example
    file_schema = """Timestamp TIMESTAMP,Age INT,Gender STRING,Country STRING,state STRING,self_employed STRING,
                family_history STRING,treatment STRING,work_interfere STRING,no_employees STRING,remote_work STRING,
                tech_company STRING,benefits STRING,care_options STRING,wellness_program STRING,seek_help STRING,anonymity STRING,
                leave STRING,mental_health_consequence STRING,phys_health_consequence STRING,coworkers STRING,
                supervisor STRING,mental_health_interview STRING,phys_health_interview STRING,mental_vs_physical STRING,
                obs_consequence STRING,comments STRING"""
    # return dataframe
    return (spark.read.format("csv")
            .option("header", "true")
            .schema(file_schema)
            .load(data_file)
        )

## Count countries
def count_by_country(survey_df):
    # filter the load_survey_df result
    return(
        survey_df.filter("Age < 40")
        .groupBy("Country")
        .count()
    )
