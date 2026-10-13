# PySpark Module for booking analysis

def read_booking_summary(spark):
    # define schema
    file_schema = "booked_by string, booking_date date, revenue double"

    # create a summary df
    summary_df = (
        # read the data from csv
        spark.read.format("csv")
            # skip first line as headers
            .option("header", "true")
            # use the created schema
            .schema(file_schema)
            # set the daat location
            .load("/Volumes/dev/spark_db/datasets/spark_programming/data/booking-summary.csv")
    )
    
    return summary_df

def top_3_revenue(summary_df):
    
    # import required libraries
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, rank

    # define window spec
    window_spec = (
        # analysis for each book_by group
        Window.partitionBy("booked_by")
            # order descending by revenue
            .orderBy(col("revenue").desc())
            # default aggregate loop behavior
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # create result df
    result_df = (
        # use the summary df
        summary_df
            # add new column called rank
            .withColumn("rank", rank().over(window_spec))
            # filter top 3 records
            .where("rank <= 3")
            # drop the rank column after finishing ranking data
            .drop("rank")
    )

    return result_df
    