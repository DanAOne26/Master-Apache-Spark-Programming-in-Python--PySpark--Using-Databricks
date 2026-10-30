import sys
from lib.utils import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: HelloSpark {local, qa, prod} file_path: Arguments are missing")
        sys.exit(-1)

    spark = get_spark_session(sys.argv[1])
    file_path = sys.argv[2]
    survey_df = load_survey_df(spark, file_path)
    result_df = count_by_country(survey_df)
    result_df.show()
