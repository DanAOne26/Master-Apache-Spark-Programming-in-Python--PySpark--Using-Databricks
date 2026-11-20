# Section 10 Spark On Your Laptop UDE Notes

## Content
49. [Setup your Laptop for Spark Development](#49-setup-your-laptop-for-spark-development)
50. [Developing Spark Application in the IDE](#50-developing-spark-application-in-the-ide)
51. [Spark Content - Remote Cluster Connection from Your IDE](#51-spark-content---remote-cluster-connection-from-your-ide)


filename.py

```python


```

Run the file
    terminal --> python filename.py

Result:



<img src="pics/name.png" width="800" />
<br>
<br>



## 49. Set up your Laptop for Spark Development

[⬆ Back to content](#content)

### Requirements for Spark 4.01

Install all requirements below to be able to work with Apache spark locally:

1. JDK 17/21
2. Python 3.10+
   - Python 3.11 for Windows. 
     - Check for the current supported python by spark. Lower version is more secure and avoid unexpecte errors.
     - Make sure that Python is added to system environment PATH
3. Spark Binaries 
   - https://spark.apache.org/downloads.html 
     - Choose a Spark release: choose one version older version for example (4.0.1), so no open bugs can be active
     - Choose a package type: Pre-built for Apache Hadop 3.4 and later
     - Download Spark: spark-4.0.1-bin-hadoop3.tgz - click on the file
   - Link with the exact version should appear. Click and download the file - We suggest the following location for your download: Link
   - Unzip the file and rename it to shorter name like 'spark-4'
   - Move spark-4 directory not deep in structure on your PC, but in more simple path like C:\spark\spark-401. Long directory paths may cause errors.
  
4. Hadoop WinUtils
   - https://github.com/cdarlint/winutils - we must find the hadoop version utils we downloaded spark for - Hadop 3.4 in this case or the latest possible like hadoop-3.3.6/bin
   - download it as ZIP and unzip it. Copy the latest or most close version folder from the unzipped versions and move it into the C:\spark directory but not in spark-401 folder. Rename the hadoop utils folder to 'winutils'.

5. Environment Variables
   - SPARK_HOME
     - go to user environment variables and create new variable: variable name: SPARK_HOME variable value: C:\spark\spark-401
     - edit path user nevironment variable and set new '%SPARK_HOME%\bin'
     - OK/OK to save the changes
  
   - HADOOP_HOME
     - go to user environment variables and create new variable: variable name: HADOOP_HOME variable value: C:\spark\winutils
     - edit path user nevironment variable and set new '%HADOOP_HOME%\bin'
     - OK/OK to save the changes
  
   - PYSPARK_PYTHON - location of Python Spark will use
     - Check where is python installed: terminal --> where is python : C:\Users\<user_name>\AppData\Local\Programs\Python\Python311\python.exe
     - go to user environment variables and create new variable: variable name: PYSPARK_PYTHON variable value: C:\Users\<user_name>\AppData\Local\Programs\Python\Python311\python.exe
     - OK/OK to save the changes

   - PYTHONPATH - C:\spark\spark-401\python:C:\spark\spark-401\python\lib\py4j-x.xx.x.x-src.zip
     - go to user environment variables and create new variable: variable name: PYTHONPATH variable value: C:\spark\spark-401\python:C:\spark\spark-401\python\lib\py4j-x.xx.x.x-src.zip
     - OK/OK to save the changes

Check if Pyspark can run on the PC
    terminal --> pyspark
    This should start a spark session on your PC

    Check the version
    terminal --> spark.version
    '4.0.1'

    Exit the session
    terminal --> exit()

Now we are conffident that spark will work in our IDE

6. IDE

[⬆ Back to content](#content)


## 50. Developing Spark Application in the IDE

[⬆ Back to content](#content)

Configure IDE to work with Spark

Requirements:

Create a Spark Application as the following   
1. Reads data from a survey.csv datafile    
2. Computes the analysis results as shown below   

Country       |      count    
United States |         4   
Canada        |         2   
United Kindom |         1   

Looks like what we want to do is read data from this survey.csv file, create a data frame and perform a group by count analysis on that data frame. And then print that result or produce that result.

Before we start trying to do this, let's try to understand some basic complications or problems while working on your local machine. In this case we will use Python 3.11 for Windows.

Problem:
- Developing on local machine
- Deploy on cluster

1. Spark session (Local/Cluster)    
So the first problem is is Spark session for local machine is different and is Spark session for the cluster is different. So how do we handle that?
While developing our application and testing it on my laptop, I should be creating a local Spark session and when we go and deploy our application on the cluster, we should be creating a Spark session for the cluster.
Solution: It's just a variable. If you are doing it on local, variable value is 'local'. If you're doing it on the cluster, the variable value will be 'cluster'.

2. Data File Source (Local/Cluster)   
Which data file to read and what is the location of the data file is another variable that we have to manage.
One small consideration here is that on the cluster you might have the actual data file, which is huge, which could be huge. But locally we need only small data file for development.

### Development checks    
First sanity check - Make sure to choose the same Python version which you have configured with the Spark Home on your local machine.   
Second sanity check - make sure taht 'pyspark' package is installed included in the pyspark-home package.

### The Development   
1. We must prepare the project folder - HelloSpark in the current session   
2. We want to do two things, load the data, create a data frame, and then perform the analysis, desired analysis to produce the result. Only two things.    

Create utils.py

```python
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
```


Create HellowSpark.py

```python
# import reqired libraries
import sys
from lib.utils import *

if __name__ == "__main__":
    # if there are agument different than 3, exit with error
    if len(sys.argv) != 3:
        print("Usage: HelloSpark {local, qa, prod} file_path: Arguments are missing")
        sys.exit(-1)
    # create spark session with env name
    spark = get_spark_session(sys.argv[1])
    # load file path
    file_path = sys.argv[2]
    # Call the functions with the required arguments
    survey_df = load_survey_df(spark, file_path)
    result_df = count_by_country(survey_df)
    # show the result, show() is used in local environment
    result_df.show()
```

Run the file and check the result. it should be the same as in the requirements.


Create a local unit testing file test_hello_spark.py

```python
# import libraries
import pytest
from lib.utils import *
from pyspark.testing.utils import assertDataFrameEqual

@pytest.fixture(scope="session")
def spark():
    # Global Spark Session passed as fixture
    return get_spark_session("local")

@pytest.fixture(scope="session")
def survey_file():
    return "data/sample.csv"

def test_load_survey_df(spark, survey_file):
    # Actual Result
    expected_rows_count = load_survey_df(spark, survey_file).count()
    assert expected_rows_count == 9

def test_count_by_country(spark, survey_file):
    # Expected Results
    data_list = [("United States", 4), ("Canada", 2), ("United Kingdom", 1)]
    data_schema = "Country string, count long"
    expected_df = spark.createDataFrame(data_list, data_schema)

    #Actual Results
    raw_df = load_survey_df(spark, survey_file)
    actual_df = count_by_country(raw_df)

    # Assert
    assertDataFrameEqual(expected_df, actual_df)
```



[⬆ Back to content](#content)




## 51. Spark Content - Remote Cluster Connection from Your IDE

[⬆ Back to content](#content)

Spark on local machine

1. JDK 17/21
2. Python 3.10+
   - Python 3.11 for Windows
3. 


[⬆ Back to content](#content)




