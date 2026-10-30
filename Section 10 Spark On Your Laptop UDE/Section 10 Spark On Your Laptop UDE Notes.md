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
   - Pytho 3.11 for Windows. 
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





[⬆ Back to content](#content)




## 51. Spark Content - Remote Cluster Connection from Your IDE

[⬆ Back to content](#content)

### Subheader



[⬆ Back to content](#content)




