# Section 7 Joins in Spark Dataframe Notes

## Content
36. [Introduction to Joins in Spark](#36-introduction-to-joins-in-spark)
37. [Inner Joins](#37-inner-joins)
38. [Outer Joins](#38-outer-joins)
39. [Lateral Join](#39-lateral-join)
40. [Other Types of Joins](#40-other-types-of-joins)


filename.py

```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



## 36. Introduction to Joins in Spark

[⬆ Back to content](#content)

We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH07-Spark Joins/01-Assinment Data Preparation notebook

Run all code sections once so that data is loaded, write code is written, it will execute and all the three tables will be created.


### Join types in Apache Spark
  1. Inner Join - Return only the rows that have matching values on both sides
  2. Outer Joins
    - Left - Returns all rows from the left side and matching rows from the right side
    - Right - Returns all rows from the right side and matching rows from the left side
    - Full - Return all matching and non-matching rows from both sides
  3. Natural Join - Automatically create join criteria on the same column names (Applies to Inner and Outer Joins)
  4. Cross Join - Join without any join criteria (all possible combinations)
  5. Self Join - Join a table with itself (Applies to Inner, Outer, and Cross Joins)
  6. Semi Join - Take records from the left side when it matches with the right side - Simply checks a condition. If the record is found on the right side, then return the records from the left side.
  7. Anti Join - Take records from the left side when it does not match with the right side - Antijoin is opposite to semi join. Take records from the left side when it does not match with the right side semi join will take when it matches with the right side and anti join will take when it does not match with the right side.
  Semi and anti join are also known as left semi join or left anti join. There is no right semi join or right anti join supported in spark.
  8. Lateral Join - Each row from the driving table is used to subquery the derived table - So there is one driving table we can think of it as a left table and right table is the derived table. So lateral join will query the derived table using some values in the driving table.



### 1. Load data from files and create the following tables.
  1. facilities -> facilities.csv
  2. members -> members.csv
  3. bookings -> bookings.csv
   
Choose appropriate data types to best represent the data fields.

<img src="pics/spark-joins-26-1-1.png" width="800" />
<br>
<br>

These are the fields and these are related. So facilities table has a key facility ID, which is related with the bookings in the facility. ID and members table has got a key called member ID, which is also related to the member ID in the bookings.

these three tables represent a club where there are facilities like they have lawns, they have party halls, they have pool tables. And uh, there are members of that club. Members can make bookings for the facilities and bookings can be made in slots. And each slot has a start time. Each slot has a fixed duration.



### 2. Define schema
   
```python
facilities_schema = """facility_id INT, facility_name STRING, member_cost DOUBLE, guest_cost DOUBLE, 
    initial_outlay DOUBLE, monthly_maintainance DOUBLE"""

members_schema = """member_id INT, last_name STRING, first_name STRING, address STRING, zip_code STRING, 
    telephone STRING, recommended_by STRING, joining_date DATE"""

bookings_schema = "booking_id INT, facility_id INT, member_id INT, start_time TIMESTAMP, slots INT"
```



### 3. Load facilities table

```python
facilities_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(facilities_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/facilities.csv")
)

facilities_df.write.mode("overwrite").saveAsTable("dev.spark_db.facilities")
```


### 4. Load members table

```python
members_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(members_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/members.csv")
)

members_df.write.mode("overwrite").saveAsTable("dev.spark_db.members")
```





### 5. Load bookings table

```python
bookings_df = (
    spark.read.format("csv")
        .option("header", "true")
        .schema(bookings_schema)
        .load("/Volumes/dev/spark_db/datasets/spark_programming/data/bookings.csv")
)

bookings_df.write.mode("overwrite").saveAsTable("dev.spark_db.bookings")
```

<img src="pics/name.png" width="800" />
<br>
<br>


[⬆ Back to content](#content)



## 37. Inner Joins

[⬆ Back to content](#content)

We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH07-Spark Joins/02-Inner Joins notebook

We should have executed the first notebook from this section - 01-Assinment Data Preparation so we have requierd tables for the lobas.


### Q1. Prepare a facility booking reporting dataset as the following.

member_id | first_name | last_name | facility_id | slots | start_time
---------------------------------------------------------------------------
The report must meet the following criteria.    
- Facility bookings made by a person whose last name is Smith
- He has booked more than 5 slots in a single booking
- Report should be sorted by first name of the member in ascending order and number of slots in descending order



#### 1.1 Try answering with SQL

  - Join Expression - on m.member_id = b.member_id
  - Join type - inner join
  - Column name ambiguity - if columns exist in more than one table we need to spcify the table we want the values from

These three things are also applicable when working with the data frame.

```sql
-- select specific columns with name ambiguity and resulted join column: m.member_id
select m.member_id, first_name, last_name, facility_id, slots, start_time
-- set the join criteria and alias. Join expression is: on m.member_id = b.member_id, Join type: inner join
from dev.spark_db.bookings as b inner join dev.spark_db.members as m on m.member_id = b.member_id
-- specify the additional name condition
where m.last_name = "Smith" and b.slots > 5
-- set ascending and descending order for the result join
order by m.first_name asc, b.slots desc
```

<img src="pics/spark-joins-37-1-1-1.png" width="1000" />
<br>
<br>



1.2 Try answering with dataframe api

  - Join Expression
  - Join type
  - Column name ambiguity

```python
from pyspark.sql.functions import expr, col

members_df = spark.table("dev.spark_db.members").alias("m")
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

#join_expr = expr("m.menber_id == b.member_id")
join_expr = col("m.member_id") == col("b.member_id")

reports_df = (
    members_df.join(bookings_df, join_expr, "inner")
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.slots", "b.start_time")
        .orderBy("m.first_name", col("b.slots").desc())
)

reports_df.display()
```

<img src="pics/spark-joins-37-1-1-2.png" width="400" />
<br>
<br>

<img src="pics/spark-joins-37-1-2-3.png" width="1000" />
<br>
<br>



### Q2. Show me a facility bookings report as the following.

member_id | first_name | last_name | facility_name | slots | booking_amount | start_time    
--------------------------------------------------------------------------------------------

The report must meet the following criteria.
  1. Facility bookings made by a person whose last name is Smith
  2. He has booked more than 5 slots in a single booking
  3. Report should be sorted by first name of the member in ascending order and booking amount in descending order

```python
from pyspark.sql.functions import col, expr

bookings_df = spark.table("dev.spark_db.bookings").alias("b")
members_df = spark.table("dev.spark_db.members").alias("m")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

report_df = (
    bookings_df
        .join(members_df, expr("b.member_id == m.member_id"), "inner")
        .join(facilities_df, col("b.facility_id") == col("f.facility_id"), "inner")
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .selectExpr("m.member_id", "m.first_name", "m.last_name", "f.facility_name", "b.slots",
            "b.slots * f.member_cost as booking_amount", "b.start_time")
        .orderBy(col("m.first_name").asc(),
                 col("booking_amount").desc())
)

report_df.display()
```

<img src="pics/spark-joins-37-2-1.png" width="400" />
<br>
<br>


<img src="pics/spark-joins-37-2-2.png" width="1000" />
<br>
<br>


[⬆ Back to content](#content)



## 38. Outer Joins

[⬆ Back to content](#content)

We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH07-Spark Joins/ notebook



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>

```python


```

<img src="pics/name.png" width="800" />
<br>
<br>




```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>




```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



[⬆ Back to content](#content)



## 39. Lateral Join

[⬆ Back to content](#content)


We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH07-Spark Joins/ notebook



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>


```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>





```python


```

<img src="pics/name.png" width="800" />
<br>
<br>





[⬆ Back to content](#content)



## 40. Other Types of Joins

[⬆ Back to content](#content)


We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH07-Spark Joins/ notebook





```python


```

<img src="pics/name.png" width="800" />
<br>
<br>




```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>




```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



```python


```

<img src="pics/name.png" width="800" />
<br>
<br>





[⬆ Back to content](#content)


