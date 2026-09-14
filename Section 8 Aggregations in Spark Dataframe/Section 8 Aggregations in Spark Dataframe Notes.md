# Section 8 Aggregations in Spark Dataframe Notes

## Content
41. [Simple Aggregation](#41-simple-aggregation)
42. [Grouping Aggregation](#42-grouping-aggregation)
43. [Multilevel Aggregation](#43-multilevel-aggregation)
44. [Windowing Aggregation](#44-windowing-aggregation)


```python


```

<img src="pics/name.png" width="800" />
<br>
<br>



## 41. Simple Aggregation

[⬆ Back to content](#content)


We should have imported all required files in section 9. Setup Your Hands-On Environment by executing the spark_programming.dbc notebook.

Login to Databricks, connect to serverless cluster and open CH08-Spark Aggregates/01-Simple Aggregation notebook


### Introduction to Aggregation

Aggregation in Spark are implemented using functions

**Comonly used aggregate functions**

    1. count(*), count(expr), count(DISTINCT expr)      
    2. min(expr), max(expr), avg(expr), sum(expr)       

**Types of Aggregation**

    1. Simple Aggregation
    2. Grouped Aggregation
    3. Multilevel Aggregation
    4. Window Aggregation

**Requirement - Analysis Data Set**     
Prepare club bookings dataset for analysis      
|booking_id|member_name|facility_name|start_time|booking_amount|



```python
bookings_df = spark.table("dev.spark_db.bookings")
facilities_df = spark.table("dev.spark_db.facilities")
members_df = spark.table("dev.spark_db.members")

club_bookings_df = (
    bookings_df.join(facilities_df, "facility_id")
            .join(members_df, "member_id", "left")
            .selectExpr("booking_id",
                        "case when member_id==0 then 'Guest Member' else concat_ws(' ', first_name, last_name) end as member_name",
                        "facility_name","start_time",
                        "case when member_id == 0 then slots * guest_cost else slots * member_cost end as booking_amount")            
)

club_bookings_df.display()
```

<img src="pics/aggregations-41-0-1.png" width="300" />
<img src="pics/aggregations-41-0-2.png" width="300" />
<br>
<br>

<img src="pics/aggregations-41-0-3.png" width="800" />
<br>
<br>


### 1. Calculate total earnings and average booking value.

#### 1.1 Using sql like expressions

```python
result_df = (
    club_bookings_df.selectExpr("sum(booking_amount) as total_earning",
                                "avg(booking_amount) as avg_booking_value")
)

result_df.display()
```

<img src="pics/aggregations-41-1-1-1.png" width="400" />
<br>
<br>



#### 1.2 Using column expressions


```python
from pyspark.sql.functions import sum, avg

result_df = (
    club_bookings_df.select(sum("booking_amount").alias("total_earning"),
                            avg("booking_amount").alias("avg_booking_value"))
)

result_df.display()
```

<img src="pics/aggregations-41-1-2-1.png" width="400" />
<br>
<br>


#### 1.3 Using aggregate transformation


```python
from pyspark.sql.functions import sum, avg

result_df = (
    club_bookings_df.agg(sum("booking_amount").alias("total_earning"),
                         avg("booking_amount").alias("avg_booking_value"))
)

result_df.display()
```

<img src="pics/aggregations-41-1-3-1.png" width="400" />
<br>
<br>



[⬆ Back to content](#content)


## 42. Grouping Aggregation

[⬆ Back to content](#content)

### Subheader



[⬆ Back to content](#content)


## 43. Multilevel Aggregation

[⬆ Back to content](#content)

### Subheader



[⬆ Back to content](#content)



## 44. Windowing Aggregation

[⬆ Back to content](#content)

### Subheader



[⬆ Back to content](#content)



