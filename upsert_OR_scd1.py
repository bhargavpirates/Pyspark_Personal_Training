Data Prepartion:
********************
>>> from pyspark.sql import functions as F
>>> schema = "emp_id,emp_name,emp_city,emp_salary"
>>> old_data = [[1, 'John', 'Sydney', 35000.00],[2, 'Peter', 'Melbourne', 45000.00],[3, 'Sam', 'Sydney',55000.00]]
>>> new_data = [[2, "Peter", "Melbourne", 55000.00],[5, "Jessie", "Brisbane", 42000.00]]
>>> old_df= spark.createDataFrame(old_data,schema.split(","))
>>> new_df= spark.createDataFrame(new_data,schema.split(","))


>>> old_df.show()
+------+--------+---------+----------+
|emp_id|emp_name| emp_city|emp_salary|
+------+--------+---------+----------+
|     1|    John|   Sydney|   35000.0|
|     2|   Peter|Melbourne|   45000.0|
|     3|     Sam|   Sydney|   55000.0|
+------+--------+---------+----------+

>>> new_df.show()
+------+--------+---------+----------+
|emp_id|emp_name| emp_city|emp_salary|
+------+--------+---------+----------+
|     2|   Peter|Melbourne|   55000.0|
|     5|  Jessie| Brisbane|   42000.0|
+------+--------+---------+----------+

LOGIC:
********

>>> old_df.unionAll(new_df)
DataFrame[emp_id: bigint, emp_name: string, emp_city: string, emp_salary: double]
>>> old_df.unionAll(new_df).show()
+------+--------+---------+----------+
|emp_id|emp_name| emp_city|emp_salary|
+------+--------+---------+----------+
|     1|    John|   Sydney|   35000.0|
|     2|   Peter|Melbourne|   45000.0|
|     3|     Sam|   Sydney|   55000.0|
|     2|   Peter|Melbourne|   55000.0|
|     5|  Jessie| Brisbane|   42000.0|
+------+--------+---------+----------+

>>> union_df=old_df.unionAll(new_df)
>>> from pyspark.sql import Window

window_agg=Window.partitionBy("emp_id").orderBy(F.desc("emp_salary"))


>>> union_df.select(F.row_number().over(window_agg).alias("rn"),"*").filter("rn=1").select(new_df.columns).orderBy("emp_id").show()
+------+--------+---------+----------+
|emp_id|emp_name| emp_city|emp_salary|
+------+--------+---------+----------+
|     1|    John|   Sydney|   35000.0|
|     2|   Peter|Melbourne|   55000.0|
|     3|     Sam|   Sydney|   55000.0|
|     5|  Jessie| Brisbane|   42000.0|
+------+--------+---------+----------+



===================================================================================================================================
                               Another Way
===================================================================================================================================

from datetime import datetime
old_df1=old_df.withColumn("timestamp",F.lit(datetime.now()))
new_df1=new_df.withColumn("timestamp",F.lit(datetime.now()))

union_df1=old_df1.unionAll(new_df1)
window_agg=Window.partitionBy("emp_id").orderBy(F.desc("timestamp"))
union_df1.select(F.row_number().over(window_agg).alias("rn"),"*").filter("rn=1").select(new_df1.columns).orderBy("emp_id").show()

+------+--------+---------+----------+--------------------------+
|emp_id|emp_name|emp_city |emp_salary|timestamp                 |
+------+--------+---------+----------+--------------------------+
|1     |John    |Sydney   |35000.0   |2021-03-24 07:04:41.204323|
|2     |Peter   |Melbourne|55000.0   |2021-03-24 07:04:52.599981|
|3     |Sam     |Sydney   |55000.0   |2021-03-24 07:04:41.204323|
|5     |Jessie  |Brisbane |42000.0   |2021-03-24 07:04:52.599981|
+------+--------+---------+----------+--------------------------+



union_df1.select(F.row_number().over(window_agg).alias("rn"),"*").filter("rn=1").select(new_df.columns).orderBy("emp_id").show()

+------+--------+---------+----------+
|emp_id|emp_name| emp_city|emp_salary|
+------+--------+---------+----------+
|     1|    John|   Sydney|   35000.0|
|     2|   Peter|Melbourne|   55000.0|
|     3|     Sam|   Sydney|   55000.0|
|     5|  Jessie| Brisbane|   42000.0|
+------+--------+---------+----------+
