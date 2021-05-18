*************************************************************************
						NOTE:
*************************************************************************						
"concat_ws" and "map" are hive udf 
"collect_list" is a hive udaf.
*************************************************************************



table1 = [(1,"James","","Smith","36636","M",3000),
    (2,"Michael","Rose","","40288","M",4000),
    (3,"Robert","","Williams","42114","M",4000),
    (4,"Maria","Anne","Jones","39192","F",4000),
    (5,"Jen","Mary","Brown","","F",-1)
  ]

df1 = spark.createDataFrame(data=table1,schema=["id","firstname","middlename","lastname","id_new","gender","salary"])


import pyspark.sql.functions as F
df.select(F.collect_set("firstname")).show()

>>> df1.select(F.collect_set("firstname")).show(truncate=False)
+------------------------------------+
|collect_set(firstname)              |
+------------------------------------+
|[Robert, Maria, Michael, Jen, James]|
+------------------------------------+

>>> df1.select(F.collect_list("firstname")).show(truncate=False)
+------------------------------------+
|collect_list(firstname)             |
+------------------------------------+
|[Robert, Maria, Jen, James, Michael]|
+------------------------------------+


df1.select(F.array("firstname")).show(truncate=False)

>>> df1.select(F.array("firstname")).show(truncate=False)
+----------------+
|array(firstname)|
+----------------+
|[James]         |
|[Michael]       |
|[Robert]        |
|[Maria]         |
|[Jen]           |
+----------------+


>>> df1.select(F.array("firstname","middlename","lastname")).show(truncate=False)
+--------------------------------------+
|array(firstname, middlename, lastname)|
+--------------------------------------+
|[James, , Smith]                      |
|[Michael, Rose, ]                     |
|[Robert, , Williams]                  |
|[Maria, Anne, Jones]                  |
|[Jen, Mary, Brown]                    |
+--------------------------------------+


>>> df1.select(F.concat("firstname","middlename","lastname")).show(truncate=False)
+---------------------------------------+
|concat(firstname, middlename, lastname)|
+---------------------------------------+
|JamesSmith                             |
|MichaelRose                            |
|RobertWilliams                         |
|MariaAnneJones                         |
|JenMaryBrown                           |
+---------------------------------------+

>>> df1.select(F.concat_ws(" ","firstname","middlename","lastname")).show(truncate=False)
+---------------------------------------------+
|concat_ws( , firstname, middlename, lastname)|
+---------------------------------------------+
|James  Smith                                 |
|Michael Rose                                 |
|Robert  Williams                             |
|Maria Anne Jones                             |
|Jen Mary Brown                               |
+---------------------------------------------+




>>> df1.show()
+---+---------+----------+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|
|  2|  Michael|      Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|
|  4|    Maria|      Anne|   Jones| 39192|     F|  4000|
|  5|      Jen|      Mary|   Brown|      |     F|    -1|
+---+---------+----------+--------+------+------+------+



>>> df1.select(create_map("id","salary").alias("id_salary")).show(truncate=False)
+-----------+
|id_salary  |
+-----------+
|[1 -> 3000]|
|[2 -> 4000]|
|[3 -> 4000]|
|[4 -> 4000]|
|[5 -> -1]  |
+-----------+




#================================================================================================================================================
table1 = [(1,"James","","Smith","36636","M",3000),
    (2,"Michael","Rose","","40288","M",4000),
    (3,"Robert","","Williams","42114","M",4000),
    (4,"Maria","Anne","Jones","39192","F",4000),
    (5,"Jen","Mary","Brown","","F",-1)
  ]

df1 = spark.createDataFrame(data=table1,schema=["id","firstname","middlename","lastname","id_new","gender","salary"])

df1.groupBy("gender").count()

>>> df1.groupBy("gender").count().show()
+------+-----+
|gender|count|
+------+-----+
|     F|    2|
|     M|    3|
+------+-----+

>>> import pyspark.sql.functions as F
>>> df1.groupBy("gender").agg(F.collect_set("firstname")).show()
+------+----------------------+
|gender|collect_set(firstname)|
+------+----------------------+
|     F|          [Maria, Jen]|
|     M|  [Robert, Michael,...|
+------+----------------------+

>>> df1.select(F.collect_set("firstname")).show()
+----------------------+
|collect_set(firstname)|
+----------------------+
|  [Robert, Maria, M...|
+----------------------+