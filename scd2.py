schema="pk,amount,startDate,endDate,active"


existing_data=[[ 1,10,"2019-01-01 12:00:00","2019-01-20 05:00:00",0],\
[ 1,20,"2019-01-20 05:00:00",None,1],\
[ 2,100,"2019-01-01 00:00:00",None,1],\
[ 3,75,"2019-01-01 06:00:00","2019-01-26 08:00:00",0],\
[ 3,750,"2019-01-26 08:00:00",None,1],\
[10,40,"2019-01-01 00:00:00",None,1]]


new_incoming_data=[[1,50,"2019-02-01 07:00:00","2019-02-02 08:00:00",0],\
[1,75,"2019-02-02 08:00:00",               None,1],\
[2,200,"2019-02-01 05:00:00","2019-02-01 13:00:00",0],\
[2,60,"2019-02-01 13:00:00","2019-02-01 19:00:00",0],\
[2,500,"2019-02-01 19:00:00",               None,1],\
[3,175,"2019-02-01 00:00:00",               None,1],\
[4,50,"2019-02-02 12:00:00","2019-02-02 14:00:00",0],\
[4,300,"2019-02-02 14:00:00",               None,1],\
[5,500,"2019-02-02 00:00:00",               None,1]]


DATA Prepartion:
*******************
from pyspark.sql import functions as F
old_df= spark.createDataFrame(existing_data,schema.split(","))0
new_df= spark.createDataFrame(new_incoming_data,schema.split(","))

old_df.show(truncate=False)
new_df.show(truncate=False)



LOGIC:
***********
w = Window.partitionBy("pk").orderBy("startDate")
updates = new_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").select("pk", "startDate")

join_df=(old_df.join(updates.alias("new"), "pk", "left")
      .withColumn("endDate", F.when( (F.col("endDate").isNull()) & (F.col("active") == F.lit(1)) & (F.col("new.startDate").isNotNull()),F.col("new.startDate"))
      .otherwise(F.col("endDate")))
      .withColumn("active", F.when(F.col("endDate").isNull(), F.lit(1)).otherwise(F.lit(0)))
      .drop(F.col("new.startDate")))

results = join_df.union(new_df).orderBy(F.col("pk"), F.col("startDate"))


existing_data=[
[ 1,10,   "2019-01-01 12:00:00","2019-01-20 05:00:00",0],\
[ 1,20,   "2019-01-20 05:00:00", 				 None,1],\
[ 2,100,  "2019-01-01 00:00:00", 				 None,1],\
[ 3,75,   "2019-01-01 06:00:00","2019-01-26 08:00:00",0],\
[ 3,750,  "2019-01-26 08:00:00", 				 None,1],\
[10,40,   "2019-01-01 00:00:00", 				 None,1]]


new_incoming_data=[
[1, 50,"2019-02-01 07:00:00","2019-02-02 08:00:00",0],\
[1, 75,"2019-02-02 08:00:00",                 None,1],\
[2,200,"2019-02-01 05:00:00","2019-02-01 13:00:00",0],\
[2, 60,"2019-02-01 13:00:00","2019-02-01 19:00:00",0],\
[2,500,"2019-02-01 19:00:00",                 None,1],\
[3,175,"2019-02-01 00:00:00",                 None,1],\
[4, 50,"2019-02-02 12:00:00","2019-02-02 14:00:00",0],\
[4,300,"2019-02-02 14:00:00",                 None,1],\
[5,500,"2019-02-02 00:00:00",                 None,1]]



Results:
**********
>>> join_df.union(new_df).orderBy(F.col("pk"), F.col("startDate")).show()
+---+------+-------------------+-------------------+------+
| pk|amount|          startDate|            endDate|active|
+---+------+-------------------+-------------------+------+
|  1|    10|2019-01-01 12:00:00|2019-01-20 05:00:00|     0|
|  1|    20|2019-01-20 05:00:00|2019-02-01 07:00:00|     0|
|  1|    50|2019-02-01 07:00:00|2019-02-02 08:00:00|     0|
|  1|    75|2019-02-02 08:00:00|               null|     1|
|  2|   100|2019-01-01 00:00:00|2019-02-01 05:00:00|     0|
|  2|   200|2019-02-01 05:00:00|2019-02-01 13:00:00|     0|
|  2|    60|2019-02-01 13:00:00|2019-02-01 19:00:00|     0|
|  2|   500|2019-02-01 19:00:00|               null|     1|
|  3|    75|2019-01-01 06:00:00|2019-01-26 08:00:00|     0|
|  3|   750|2019-01-26 08:00:00|2019-02-01 00:00:00|     0|
|  3|   175|2019-02-01 00:00:00|               null|     1|
|  4|    50|2019-02-02 12:00:00|2019-02-02 14:00:00|     0|
|  4|   300|2019-02-02 14:00:00|               null|     1|
|  5|   500|2019-02-02 00:00:00|               null|     1|
| 10|    40|2019-01-01 00:00:00|               null|     1|
+---+------+-------------------+-------------------+------+