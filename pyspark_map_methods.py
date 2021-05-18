#How to collect a map after group by in Pyspark dataframe?

import pyspark.sql.functions as F
from pyspark.sql.types import StringType

df = spark.createDataFrame([(1,'t1','a'),(1,'t2','b'),(2,'t3','b'),(2,'t4','c'),(2,'t5','b'),\
(3,'t6','a'),(3,'t7','a'),(3,'t8','a')],\
('id','time','cat'))

>>> df.show()
+---+----+---+
| id|time|cat|
+---+----+---+
|  1|  t1|  a|
|  1|  t2|  b|
|  2|  t3|  b|
|  2|  t4|  c|
|  2|  t5|  b|
|  3|  t6|  a|
|  3|  t7|  a|
|  3|  t8|  a|
+---+----+---+


>>> df.groupBy("id","cat").agg(F.collect_set("time")).show()
+---+---+-----------------+
| id|cat|collect_set(time)|
+---+---+-----------------+
|  2|  b|         [t5, t3]|
|  3|  a|     [t8, t6, t7]|
|  2|  c|             [t4]|
|  1|  a|             [t1]|
|  1|  b|             [t2]|
+---+---+-----------------+

df.groupBy(['id', 'cat'])\
   .agg(F.count(F.col('cat'))).show()




(df.groupBy(['id', 'cat'])
   .agg(F.count(F.col('cat')).cast(StringType()).alias('counted'))
   .select(['id', F.concat_ws('->', F.col('cat'), F.col('counted')).alias('arrowed')])
   .groupBy('id')
   .agg(F.collect_list('arrowed'))
   .show()
)

>>> (df.groupBy(['id', 'cat'])
...    .agg(F.count(F.col('cat')).cast(StringType()).alias('counted'))
...    .select(['id', F.concat_ws('->', F.col('cat'), F.col('counted')).alias('arrowed')]).show())
+---+-------+
| id|arrowed|
+---+-------+
|  2|   b->2|
|  3|   a->3|
|  2|   c->1|
|  1|   a->1|
|  1|   b->1|
+---+-------+

>>>
>>> (df.groupBy(['id', 'cat'])
...    .agg(F.count(F.col('cat')).cast(StringType()).alias('counted'))
...    .select(['id', F.concat_ws('->', F.col('cat'), F.col('counted')).alias('arrowed')])
...    .groupBy('id')
...    .agg(F.collect_list('arrowed'))
...    .show()
... )

+---+---------------------+
| id|collect_list(arrowed)|
+---+---------------------+
|  1|         [a->1, b->1]|
|  3|               [a->3]|
|  2|         [b->2, c->1]|





df.groupBy(['id', 'cat'])\
   .count()\
   .select(['id',F.create_map('cat', 'count').alias('map')])\
   .groupBy('id')\
   .agg(F.collect_list('map').alias('cat'))\
   .show()


>>> df.groupBy(['id', 'cat'])\
...    .count().show()
+---+---+-----+
| id|cat|count|
+---+---+-----+
|  2|  b|    2|
|  3|  a|    3|
|  2|  c|    1|
|  1|  a|    1|
|  1|  b|    1|
+---+---+-----+

>>> df.groupBy(['id', 'cat'])\
...    .count()\
...    .select(['id',F.create_map('cat', 'count').alias('map')]).show()
+---+--------+
| id|     map|
+---+--------+
|  2|[b -> 2]|
|  3|[a -> 3]|
|  2|[c -> 1]|
|  1|[a -> 1]|
|  1|[b -> 1]|
+---+--------+


>>> df.groupBy(['id', 'cat'])\
...    .count()\
...    .select(['id',F.create_map('cat', 'count').alias('map')])\
...    .groupBy('id')\
...    .agg(F.collect_list('map').alias('cat'))\
...    .show()
+---+--------------------+
| id|                 cat|
+---+--------------------+
|  1|[[a -> 1], [b -> 1]]|
|  3|          [[a -> 3]]|
|  2|[[b -> 2], [c -> 1]]|
+---+--------------------+



df_res=df.groupBy(['id', 'cat'])\
   .count()\
   .select(['id',F.create_map('cat', 'count').alias('map')])\
   .groupBy('id')\
   .agg(F.collect_list('map').alias('cat'))

>>> df_resdf_res.printSchema()
root
 |-- id: long (nullable = true)
 |-- cat: array (nullable = true)
 |    |-- element: map (containsNull = true)
 |    |    |-- key: string
 |    |    |-- value: long (valueContainsNull = false)





=================================================================================================================================


df1 = df.groupby("id", "cat").count()
df2 = df1.groupby("id")\
         .agg(F.map_from_entries(F.collect_list(F.struct("cat","count"))).alias("cat"))


>>> df1 = df.groupby("id", "cat").count()
>>> df2 = df1.groupby("id")\
...          .agg(F.map_from_entries(F.collect_list(F.struct("cat","count"))).alias("cat"))
>>> df2.show()
+---+----------------+
| id|             cat|
+---+----------------+
|  1|[a -> 1, b -> 1]|
|  3|        [a -> 3]|
|  2|[b -> 2, c -> 1]|
+---+----------------+

=========================================================================================================================

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

df = spark.createDataFrame([['Alabama',],['Texas',],['Antioquia',]]).toDF('state')
mapping = {'Alabama': 'AL', 'Texas': 'TX'}
mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
df.withColumn("state_abbreviation", mapping_expr.getItem(col("state"))).show()

res:
----
mapping_expr=[Column<Alabama>, Column<AL>, Column<Texas>, Column<TX>]
+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+

=========================================================================================================================

dicts = sc.broadcast(dict([('india','ind'), ('usa','us'),('japan','jpn'),('uruguay','urg')]))

from pyspark.sql import functions as f
from pyspark.sql import types as t
def newCols(x):
    return dicts.value[x]

callnewColsUdf = f.udf(newCols, t.StringType())

df.withColumn('col1_map', callnewColsUdf(f.col('col1')))\
    .withColumn('col2_map', callnewColsUdf(f.col('col2')))\
    .show(truncate=False)

