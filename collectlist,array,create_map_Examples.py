
data =[[ "a",2012, 10],[ "b",2012, 12],[ "c",2013, 5 ],[ "b",2014, 7 ],[ "c",2012, 3 ]]
df11 = spark.createDataFrame(data,schema=['id','year','qty'])

>>> df11 = spark.createDataFrame(data,schema=['id','year','qty'])
>>> df11
DataFrame[name: string, year: bigint, val: bigint]
>>> df11.show()
+----+----+---+
|name|year|val|
+----+----+---+
|   a|2012| 10|
|   b|2012| 12|
|   c|2013|  5|
|   b|2014|  7|
|   c|2012|  3|
+----+----+---+


df11.groupBy("name").agg(F.collect_list("val")).show(truncate=False)

>>> df11.groupBy("name").agg(F.collect_list("val")).show(truncate=False)
+----+-----------------+
|name|collect_list(val)|
+----+-----------------+
|c   |[5, 3]           |
|b   |[7, 12]          |
|a   |[10]             |
+----+-----------------+


from itertools import chain

years = sorted(chain(*df11.select("year").distinct().collect()))
df11.groupBy("id").pivot("year", years).sum("qty")

>>> years = sorted(chain(*df11.select("year").distinct().collect()))
>>> df11.groupBy("id").pivot("year", years).sum("qty")
DataFrame[id: string, 2012: bigint, 2013: bigint, 2014: bigint]
>>> df11.groupBy("id").pivot("year", years).sum("qty").show()
+---+----+----+----+
| id|2012|2013|2014|
+---+----+----+----+
|  c|   3|   5|null|
|  b|  12|null|   7|
|  a|  10|null|null|
+---+----+----+----+







rdd = sc.parallelize([('123k', 1.3, 6.3, 7.6),
                      ('d23d', 1.5, 2.0, 2.2), 
                      ('as3d', 2.2, 4.3, 9.0)
                          ])
schema = StructType([StructField('key', StringType(), True),
                     StructField('metric1', FloatType(), True),
                     StructField('metric2', FloatType(), True),
                     StructField('metric3', FloatType(), True)])
df = sqlContext.createDataFrame(rdd, schema)
>>> df.show()
+----+-------+-------+-------+
| key|metric1|metric2|metric3|
+----+-------+-------+-------+
|123k|    1.3|    6.3|    7.6|
|d23d|    1.5|    2.0|    2.2|
|as3d|    2.2|    4.3|    9.0|
+----+-------+-------+-------+

df.select(F.array("metric1","metric2","metric3")).show()
>>> df.select("key", F.array("metric1","metric2","metric3").alias("metric")).show()
+----+---------------+
| key|         metric|
+----+---------------+
|123k|[1.3, 6.3, 7.6]|
|d23d|[1.5, 2.0, 2.2]|
|as3d|[2.2, 4.3, 9.0]|
+----+---------------+

from pyspark.sql.functions import lit, col, create_map
from itertools import chain

metric = create_map(  list(  chain(  *(  (lit(name), col(name)) for name in df.columns if "metric" in name)  ) )  ).alias("metric")
df.select("key", metric).show(truncate=False)

>>> df.select("key", metric).show(truncate=False)
+----+------------------------------------------------+
|key |metric                                          |
+----+------------------------------------------------+
|123k|[metric1 -> 1.3, metric2 -> 6.3, metric3 -> 7.6]|
|d23d|[metric1 -> 1.5, metric2 -> 2.0, metric3 -> 2.2]|
|as3d|[metric1 -> 2.2, metric2 -> 4.3, metric3 -> 9.0]|
+----+------------------------------------------------+

