{
	"user": "gT35Hhhre9m",
	"dates": ["2016-01-29", "2016-01-28"],
	"status": "OK",
	"reason": "some reason",
	"content": [{
		"foo": 123,
		"bar": "val1"
	}, {
		"foo": 456,
		"bar": "val2"
	}, {
		"foo": 789,
		"bar": "val3"
	}, {
		"foo": 124,
		"bar": "val4"
	}, {
		"foo": 126,
		"bar": "val5"
	}]
}


scala> val df = sqlContext.read.json("sample.json")
df: org.apache.spark.sql.DataFrame = [content: array<struct<bar:string,foo:bigint>>, dates: array<string>, reason: string, status: string, user: string]
#output
df.show
+--------------------+--------------------+-----------+------+-----------+
|             content|               dates|     reason|status|       user|
+--------------------+--------------------+-----------+------+-----------+
|[[val1,123], [val...|[2016-01-29, 2016...|some reason|    OK|gT35Hhhre9m|
+--------------------+--------------------+-----------+------+-----------+

#explode dates field
scala> val dfDates = df.select(explode(df("dates")))
#output
dfDates.show
+----------+
|       col|
+----------+
|2016-01-29|
|2016-01-28|
+----------+

#rename "col" to "dates"
scala> val dfDates = df.select(explode(df("dates"))).toDF("dates")
#output
dfDates.show
+----------+
|     dates|
+----------+
|2016-01-29|
|2016-01-28|
+----------+

#explode content field
scala> val dfContent = df.select(explode(df("content")))
dfContent: org.apache.spark.sql.DataFrame = [col: struct<bar:string,foo:bigint>]
#output
scala> dfContent.show
+----------+
|       col|
+----------+
|[val1,123]|
|[val2,456]|
|[val3,789]|
|[val4,124]|
|[val5,126]|
+----------+

#rename "col" to "content"
scala> val dfContent = df.select(explode(df("content"))).toDF("content")
#output
scala> dfContent.show
+----------+
|   content|
+----------+
|[val1,123]|
|[val2,456]|
|[val3,789]|
|[val4,124]|
|[val5,126]|
+----------+

#extracting fields in struct
scala> val dfFooBar = dfContent.select("content.foo", "content.bar")
dfFooBar: org.apache.spark.sql.DataFrame = [foo: bigint, bar: string]

#output
scala> dfFooBar.show
+---+----+
|foo| bar|
+---+----+
|123|val1|
|456|val2|
|789|val3|
|124|val4|
|126|val5|
+---+----+




===========================================================================================================================


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)





>>> df.select('family.children','family.spouses','family.spouses.alive','family.spouses.name').show(truncate=False)
+---------------------------+---------------------------------+-------------+----------------+
|children                   |spouses                          |alive        |name            |
+---------------------------+---------------------------------+-------------+----------------+
|null                       |null                             |null         |null            |
|null                       |[[true, tyrion], [false, ramsay]]|[true, false]|[tyrion, ramsay]|
|[joffrey, myrcella, tommen]|[[false, robert]]                |[false]      |[robert]        |
|[joffrey, myrcella, tommen]|null                             |null         |null            |
+---------------------------+---------------------------------+-------------+----------------+



>>> df.printSchema
<bound method DataFrame.printSchema of DataFrame[family: struct<children:array<string>,spouses:array<struct<alive:boolean,name:string>>>, name: string, weapon: string]>




df.select('name','weapon','family','family.children','family.spouses.alive','family.spouses.name').show(truncate=False)

>>> df.select('name','weapon','family','family.children','family.spouses.alive','family.spouses.name').show(truncate=False)

+------+----------+------------------------------------------------+---------------------------+-------------+----------------+
|name  |weapon    |family                                          |children                   |alive        |name            |
+------+----------+------------------------------------------------+---------------------------+-------------+----------------+
|arya  |needle    |null                                            |null                       |null         |null            |
|sansa |null      |[, [[true, tyrion], [false, ramsay]]]           |null                       |[true, false]|[tyrion, ramsay]|
|cersei|null      |[[joffrey, myrcella, tommen], [[false, robert]]]|[joffrey, myrcella, tommen]|[false]      |[robert]        |
|jamie |oathkeeper|[[joffrey, myrcella, tommen],]                  |[joffrey, myrcella, tommen]|null         |null            |
+------+----------+------------------------------------------------+---------------------------+-------------+----------------+


df.select('name','weapon','family','family.children',F.map_from_arrays('family.spouses.name', 'family.spouses.alive')).show(truncate=False)

>>> df.select('name','weapon','family','family.children',F.map_from_arrays('family.spouses.name', 'family.spouses.alive')).show(truncate=False)
+------+----------+------------------------------------------------+---------------------------+----------------------------------------------------------+
|name  |weapon    |family                                          |children                   |map_from_arrays(family.spouses.name, family.spouses.alive)|
+------+----------+------------------------------------------------+---------------------------+----------------------------------------------------------+
|arya  |needle    |null                                            |null                       |null                                                      |
|sansa |null      |[, [[true, tyrion], [false, ramsay]]]           |null                       |[tyrion -> true, ramsay -> false]                         |
|cersei|null      |[[joffrey, myrcella, tommen], [[false, robert]]]|[joffrey, myrcella, tommen]|[robert -> false]                                         |
|jamie |oathkeeper|[[joffrey, myrcella, tommen],]                  |[joffrey, myrcella, tommen]|null                                                      |
+------+----------+------------------------------------------------+---------------------------+----------------------------------------------------------+



