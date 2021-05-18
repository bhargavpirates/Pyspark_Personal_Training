#https://stackoverflow.com/questions/60342610/spark-aggregations-where-output-columns-are-functions-and-rows-are-columns

df = spark.createDataFrame([("A", 1, 2), ("B", 2, 4), ("C", 5, 6), ("D", 6, 8)], ['cola', 'colb', 'colc'])

agg = F.map_from_entries(F.array(*[ F.struct( F.lit(c), F.struct(F.max(c).alias("Max"), F.min(c).alias("Min")) ) for c in df.columns ] )).alias("Metrics")

df.agg(agg).select(F.explode("Metrics").alias("col", "Metrics")) \
    .select("col", "Metrics.*") \
    .show()


>>> df = spark.createDataFrame([("A", 1, 2), ("B", 2, 4), ("C", 5, 6), ("D", 6, 8)], ['cola', 'colb', 'colc'])
>>> df.show()
+----+----+----+
|cola|colb|colc|
+----+----+----+
|   A|   1|   2|
|   B|   2|   4|
|   C|   5|   6|
|   D|   6|   8|
+----+----+----+

>>> from pyspark.sql import functions as F
>>> df.select(F.max("cola"),F.min("cola"),F.max("colb"),F.min("colb"),F.max("colc"),F.min("colc")).show()
+---------+---------+---------+---------+---------+---------+
|max(cola)|min(cola)|max(colb)|min(colb)|max(colc)|min(colc)|
+---------+---------+---------+---------+---------+---------+
|        D|        A|        6|        1|        8|        2|
+---------+---------+---------+---------+---------+---------+

>>> [ F.struct( F.lit(c), F.struct(max(c), min(c)) ) for c in df.columns ]
[Column<named_struct(col1, cola, col2, named_struct(NamePlaceholder(), o, NamePlaceholder(), a))>, 
 Column<named_struct(col1, colb, col2, named_struct(NamePlaceholder(), o, NamePlaceholder(), b))>, 
 Column<named_struct(col1, colc, col2, named_struct(NamePlaceholder(), o, NamePlaceholder(), c))>]

>>> agg
Column<map_from_entries(array(named_struct(col1, cola, col2, named_struct(NamePlaceholder(), max(cola) AS `Max`, NamePlaceholder(), min(cola) AS `Min`)), 
							  named_struct(col1, colb, col2, named_struct(NamePlaceholder(), max(colb) AS `Max`, NamePlaceholder(), min(colb) AS `Min`)), 
							  named_struct(col1, colc, col2, named_struct(NamePlaceholder(), max(colc) AS `Max`, NamePlaceholder(), min(colc) AS `Min`)))) AS `Metrics`>


>>> df.agg(agg).select("*").show(truncate=False)
+------------------------------------------------+
|Metrics                                         |
+------------------------------------------------+
|[cola -> [D, A], colb -> [6, 1], colc -> [8, 2]]|
+------------------------------------------------+

>>> df.agg(agg).select(F.explode("Metrics").alias("col", "Metrics")).show()
+----+-------+
| col|Metrics|
+----+-------+
|cola| [D, A]|
|colb| [6, 1]|
|colc| [8, 2]|
+----+-------+



>>> df.agg(agg).select(F.explode("Metrics").alias("col", "Metrics")) \
...     .select("col", "Metrics.*") \
...     .show()
+----+---+---+
| col|Max|Min|
+----+---+---+
|cola|  D|  A|
|colb|  6|  1|
|colc|  8|  2|
+----+---+---+



