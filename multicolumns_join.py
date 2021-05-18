#NOte : important in joins:
Question : Including null values in an Apache Spark Join --> to acheiove this we use eqNullSafe(column) method in join where condition 
val numbersDf = Seq(
  ("123"),
  ("456"),
  (null),
  ("")
).toDF("numbers")

val lettersDf = Seq(
  ("123", "abc"),
  ("456", "def"),
  (null, "zzz"),
  ("", "hhh")
).toDF("numbers", "letters")

val joinedDf = numbersDf.join(lettersDf, Seq("numbers"))

Here is the output of joinedDf.show():
+-------+-------+
|numbers|letters|
+-------+-------+
|    123|    abc|
|    456|    def|
|       |    hhh|
+-------+-------+
This is the output I would like:

+-------+-------+
|numbers|letters|
+-------+-------+
|    123|    abc|
|    456|    def|
|       |    hhh|
|   null|    zzz|
+-------+-------+

-------------------------------------- to acheive above output  we need this method : In Spark 2.3.0 or later you can use Column.eqNullSafe in PySpark:
numbers_df = sc.parallelize([
    ("123", ), ("456", ), (None, ), ("", )
]).toDF(["numbers"])

letters_df = sc.parallelize([
    ("123", "abc"), ("456", "def"), (None, "zzz"), ("", "hhh")
]).toDF(["numbers", "letters"])

numbers_df.join(letters_df, numbers_df.numbers.eqNullSafe(letters_df.numbers))

+-------+-------+-------+
|numbers|numbers|letters|
+-------+-------+-------+
|    456|    456|    def|
|   null|   null|    zzz|
|       |       |    hhh|
|    123|    123|    abc|
+-------+-------+-------+


SPARK SQL:
numbersDf.alias("numbers")
  .join(lettersDf.alias("letters"))
  .where("numbers.numbers IS NOT DISTINCT FROM letters.numbers")

==================================================================================================================================================

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
table1 = [(1,"James","","Smith","36636","M",3000),
    (2,"Michael","Rose","","40288","M",4000),
    (3,"Robert","","Williams","42114","M",4000),
    (4,"Maria","Anne","Jones","39192","F",4000),
    (5,"Jen","Mary","Brown","","F",-1)
  ]


table2 = [(1,"James","","Smith","36636","M",3000),
    (1,"Michael","Rose","","40288","M",4000),
    (2,"Robert","","Williams","42114","M",4000),
    (3,"Maria","Anne","Jones","39192","F",4000),
    (3,"Jen","Mary","Brown","","F",-1)
  ]



#schema = StructType([ \
#    StructField("firstname",StringType(),True), \
#    StructField("middlename",StringType(),True), \
#    StructField("lastname",StringType(),True), \
#    StructField("id", StringType(), True), \
#    StructField("gender", StringType(), True), \
#    StructField("salary", IntegerType(), True) \
#  ])
 
df1 = spark.createDataFrame(data=table1,schema=["id","firstname","middlename","lastname","id_new","gender","salary"])
df2 = spark.createDataFrame(data=table2,schema=["id","fname","mname","lname","id_new","gender","salary"])

>>> df1 = spark.createDataFrame(data=table1,schema=["id","firstname","middlename","lastname","id_new","gender","salary"])
>>> df2 = spark.createDataFrame(data=table2,schema=["id","fname","mname","lname","id_new","gender","salary"])
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

>>> df2.show()
+---+-------+-----+--------+------+------+------+
| id|  fname|mname|   lname|id_new|gender|salary|
+---+-------+-----+--------+------+------+------+
|  1|  James|     |   Smith| 36636|     M|  3000|
|  1|Michael| Rose|        | 40288|     M|  4000|
|  2| Robert|     |Williams| 42114|     M|  4000|
|  3|  Maria| Anne|   Jones| 39192|     F|  4000|
|  3|    Jen| Mary|   Brown|      |     F|    -1|
+---+-------+-----+--------+------+------+------+



#joins:
#1. Inner
df1.join(df2,df1.id==df2.id,how="inner")
df1.join(df2,on="names",how="inner")

>>> df1.join(df2,df1.id==df2.id,how="inner")
DataFrame[id: bigint, firstname: string, middlename: string, lastname: string, id_new: string, gender: string, salary: bigint, id: bigint, fname: string, mname: string, lname: string, id_new: string, gender: string, salary: bigint]
>>> df1.join(df2,df1.id==df2.id,how="inner").show()
+---+---------+----------+--------+------+------+------+---+-------+-----+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary| id|  fname|mname|   lname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+---+-------+-----+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|  1|  James|     |   Smith| 36636|     M|  3000|
|  1|    James|          |   Smith| 36636|     M|  3000|  1|Michael| Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  3|  Maria| Anne|   Jones| 39192|     F|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  3|    Jen| Mary|   Brown|      |     F|    -1|
|  2|  Michael|      Rose|        | 40288|     M|  4000|  2| Robert|     |Williams| 42114|     M|  4000|
+---+---------+----------+--------+------+------+------+---+-------+-----+--------+------+------+------+


>>> df1.join(df2,on="id",how="inner").show()
+---+---------+----------+--------+------+------+------+-------+-----+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|  fname|mname|   lname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+-------+-----+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|  James|     |   Smith| 36636|     M|  3000|
|  1|    James|          |   Smith| 36636|     M|  3000|Michael| Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  Maria| Anne|   Jones| 39192|     F|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|    Jen| Mary|   Brown|      |     F|    -1|
|  2|  Michael|      Rose|        | 40288|     M|  4000| Robert|     |Williams| 42114|     M|  4000|
+---+---------+----------+--------+------+------+------+-------+-----+--------+------+------+------+

table1 = [(1,"James","","Smith","36636","M",3000),
    (2,"Michael","Rose","","40288","M",4000),
    (3,"Robert","","Williams","42114","M",4000),
    (4,"Maria","Anne","Jones","39192","F",4000),
    (5,"Jen","Mary","Brown","","F",-1)
  ]


table2 = [(1,"James","","Smith","36636","M",3000),
    (1,"Michael","Rose","","40288","F",4000),
    (2,"Robert","","Williams","42114","M",4000),
    (3,"Maria","Anne","Jones","39192","F",4000),
    (3,"Jen","Mary","Brown","","M",-1)
  ]

df2 = spark.createDataFrame(data=table2,schema=["id","fname","mname","lname","id_new","gender","salary"])

#df1.join(df2, (df1.x1 == df2.x1) & (df1.x2 == df2.x2))
df1.join(df2, (df1.id==df2.id) & (df1.gender == df2.gender) ,how="inner")
df1.join(df2,on="names",how="inner")

>>> df1.join(df2, (df1.id==df2.id) & (df1.gender == df2.gender) ,how="inner").show()
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary| id| fname|mname|   lname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
|  2|  Michael|      Rose|        | 40288|     M|  4000|  2|Robert|     |Williams| 42114|     M|  4000|
|  1|    James|          |   Smith| 36636|     M|  3000|  1| James|     |   Smith| 36636|     M|  3000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  3|   Jen| Mary|   Brown|      |     M|    -1|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+



>>> df1.join(df2, (df1.id==df2.id) & (df1.gender == df2.gender)).show()
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary| id| fname|mname|   lname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
|  2|  Michael|      Rose|        | 40288|     M|  4000|  2|Robert|     |Williams| 42114|     M|  4000|
|  1|    James|          |   Smith| 36636|     M|  3000|  1| James|     |   Smith| 36636|     M|  3000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  3|   Jen| Mary|   Brown|      |     M|    -1|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+

#df.fillna(0, subset=['a', 'b'])
df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).fillna("--", subset=['middlename', 'lastname']).show()
df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("a.*").show()

>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("a.*").show()
+---+---------+----------+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+
|  2|  Michael|      Rose|        | 40288|     M|  4000|
|  1|    James|          |   Smith| 36636|     M|  3000|
|  3|   Robert|          |Williams| 42114|     M|  4000|
+---+---------+----------+--------+------+------+------+

>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("*").show()
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary| id| fname|mname|   lname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+
|  2|  Michael|      Rose|        | 40288|     M|  4000|  2|Robert|     |Williams| 42114|     M|  4000|
|  1|    James|          |   Smith| 36636|     M|  3000|  1| James|     |   Smith| 36636|     M|  3000|
|  3|   Robert|          |Williams| 42114|     M|  4000|  3|   Jen| Mary|   Brown|      |     M|    -1|
+---+---------+----------+--------+------+------+------+---+------+-----+--------+------+------+------+

df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("a.id").show()
>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("a.id").show()
+---+
| id|
+---+
|  2|
|  1|
|  3|
+---+

df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("lastname").show()
>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("lastname").show()
+--------+
|lastname|
+--------+
|        |
|   Smith|
|Williams|
+--------+

df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).fillna("--", subset=['mname']).show()

df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select("--", subset=['mname']).show()

from pyspark.sql import functions as F
df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()

df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select(df1.firstname,F.when(df2.mname=="","--").otherwise(df2.mname)).show()

>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select(df1.firstname,F.when(df2.mname=="","--").otherwise(df2.mname)).show()
+---------+-------------------------------------------+
|firstname|CASE WHEN (mname = ) THEN -- ELSE mname END|
+---------+-------------------------------------------+
|  Michael|                                         --|
|    James|                                         --|
|   Robert|                                       Mary|
+---------+-------------------------------------------+

>>> df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select(df1.firstname,F.when(df2.mname=="",None).otherwise(df2.mname)).show()
+---------+---------------------------------------------+
|firstname|CASE WHEN (mname = ) THEN NULL ELSE mname END|
+---------+---------------------------------------------+
|  Michael|                                         null|
|    James|                                         null|
|   Robert|                                         Mary|
+---------+---------------------------------------------+


Empty to NUll:
***************
    from pyspark.sql.functions import col, when

    def blank_as_null(x):
        return when(col(x) != "", col(x)).otherwise(None)

    dfWithEmptyReplaced = testDF.withColumn("col1", blank_as_null("col1"))

    1.  If you want to fill multiple columns you can for example reduce:
        to_convert = set([...]) # Some set of columns
        
        reduce(lambda df, x: df.withColumn(x, blank_as_null(x)), to_convert, testDF)

    2.  or use comprehension:
            exprs = [blank_as_null(x).alias(x) if x in to_convert else x for x in testDF.columns]
            testDF.select(*exprs)


def blank_as_null(x):
    return F.when(col(x) != "", col(x)).otherwise(None)

#dfWithEmptyReplaced = df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select( F.when(df2.mname != "", df2.mname).otherwise(None).alias("mname") )
>>> dfWithEmptyReplaced = df1.alias("a").join(df2.alias("b"), (df1.id==df2.id) & (df1.gender == df2.gender)).select( F.when(df2.mname != "", df2.mname).otherwise(None).alias("mname") )
>>> dfWithEmptyReplaced.show()
+-----+
|mname|
+-----+
| null|
| null|
| Mary|
+-----+


#df1_alias = df1.select(*(col(x).alias(x + '_df1') for x in df1.columns))
#df2_alias = df2.select(*(col(x).alias(x + '_df2') for x in df2.columns))
#df_join_alias = df1_alias.alias("a").join(df2_alias.alias("b"), (df1_alias.id_df1==df2_alias.id_df2) & (df1_alias.gender_df1 == df2_alias.gender_df2))
#to_convert = ['middlename_df1','lastname_df1','mname_df2','id_new_df2']
#exprs = [blank_as_null(x).alias(x) if x in to_convert else x for x in df_join_alias.columns]
#df_join_alias.select(*exprs).show()

>>> exprs
['id_df1', 'firstname_df1', Column<CASE WHEN (NOT (middlename_df1 = )) THEN middlename_df1 ELSE NULL END AS `middlename_df1`>, Column<CASE WHEN (NOT (lastname_df1 = )) THEN lastname_df1 ELSE NULL END AS `lastname_df1`>, 'id_new_df1', 'gender_df1', 'salary_df1', 'id_df2', 'fname_df2', Column<CASE WHEN (NOT (mname_df2 = )) THEN mname_df2 ELSE NULL END AS `mname_df2`>, 'lname_df2', Column<CASE WHEN (NOT (id_new_df2 = )) THEN id_new_df2 ELSE NULL END AS `id_new_df2`>, 'gender_df2', 'salary_df2']


>>> df1_alias = df1.select(*(col(x).alias(x + '_df1') for x in df1.columns))
>>> df2_alias = df2.select(*(col(x).alias(x + '_df2') for x in df2.columns))
>>> df_join_alias = df1_alias.alias("a").join(df2_alias.alias("b"), (df1_alias.id_df1==df2_alias.id_df2) & (df1_alias.gender_df1 == df2_alias.gender_df2))
>>> to_convert = ['middlename_df1','lastname_df1','mname_df2','id_new_df2']
>>> exprs = [blank_as_null(x).alias(x) if x in to_convert else x for x in df_join_alias.columns]
>>> df_join_alias.select(*exprs).show()
+------+-------------+--------------+------------+----------+----------+----------+------+---------+---------+---------+----------+----------+----------+
|id_df1|firstname_df1|middlename_df1|lastname_df1|id_new_df1|gender_df1|salary_df1|id_df2|fname_df2|mname_df2|lname_df2|id_new_df2|gender_df2|salary_df2|
+------+-------------+--------------+------------+----------+----------+----------+------+---------+---------+---------+----------+----------+----------+
|     2|      Michael|          Rose|        null|     40288|         M|      4000|     2|   Robert|     null| Williams|     42114|         M|      4000|
|     1|        James|          null|       Smith|     36636|         M|      3000|     1|    James|     null|    Smith|     36636|         M|      3000|
|     3|       Robert|          null|    Williams|     42114|         M|      4000|     3|      Jen|     Mary|    Brown|      null|         M|        -1|
+------+-------------+--------------+------------+----------+----------+----------+------+---------+---------+---------+----------+----------+----------+



Find Null Rows in pyspark :
***************************

df1.filter(df1.middlename.isNull()).count()
df1.filter(df1.middlename.isNull()).count()

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

>>> df1.filter(df1.middlename=="").count()
2

df1.filter( (df1.middlename=="") | (df1.lastname=="") ).count()

>>> df1.filter( (df1.middlename=="") | (df1.lastname=="") ).count()
3
>>> df1.filter( (df1.middlename=="") | (df1.lastname=="") ).show()
+---+---------+----------+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|
|  2|  Michael|      Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|
+---+---------+----------+--------+------+------+------+


def find_empty_rows(x):
    return (col(x)=="") 

>>> [ find_empty_rows(x) for x in df1.columns ]
[Column<(id = )>, Column<(firstname = )>, Column<(middlename = )>, Column<(lastname = )>, Column<(id_new = )>, Column<(gender = )>, Column<(salary = )>]

>>> " | ".join([ find_empty_rows(x) for x in df1.columns ])
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: sequence item 0: expected string, Column found



>>> def find_empty_rows(x):
...     return '{}==""'.format(x)
>>> expr = " or ".join([ find_empty_rows(x) for x in df1.columns ])
>>> expr
'id=="" or firstname=="" or middlename=="" or lastname=="" or id_new=="" or gender=="" or salary==""'
>>> df1.filter( expr ).show()
+---+---------+----------+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|
|  2|  Michael|      Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|
|  5|      Jen|      Mary|   Brown|      |     F|    -1|
+---+---------+----------+--------+------+------+------+
