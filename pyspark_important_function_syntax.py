Syntax:

#Join:
#*****
1. join(other, on=None, how=None)

2. 	df.join(df2, 'name', 'outer').select('name', 'height').sort(desc("name")).collect()

3.  cond = [df.name == df3.name, df.age == df3.age]
	df.join(df3, cond, 'outer').select(df.name, df3.age).collect()

4.	df.join(df2, 'name').select(df.name, df2.height).collect()

5. 	df.join(df4, ['name', 'age']).select(df.name, df.age).collect()

eqNullSafe --> equity test safe for null values in join conditions  --> above 2.3x
<=>  --> equity test safe for null values in join conditions --> Be careful not to use it with Spark 1.5 or earlier. Prior to Spark 1.6 it required a Cartesian product (SPARK-11111 - Fast null-safe join)

6. df1.join(df2, df1["value"].eqNullSafe(df2["value"])).count()

#Alias in join :
#*************

df1.join(df2, df1['a'] == df2['a']).select(df1['f']).show(2)
##  +--------------------+
##  |                   f|
##  +--------------------+
##  |(5,[0,1,2,3,4],[0...|
##  |(5,[0,1,2,3,4],[0...|
##  +--------------------+

df1_a = df1.alias("df1_a")
df2_a = df2.alias("df2_a")

df1_a.join(df2_a, col('df1_a.a') == col('df2_a.a')).select('df1_a.f').show(2)

##  +--------------------+
##  |                   f|
##  +--------------------+
##  |(5,[0,1,2,3,4],[0...|
##  |(5,[0,1,2,3,4],[0...|
##  +--------------------+

df1_r = df1.select(*(col(x).alias(x + '_df1') for x in df1.columns))
df2_r = df2.select(*(col(x).alias(x + '_df2') for x in df2.columns))

df1_r.join(df2_r, col('a_df1') == col('a_df2')).select(col('f_df1')).show(2)

## +--------------------+
## |               f_df1|
## +--------------------+
## |(5,[0,1,2,3,4],[0...|
## |(5,[0,1,2,3,4],[0...|
## +--------------------+

#******************** ( or ) below way **********

df1.select(col("a") as "df1_a", col("f") as "df1_f").join(df2.select(col("a") as "df2_a", col("f") as "df2_f"), col("df1_a" === col("df2_a"))
38
The resulting DataFrame will have schema
(df1_a, df1_f, df2_a, df2_f)




#Pyspark Operators Presedence:
#*******************************
It is a matter of operator precedence. 
1.	The boolean OR operator or has lower precedence than the comparison operators so
 	col(my_column) < 'X' or col(my_column) > 'Y'
	reads as
	(col(my_column) < 'X') or (col(my_column) > 'Y')

2.	But the bitwise OR operator | has higher precedence than the comparison operators and
	col(my_column) < 'X' | col(my_column) > 'Y'
	actually reads as
	col(my_column) < ('X' | col(my_column)) > 'Y'

Note : 	Despite | being redefined on the Column type to have the same effect as the or operator, its precedence does not change, 
		so you need to manually parenthesise each subexpression.

Exp :: df_out = df.withColumn(my_column, when((col(my_column) < '1900-01-01') | (col(my_column) > '2019-12-09'), lit(None)).otherwise(col(my_column)))


#EmptyToNull for MultipleColumns:
#*******************************

#Create Expression Function:
def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(None)

#to_convert --> list of columns
to_convert = [column1,column2,...........]
exprs = [blank_as_null(x).alias(x) if x in to_convert else x for x in df.columns]
# Result exprs --> ['id_df1', 'firstname_df1', Column<CASE WHEN (NOT (middlename_df1 = )) THEN middlename_df1 ELSE NULL END AS `middlename_df1`>, Column<CASE WHEN (NOT (lastname_df1 = )) THEN lastname_df1 ELSE NULL END AS `lastname_df1`>, 'id_new_df1', 'gender_df1', 'salary_df1', 'id_df2', 'fname_df2', Column<CASE WHEN (NOT (mname_df2 = )) THEN mname_df2 ELSE NULL END AS `mname_df2`>, 'lname_df2', Column<CASE WHEN (NOT (id_new_df2 = )) THEN id_new_df2 ELSE NULL END AS `id_new_df2`>, 'gender_df2', 'salary_df2']
df.select(*exprs)

#Filter Empty Rows:
#*******************************


def find_empty_rows(x):
    return '{}==""'.format(x)

expr = " or ".join([ find_empty_rows(x) for x in df1.columns ])
# Result expr --> 'id=="" or firstname=="" or middlename=="" or lastname=="" or id_new=="" or gender=="" or salary==""'

df1.filter( expr ).show()
+---+---------+----------+--------+------+------+------+
| id|firstname|middlename|lastname|id_new|gender|salary|
+---+---------+----------+--------+------+------+------+
|  1|    James|          |   Smith| 36636|     M|  3000|
|  2|  Michael|      Rose|        | 40288|     M|  4000|
|  3|   Robert|          |Williams| 42114|     M|  4000|
|  5|      Jen|      Mary|   Brown|      |     F|    -1|
+---+---------+----------+--------+------+------+------+
