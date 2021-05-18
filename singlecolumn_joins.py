

INSERT INTO A VALUES
    ('Amy'),
    ('John'),
    ('Lisa'),
    ('Marco'),
    ('Phil');

INSERT INTO B VALUES
    ('Lisa'),
    ('Marco'),
    ('Phil'),
    ('Tim'),
    ('Vincent');

table1=[['Amy'],['John'],['Lisa'],['Marco'],['Phil']]
table2=[['Lisa'],['Marco'],['Phil'],['Tim'],['Vincent']]

df1=spark.createDataFrame(table1,schema=['names'])
df2=spark.createDataFrame(table2,schema=['names'])

#joins:
#1. Inner
df1.join(df2,df1.names==df2.names,how="inner")
df1.join(df2,on="names",how="inner")

>>> df1.join(df2,df1.names==df2.names,how="inner").show()
+-----+-----+
|names|names|
+-----+-----+
|Marco|Marco|
| Phil| Phil|
| Lisa| Lisa|
+-----+-----+

>>> df1.join(df2,on="names",how="inner").show()
+-----+
|names|
+-----+
|Marco|
| Phil|
| Lisa|
+-----+

#2. left
df1.join(df2,df1.names==df2.names,how="left").show()
df1.join(df2,on="names",how="left").show()

>>> df1.join(df2,df1.names==df2.names,how="left").show()
+-----+-----+
|names|names|
+-----+-----+
|Marco|Marco|
| Phil| Phil|
|  Amy| null|
| John| null|
| Lisa| Lisa|
+-----+-----+

>>> df1.join(df2,on="names",how="left").show()
+-----+
|names|
+-----+
|Marco|
| Phil|
|  Amy|
| John|
| Lisa|
+-----+


#3.Right
df1.join(df2,df1.names==df2.names,how="right").show()
df1.join(df2,on="names",how="right").show()


>>> df1.join(df2,df1.names==df2.names,how="right").show()
+-----+-------+
|names|  names|
+-----+-------+
|Marco|  Marco|
| Phil|   Phil|
| null|    Tim|
| null|Vincent|
| Lisa|   Lisa|
+-----+-------+

#4.Antileft

df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNull()).show()
df1.join(df2,on="names",how="left").filter(df2.names.isNull()).show()

>>> df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNull()).show()
+-----+-----+
|names|names|
+-----+-----+
|  Amy| null|
| John| null|
+-----+-----+

>>> df1.join(df2,on="names",how="left").filter(df2.names.isNull()).show()
+-----+
|names|
+-----+
|  Amy|
| John|
+-----+
------------------------------------------------------------------------------------

df1.join(df2,df1.names==df2.names,how="leftanti").show()
df1.join(df2,on="names",how="leftanti").show()

>>> df1.join(df2,df1.names==df2.names,how="leftanti").show()
+-----+
|names|
+-----+
|  Amy|
| John|
+-----+

>>> df1.join(df2,on="names",how="leftanti").show()
+-----+
|names|
+-----+
|  Amy|
| John|
+-----+




#5.left-semi

df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNotNull()).show()
df1.join(df2,on="names",how="left").filter(df2.names.isNotNull()).show()

>>> df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNotNull()).show()
+-----+-----+
|names|names|
+-----+-----+
|Marco|Marco|
| Phil| Phil|
| Lisa| Lisa|
+-----+-----+

>>> df1.join(df2,on="names",how="left").filter(df2.names.isNotNull()).show()
+-----+
|names|
+-----+
|Marco|
| Phil|
| Lisa|
+-----+

------------------------------------------------------------------------------------
df1.join(df2,df1.names==df2.names,how="leftsemi").show()
df1.join(df2,on="names",how="leftsemi").show()

>>> df1.join(df2,df1.names==df2.names,how="leftsemi").show()
+-----+
|names|
+-----+
|Marco|
| Phil|
| Lisa|
+-----+

>>> df1.join(df2,on="names",how="leftsemi").show()
+-----+
|names|
+-----+
|Marco|
| Phil|
| Lisa|
+-----+




#6. AntiRight
df1.join(df2,df1.names==df2.names,how="right").filter(df1.names.isNull()).show()
df1.join(df2,on="names",how="right").filter(df1.names.isNull()).show()

>>> df1.join(df2,df1.names==df2.names,how="right").filter(df1.names.isNull()).show()
+-----+-------+
|names|  names|
+-----+-------+
| null|    Tim|
| null|Vincent|
+-----+-------+

>>> df1.join(df2,on="names",how="right").filter(df1.names.isNull()).show()
+-------+
|  names|
+-------+
|    Tim|
|Vincent|
+-------+

------------------------------------------------------------------------------------

df1.join(df2,df1.names==df2.names,how="rightanti").show()
df1.join(df2,on="names",how="rightanti").show()


NO Rightanti or Rightlefy join Type in spark


#7.cross
df1.join(df2,df1.names==df2.names).show()   ---- Not Woring in above 2x spark 
df1.join(df2,on="names",how="cross").show()  ---- Not Woring in above 2x spark 

df1.crossJoin(df2).show()

>>> df1.crossJoin(df2).show()
+-----+-------+
|names|  names|
+-----+-------+
|  Amy|   Lisa|
|  Amy|  Marco|
| John|   Lisa|
| John|  Marco|
|  Amy|   Phil|
|  Amy|    Tim|
|  Amy|Vincent|
| John|   Phil|
| John|    Tim|
| John|Vincent|
| Lisa|   Lisa|
| Lisa|  Marco|
|Marco|   Lisa|
|Marco|  Marco|
| Phil|   Lisa|
| Phil|  Marco|
| Lisa|   Phil|
| Lisa|    Tim|
| Lisa|Vincent|
|Marco|   Phil|
+-----+-------+
only showing top 20 rows


df1.crossJoin(df2).show(count())

>>> countdf=df1.crossJoin(df2).count()
>>> countdf
25
>>> df1.crossJoin(df2).show(countdf)
+-----+-------+
|names|  names|
+-----+-------+
|  Amy|   Lisa|
|  Amy|  Marco|
| John|   Lisa|
| John|  Marco|
|  Amy|   Phil|
|  Amy|    Tim|
|  Amy|Vincent|
| John|   Phil|
| John|    Tim|
| John|Vincent|
| Lisa|   Lisa|
| Lisa|  Marco|
|Marco|   Lisa|
|Marco|  Marco|
| Phil|   Lisa|
| Phil|  Marco|
| Lisa|   Phil|
| Lisa|    Tim|
| Lisa|Vincent|
|Marco|   Phil|
|Marco|    Tim|
|Marco|Vincent|
| Phil|   Phil|
| Phil|    Tim|
| Phil|Vincent|
+-----+-------+


===========================================================================================================================

