from pyspark.sql import Row
row=Row("James",40)
print(row[0] +","+str(row[1]))

row=Row(name="Alice", age=11)
print(row.name) 


Person = Row("name", "age")
p1=Person("James", 40)
p2=Person("Alice", 35)
print(p1.name +","+p2.name)


***********************************************************************************************************
Way 1:
-------
data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"), 
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())



collData=rdd.collect()
for row in collData:
    print(row.name + "," +str(row.lang))

Way 2:
-------
Person=Row("name","lang","state")
data = [Person("James,,Smith",["Java","Scala","C++"],"CA"), 
    Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
    Person("Robert,,Williams",["CSharp","VB"],"NV")]

***********************************************************************************************************

Person=Row("name","lang","state")
data = [Person("James,,Smith",["Java","Scala","C++"],"CA"), 
    Person("Michael,Rose,",["Spark","Java","C++"],"NJ"),
    Person("Robert,,Williams",["CSharp","VB"],"NV")]

df=spark.createDataFrame(data)
df.printSchema()
df.show()
+----------------+------------------+-----+
|            name|              lang|state|
+----------------+------------------+-----+
|    James,,Smith|[Java, Scala, C++]|   CA|
|   Michael,Rose,|[Spark, Java, C++]|   NJ|
|Robert,,Williams|      [CSharp, VB]|   NV|
+----------------+------------------+-----+

columns = ["name","languagesAtSchool","currentState"]
df=spark.createDataFrame(data).toDF(*columns)
df.printSchema()

>>> df.show()
+----------------+------------------+------------+
|            name| languagesAtSchool|currentState|
+----------------+------------------+------------+
|    James,,Smith|[Java, Scala, C++]|          CA|
|   Michael,Rose,|[Spark, Java, C++]|          NJ|
|Robert,,Williams|      [CSharp, VB]|          NV|
+----------------+------------------+------------+
