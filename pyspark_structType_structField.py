*************************************************** simple StructType ****************************
data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)

Schema:
-------
root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

*************************************************** Nested StructType ****************************

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]
structureSchema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df2 = spark.createDataFrame(data=structureData,schema=structureSchema)
df2.printSchema()
df2.show(truncate=False)


Schema:
-------
root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)



 *******************************************  Creating StructType object struct from JSON file *************
Store DataFrame schema in File:
SToring DataFrme schema in Json :
df2.schema.json()

import json
schemaFromJson = StructType.fromJson(json.loads(schema.json))
val df3 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),schemaFromJson)
df3.printSchema() 

*************************************************Adding & Changing struct of the DataFrame ******************

updatedDF = df2.withColumn("OtherInfo", F.struct(
                                                col("id").alias("identifier"),
                                                col("gender").alias("gender"),
                                                col("salary").alias("salary"),
                                                when(col("salary").cast(IntegerType()) < 2000,"Low")
                                                .when(col("salary").cast(IntegerType()) < 4000,"Medium")
                                                .otherwise("High").alias("Salary_Grade")
                                            ))
    ( or )


updatedDF = df2.withColumn("OtherInfo", struct(
                                                col("id").alias("identifier"),
                                                col("gender").alias("gender"),
                                                col("salary").alias("salary"),
                                                when(col("salary").cast(IntegerType()) < 2000,"Low")
                                                .when(col("salary").cast(IntegerType()) < 4000,"Medium")
                                                .otherwise("High").alias("Salary_Grade")
                                            )
                            ).drop("id","gender","salary")

updatedDF.printSchema()
updatedDF.show(truncate=False)

root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- OtherInfo: struct (nullable = false)
 |    |-- identifier: string (nullable = true)
 |    |-- gender: string (nullable = true)
 |    |-- salary: integer (nullable = true)
 |    |-- Salary_Grade: string (nullable = false)


*****************************************Using SQL ArrayType and MapType ***************************

arrayStructureSchema = StructType([
                                    StructField('name', StructType([
                                                                        StructField('firstname', StringType(), True),
                                                                        StructField('middlename', StringType(), True),
                                                                        StructField('lastname', StringType(), True)
                                                                    ])),
                                    StructField('hobbies', ArrayType(StringType()), True),
                                    StructField('properties', MapType(StringType(),StringType()), True)
                                ])

root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- hobbies: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)


******************************************  Creating StructType object struct from DDL String ***********

  ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,`middle`: STRING>,`age` INT,`gender` STRING"
  ddlSchema = StructType.fromDDL(ddlSchemaStr)
  ddlSchema.printTreeString()


******************************************* Checking if a field exists in a DataFrame *******************

print(df.schema.fieldNames.contains("firstname"))
print(df.schema.contains(StructField("firstname",StringType,true)))  

