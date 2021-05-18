
************************************************************************ 
PySpark UDFs with Dictionary Arguments
*************************************************************************

1. Passing a dictionary argument to a PySpark UDF is a powerful programming technique that’ll enable you to implement some complicated algorithms that scale.
2.  Broadcasting values and writing UDFs can be tricky. UDFs only accept arguments that are column objects and dictionaries aren’t column objects. 
	This blog post shows you the nested function work-around that’s necessary for passing a dictionary to a UDF. 
	It’ll also show you how to broadcast a dictionary and why broadcasting is important in a cluster environment.
3. Several approaches that do not work and the accompanying error messages are also presented, so you can learn more about how Spark works.

=======================================================================================================
Approch 1:
============================ You can’t pass a dictionary as a UDF argument ==============================

@F.udf(returnType=StringType())
def state_abbreviation(s, mapping):
    if s is not None:
        return mapping[s]


import pyspark.sql.functions as F

df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}

df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), mapping)).show()
'''
>>> df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), mapping)).show()
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p3777.2006039/lib/spark/python/pyspark/sql/udf.py", line 189, in wrapper
    return self(*args)
  File "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p3777.2006039/lib/spark/python/pyspark/sql/udf.py", line 169, in __call__
    return Column(judf.apply(_to_seq(sc, cols, _to_java_column)))
  File "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p3777.2006039/lib/spark/python/pyspark/sql/column.py", line 65, in _to_seq
    cols = [converter(c) for c in cols]
  File "/opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p3777.2006039/lib/spark/python/pyspark/sql/column.py", line 53, in _to_java_column
    "function.".format(col, type(col)))
TypeError: Invalid argument, not a string or column: {'Alabama': 'AL', 'Texas': 'TX'} of type <type 'dict'>. For column literals, use 'lit', 'array', 'struct' or 'create_map' function.
'''


Note : The create_map function sounds like a promising solution in our case, but that function doesn’t help.
Let’s see if the lit function can help.
df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), lit(mapping))).show()
This doesn’t work either and errors out with this message:
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.sql.functions.lit: 
java.lang.RuntimeException: Unsupported literal type class java.util.HashMap {Texas=TX, Alabama=AL}

Note : The lit() function doesn’t work with dictionaries.
Let’s try broadcasting the dictionary with the pyspark.sql.functions.broadcast() method and see if that helps.

df.withColumn('state_abbreviation', state_abbreviation(F.col('state'), F.broadcast(mapping))).show()

Broadcasting in this manner doesn’t help and yields this error message: AttributeError: 'dict' object has no attribute '_jdf'.

Broadcasting with spark.sparkContext.broadcast() will also error out. You need to approach the problem differently.


=======================================================================================================
Approch 2:
============================================================== Simple solution ===============
def working_fun(mapping):
    def f(x):
        return mapping.get(x)
    return F.udf(f)


df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}

df.withColumn('state_abbreviation', working_fun(mapping)(F.col('state'))).show()

+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+



This approach works if the dictionary is defined in the codebase (if the dictionary is defined in a Python project that’s packaged in a wheel file and 
attached to a cluster for example). This code will not work in a cluster environment if the dictionary hasn’t been spread to all the nodes in the cluster. 
It’s better to explicitly broadcast the dictionary to make sure it’ll work when run on a cluster.


=======================================================================================================
Approch 3:
=============================================== Broadcast solution ==========================================

def working_fun(mapping_broadcasted):
    def f(x):
        return mapping_broadcasted.value.get(x)
    return F.udf(f)

df = spark.createDataFrame([
    ['Alabama',],
    ['Texas',],
    ['Antioquia',]
]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}
b = spark.sparkContext.broadcast(mapping)

df.withColumn('state_abbreviation', working_fun(b)(F.col('state'))).show()

+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+


Take note that you need to use value to access the dictionary in mapping_broadcasted.value.get(x). 
If you try to run mapping_broadcasted.get(x), you’ll get this error message: AttributeError: 'Broadcast' object has no attribute 'get'. 
You’ll see that error message whenever your trying to access a variable that’s been broadcasted and forget to call value.
Explicitly broadcasting is the best and most reliable way to approach this problem. The dictionary should be explicitly broadcasted, 
even if it is defined in your code.

============================================  Creating dictionaries to be broadcasted  ============================

You’ll typically read a dataset from a file, convert it to a dictionary, 
broadcast the dictionary, and then access the broadcasted variable in your code.

df = spark.read.option('header', True).csv(word_prob_path)
word_prob = {x['word']: x['word_prob'] for x in df.select('word', 'word_prob').collect()}
word_prob_b = spark.sparkContext.broadcast(word_prob)

import quinn
word_prob = quinn.two_columns_to_dictionary(df, 'word', 'word_prob')
word_prob_b = spark.sparkContext.broadcast(word_prob)


============================================  Broadcast limitations ======================================

The broadcast size limit was 2GB and was increased to 8GB as of Spark 2.4, see here. 
Big dictionaries can be broadcasted, but you’ll need to investigate alternate solutions if that dataset you need to broadcast is truly massive.




=================================================================================================================
My analysis

def working_fun(mapping_broadcasted):
    def f(x):
        if(mapping_broadcasted.value.get(x)):
        	return True
        else:
        	return False
    return F.udf(f)

df = spark.createDataFrame([['Alabama',],['Texas',],['Antioquia',]]).toDF('state')

mapping = {'Alabama': 'AL', 'Texas': 'TX'}
b = spark.sparkContext.broadcast(mapping)

df.withColumn('state_abbreviation', working_fun(b)(F.col('state'))).show()

'''
>>> def working_fun(mapping_broadcasted):
...     def f(x):
...         if(mapping_broadcasted.value.get(x)):
...             return True
...         else:
...             return False
...     return F.udf(f)
...
>>>
>>> df = spark.createDataFrame([['Alabama',],['Texas',],['Antioquia',]]).toDF('state')
>>> mapping = {'Alabama': 'AL', 'Texas': 'TX'}
>>> b = spark.sparkContext.broadcast(mapping)
>>> df.withColumn('state_abbreviation', working_fun(b)(F.col('state'))).show()
+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|              true|
|    Texas|              true|
|Antioquia|             false|
+---------+------------------+
'''

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

df = spark.createDataFrame([['Alabama',],['Texas',],['Antioquia',]]).toDF('state')
mapping = {'Alabama': 'AL', 'Texas': 'TX'}
mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
df.withColumn("state_abbreviation", mapping_expr.getItem(col("state"))).show()

'''
>>> df = spark.createDataFrame([['Alabama',],['Texas',],['Antioquia',]]).toDF('state')
df.withColumn("state_abbreviation", mapping_expr.getItem(col("state"))).show()>>> mapping = {'Alabama': 'AL', 'Texas': 'TX'}
>>> mapping_expr = create_map([lit(x) for x in chain(*mapping.items())])
>>> df.withColumn("state_abbreviation", mapping_expr.getItem(col("state"))).show()
+---------+------------------+
|    state|state_abbreviation|
+---------+------------------+
|  Alabama|                AL|
|    Texas|                TX|
|Antioquia|              null|
+---------+------------------+
'''


mapping_expr = create_map( [ lit(x) for x in chain(*mapping.items()) ] )