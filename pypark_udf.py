from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


Method1:
********
df = sqlContext.createDataFrame([{'name': 'Alice', 'age': 1}])

maturity_udf = udf(lambda age: "adult" if age >=18 else "child", StringType())
df.withColumn("maturity", maturity_udf(df.age))


Method2:
********
def maturity_func(x):
	return "adult" if x >=18 else "child"

maturity_udf = udf(maturity_func, StringType())
df.withColumn("maturity", maturity_udf(df.age))