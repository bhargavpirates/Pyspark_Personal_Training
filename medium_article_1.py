https://medium.com/analytics-vidhya/make-your-apache-spark-column-based-in-built-functions-more-dynamic-and-avoid-udfs-using-54486f1dbf47:

Make your Apache Spark column based in-built functions more dynamic and avoid UDFs, using expressions.



For example:
Correlation function:( takes 2 columns as inputs)
pyspark.sql.functions.corr(col1, col2)

Correlation will be computed of each row combo of col1 and col2
Substring function:(str(column), pos(value), len(value))
pyspark.sql.functions.substring(str, pos, len)


Therefore, unlike the str which can take a col, pos and len are literal values that will not change for every row.
Here, you can use expressions to make your substring function more dynamic, by allowing it to take pos and len values as entire columns, instead of assigning static literal values.
If you try to assign columns to your pos and len positions in PySpark syntax as shown above, you well get an error:

TypeError: Column is not iterable


What are expressions?
At face value, you can see that they allow you to write sql type syntax in your spark code. However, when you take a deeper look you will find that they have the ability to empower your spark in built functions.
You can use expressions in two ways, pypspark.sql.functions.expr or df.SelectExpr.
Some basic syntax:
df.selectExpr("age * 2", "abs(age)").collect()
df.select(expr("length(name)")).collect()
Among other things, Expressions basically allow you to input column values(col) in place of literal values which is not possible to do in the usual Pyspark api syntax shown in the docs.
Now I will provide examples using expressions and in built functions to tackle real world spark problems in a dynamic and scalable manner.
Example 1:
Suppose you have a DataFrame(df) with col1 column of StringType that you have to unpack and create two new columns(date and name)out of. We will use a more dynamic substring function to unpack our column.
Image for post
You can see that the date string is the same string length every time(9), however, the string length for the name is continuously changing(Mary:4,William:8, Benjamin:9).
A regular substring function from pyspark api will enable us to unpack the date, but for the name we will use an expression to execute the function.
from pyspark.sql import functions as F
df.withColumn(“Date”, F.substring(“col1”,1,9))\
.withColumn("name, F.expr(“””substr(col1,10,length(col1))”””)).show()
OR
from pyspark.sql import functions as F
df.withColumn("Date", F.substring("col1",1,9))\
.withColumn("Length", F.length("col1"))\
.withColumn("Name", F.expr("""substr(col1,10,Length)"""))\
.drop("Length").show()
Image for post
As you can see in bold, the two expressions (F.expr) allow you to provide a column (length(col1) or Length column) to your substring function which basically makes it dynamic for each row WITHOUT using a UDF(user defined function).
This will save tons of precious compute resources and time as you are empowering you spark inbuilt function to behave like a UDF without all the compute overhead associated with UDFS.
I have answered a similar question on stackoverflow: https://stackoverflow.com/questions/60426113/how-to-add-delimiters-to-a-csv-file/60428023#60428023. The reasoning to use an expression was a little different as the length of the string was not changing, it was rather out of laziness(I did not want to count the length of the string).
Example 2:
This example is actually straight out of a stackoverflow question I have answered: https://stackoverflow.com/questions/60494549/how-to-filter-a-column-in-a-data-frame-by-the-regex-value-of-another-column-in-s/60494657#60494657
Suppose you have a DataFrame with a column(query) of StringType that you have to apply a regexp_extract function to, and you have another column(regex_patt) which has all the patterns for that regex, row by row. If you didn’t know how to make your regexp_extract function dynamic for each row, you would build a UDF taking the two columns as input, and computing the regular expression for each row.(which will be very slow and cost inefficient).
Image for post
The question basically wants to filter out rows that do not match a given pattern.
The PySpark api has an inbuilt regexp_extract:
pyspark.sql.functions.regexp_extract(str, pattern, idx)
However, it only takes the str as a column, not the pattern. The pattern has to be specified as a static string value in the function. Therefore, we can use an expression to send a column to the pattern part of the function:
from pyspark.sql import functions as F
df.withColumn("query1", F.expr("""regexp_extract(query, regex_patt)""")).filter(F.col("query1")!='').drop("query1").show(truncate=False)
Image for post
The expression as shown in bold, allows us to apply the regex row by row and filter out the non matching row, hence row 2 was removed using the filter.
As stated above, if you try to put regex_patt as a column in your usual pyspark regexp_replace function syntax, you will get this error:
TypeError: Column is not iterable
Example 3:
Suppose you have a DataFrame shown below with a loan_date column(DateType) and days_to_make_payment column(IntegerType). You would like to compute the last date for payment, which would basically be adding the days column to the date column to get the new date.
Image for post
You can do this using the spark in-built date_add function:
pyspark.sql.functions.date_add(start, days)
It Returns the date that is days days after start. However, using this syntax, it only allows us to put the start as a column, and the days as a static integer value. Hence, we can use an expression to send the days_to_make_payment column as days into our function.
from pyspark.sql import functions as F
df.withColumn(“last_date_for_payment”, F.expr(“””date_add(Loan_date,days_to_make_payment)”””)).show()
Image for post
I would just like to reiterate for the last time that if you had used the usual pyspark syntax to put the days_to_make_payment to days like this:
from pyspark.sql import functions as F
df.withColumn("last_date_for_payment", F.date_add(F.col("Loan_date"),F.col("days_to_make_payment"))).show()
You would get this error:
TypeError: Column is not iterable
Conclusion:
Spark is the gold standard of big data processing engines and it has a vast open source community contributing to it all the time. It has a plethora of functions that can allow you to perform transformations at petabyte scale. With that said, one should be well aware of its limitations when it comes to UDFs(require moving data from the executor’s JVM to a Python interpreter) and Joins(shuffles data across partitions/cores), and one should always to try to push its in-built functions to their limits as they are highly optimized and scalable for big data tasks.