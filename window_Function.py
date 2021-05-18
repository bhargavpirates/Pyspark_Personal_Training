
Windowing functions:
********************

1. Ranking   :  rank, dense_rank, percent_rank, ntile, row_number
2. Analytics :  CumeDist, firstValue, latValue, lag, lead
3. Aggregate :  avg,count,collect_list



from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

simpleData = (("James", "Sales", 3000),("Michael", "Sales", 4600),("Robert", "Sales", 4100),("Maria", "Finance", 3000),("James", "Sales", 3000),\
			  ("Scott", "Finance", 3300),("Jen", "Finance", 3900),("Jeff", "Marketing", 3000),("Kumar", "Marketing", 2000),("Saif", "Sales", 4100) )
 
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(data = simpleData, schema = columns)
df.show(truncate=False)

+-------------+----------+------+
|employee_name|department|salary|
+-------------+----------+------+
|James        |Sales     |3000  |
|Michael      |Sales     |4600  |
|Robert       |Sales     |4100  |
|Maria        |Finance   |3000  |
|James        |Sales     |3000  |
|Scott        |Finance   |3300  |
|Jen          |Finance   |3900  |
|Jeff         |Marketing |3000  |
|Kumar        |Marketing |2000  |
|Saif         |Sales     |4100  |
+-------------+----------+------+

df.orderBy("salary").show()
df.sortBy("salary").show()


window_df.select("*", F.lead("salary").over(Window.partitionBy("department").orderBy("salary")).alias("lead") ).show()



1. Ranking:
************
	a. row_number
		window_func=Window.partitionBy("department").orderBy("salary")
		df.withColumn("rn",F.row_number().over(window_func)).show()

			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        James|     Sales|  3000|  1|
			|        James|     Sales|  3000|  2|
			|       Robert|     Sales|  4100|  3|
			|         Saif|     Sales|  4100|  4|
			|      Michael|     Sales|  4600|  5|
			|        Maria|   Finance|  3000|  1|
			|        Scott|   Finance|  3300|  2|
			|          Jen|   Finance|  3900|  3|
			|        Kumar| Marketing|  2000|  1|
			|         Jeff| Marketing|  3000|  2|
			+-------------+----------+------+---+

		window_func_new=Window.orderBy("salary")
		df.withColumn("rn",F.row_number().over(window_func_new)).show()

			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        Kumar| Marketing|  2000|  1|
			|         Jeff| Marketing|  3000|  2|
			|        James|     Sales|  3000|  3|
			|        Maria|   Finance|  3000|  4|
			|        James|     Sales|  3000|  5|
			|        Scott|   Finance|  3300|  6|
			|          Jen|   Finance|  3900|  7|
			|         Saif|     Sales|  4100|  8|
			|       Robert|     Sales|  4100|  9|
			|      Michael|     Sales|  4600| 10|
			+-------------+----------+------+---+

	b. rank
		window_func=Window.partitionBy("department").orderBy("salary")
		df.withColumn("rn",F.rank().over(window_func)).show()
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        James|     Sales|  3000|  1|
			|        James|     Sales|  3000|  1|
			|       Robert|     Sales|  4100|  3|
			|         Saif|     Sales|  4100|  3|
			|      Michael|     Sales|  4600|  5|
			|        Maria|   Finance|  3000|  1|
			|        Scott|   Finance|  3300|  2|
			|          Jen|   Finance|  3900|  3|
			|        Kumar| Marketing|  2000|  1|
			|         Jeff| Marketing|  3000|  2|
			+-------------+----------+------+---+

		window_func_new=Window.orderBy("salary")
		df.withColumn("rn",F.rank().over(window_func_new)).show()
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        Kumar| Marketing|  2000|  1|
			|        James|     Sales|  3000|  2|
			|        Maria|   Finance|  3000|  2|
			|        James|     Sales|  3000|  2|
			|         Jeff| Marketing|  3000|  2|
			|        Scott|   Finance|  3300|  6|
			|          Jen|   Finance|  3900|  7|
			|       Robert|     Sales|  4100|  8|
			|         Saif|     Sales|  4100|  8|
			|      Michael|     Sales|  4600| 10|
			+-------------+----------+------+---+

		window_func=Window.partitionBy("department").orderBy(F.desc("salary"))
		df.withColumn("rn",F.rank().over(window_func)).show()
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|      Michael|     Sales|  4600|  1|
			|       Robert|     Sales|  4100|  2|
			|         Saif|     Sales|  4100|  2|
			|        James|     Sales|  3000|  4|
			|        James|     Sales|  3000|  4|
			|          Jen|   Finance|  3900|  1|
			|        Scott|   Finance|  3300|  2|
			|        Maria|   Finance|  3000|  3|
			|         Jeff| Marketing|  3000|  1|
			|        Kumar| Marketing|  2000|  2|
			+-------------+----------+------+---+			


	C. dense_rank
		window_func=Window.partitionBy("department").orderBy("salary")
		df.withColumn("rn",F.dense_rank().over(window_func)).show()		
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        James|     Sales|  3000|  1|
			|        James|     Sales|  3000|  1|
			|         Saif|     Sales|  4100|  2|
			|       Robert|     Sales|  4100|  2|
			|      Michael|     Sales|  4600|  3|
			|        Maria|   Finance|  3000|  1|
			|        Scott|   Finance|  3300|  2|
			|          Jen|   Finance|  3900|  3|
			|        Kumar| Marketing|  2000|  1|
			|         Jeff| Marketing|  3000|  2|
			+-------------+----------+------+---+

		window_func=Window.partitionBy("department").orderBy(F.desc("salary"))
		df.withColumn("rn",F.dense_rank().over(window_func)).show()			
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|      Michael|     Sales|  4600|  1|
			|         Saif|     Sales|  4100|  2|
			|       Robert|     Sales|  4100|  2|
			|        James|     Sales|  3000|  3|
			|        James|     Sales|  3000|  3|
			|          Jen|   Finance|  3900|  1|
			|        Scott|   Finance|  3300|  2|
			|        Maria|   Finance|  3000|  3|
			|         Jeff| Marketing|  3000|  1|
			|        Kumar| Marketing|  2000|  2|
			+-------------+----------+------+---+

		window_func_new=Window.orderBy("salary")
		df.withColumn("rn",F.dense_rank().over(window_func_new)).show()
		**********************************************************************************************************************************************************************************
		*  Note:21/02/24 14:33:55 WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
		**********************************************************************************************************************************************************************************
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|        Kumar| Marketing|  2000|  1|
			|         Jeff| Marketing|  3000|  2|
			|        James|     Sales|  3000|  2|
			|        Maria|   Finance|  3000|  2|
			|        James|     Sales|  3000|  2|
			|        Scott|   Finance|  3300|  3|
			|          Jen|   Finance|  3900|  4|
			|         Saif|     Sales|  4100|  5|
			|       Robert|     Sales|  4100|  5|
			|      Michael|     Sales|  4600|  6|
			+-------------+----------+------+---+		

		window_func_new=Window.orderBy(F.desc("salary"))
		df.withColumn("rn",F.dense_rank().over(window_func_new)).show()
			+-------------+----------+------+---+
			|employee_name|department|salary| rn|
			+-------------+----------+------+---+
			|      Michael|     Sales|  4600|  1|
			|         Saif|     Sales|  4100|  2|
			|       Robert|     Sales|  4100|  2|
			|          Jen|   Finance|  3900|  3|
			|        Scott|   Finance|  3300|  4|
			|         Jeff| Marketing|  3000|  5|
			|        James|     Sales|  3000|  5|
			|        Maria|   Finance|  3000|  5|
			|        James|     Sales|  3000|  5|
			|        Kumar| Marketing|  2000|  6|
			+-------------+----------+------+---+

	4. percent_rank
		window_func=Window.partitionBy("department").orderBy("salary")
		df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()
			+-------------+----------+------+------------+
			|employee_name|department|salary|percent_rank|
			+-------------+----------+------+------------+
			|        James|     Sales|  3000|         0.0|
			|        James|     Sales|  3000|         0.0|
			|         Saif|     Sales|  4100|         0.5|
			|       Robert|     Sales|  4100|         0.5|
			|      Michael|     Sales|  4600|         1.0|
			|        Maria|   Finance|  3000|         0.0|
			|        Scott|   Finance|  3300|         0.5|
			|          Jen|   Finance|  3900|         1.0|
			|        Kumar| Marketing|  2000|         0.0|
			|         Jeff| Marketing|  3000|         1.0|
			+-------------+----------+------+------------+

Aggergate Functions:
*******************

a. Basic Frame with partitionBy:
	Created with Window.partitionBy on one or more columns
	Each row has a corresponding frame
	The frame will be the same for every row in the same within the same partition. (NOTE: This will NOT be the case with Ordered Frame)
	Aggregate/Window functions can be applied on each row+frame to generate a single value

		windowSpecAgg  = Window.partitionBy("department")
		>>> df.withColumn("row",F.row_number().over(window_func)) \
		...   .withColumn("avg", F.avg("salary").over(windowSpecAgg)) \
		...   .withColumn("sum", F.sum("salary").over(windowSpecAgg)) \
		...   .withColumn("min", F.min("salary").over(windowSpecAgg)) \
		...   .withColumn("max", F.max("salary").over(windowSpecAgg)) \
		...   .select("department","avg","sum","min","max") \
		...   .show()
		+----------+------+-----+----+----+
		|department|   avg|  sum| min| max|
		+----------+------+-----+----+----+
		|     Sales|3760.0|18800|3000|4600|
		|     Sales|3760.0|18800|3000|4600|
		|     Sales|3760.0|18800|3000|4600|
		|     Sales|3760.0|18800|3000|4600|
		|     Sales|3760.0|18800|3000|4600|
		|   Finance|3400.0|10200|3000|3900|
		|   Finance|3400.0|10200|3000|3900|
		|   Finance|3400.0|10200|3000|3900|
		| Marketing|2500.0| 5000|2000|3000|
		| Marketing|2500.0| 5000|2000|3000|
		+----------+------+-----+----+----+


b. Ordered Frame with partitionBy and orderBy
	Created with Window.partitionBy on one or more columns
	Followed by orderBy on a column
	Each row have a corresponding frame
	The frame will NOT be the same for every row within the same partition. By default, the frame contains all previous rows and the currentRow.
	Aggregate/Window functions can be applied to each row+frame to generate a value
		
		windowSpecAgg  = Window.partitionBy("department").orderBy("salary")
		>>> df.withColumn("row",F.row_number().over(window_func)) \
		...   .withColumn("avg", F.avg("salary").over(windowSpecAgg)) \
		...   .withColumn("sum", F.sum("salary").over(windowSpecAgg)) \
		...   .withColumn("min", F.min("salary").over(windowSpecAgg)) \
		...   .withColumn("max", F.max("salary").over(windowSpecAgg)) \
		...   .withColumn("max", F.collect_list("salary").over(windowSpecAgg)) \
		...   .select("employee_name","salary","department","avg","sum","min","max") \
		...   .show(truncate=False)
		+-------------+------+----------+------+-----+----+------------------------------+
		|employee_name|salary|department|avg   |sum  |min |max                           |
		+-------------+------+----------+------+-----+----+------------------------------+
		|James        |3000  |Sales     |3000.0|6000 |3000|[3000, 3000]                  |
		|James        |3000  |Sales     |3000.0|6000 |3000|[3000, 3000]                  |
		|Saif         |4100  |Sales     |3550.0|14200|3000|[3000, 3000, 4100, 4100]      |
		|Robert       |4100  |Sales     |3550.0|14200|3000|[3000, 3000, 4100, 4100]      |
		|Michael      |4600  |Sales     |3760.0|18800|3000|[3000, 3000, 4100, 4100, 4600]|
		|Maria        |3000  |Finance   |3000.0|3000 |3000|[3000]                        |
		|Scott        |3300  |Finance   |3150.0|6300 |3000|[3000, 3300]                  |
		|Jen          |3900  |Finance   |3400.0|10200|3000|[3000, 3300, 3900]            |
		|Kumar        |2000  |Marketing |2000.0|2000 |2000|[2000]                        |
		|Jeff         |3000  |Marketing |2500.0|5000 |2000|[2000, 3000]                  |
		+-------------+------+----------+------+-----+----+------------------------------+

Notes : From the output we can see that column salaries by function collect_list does NOT have the same values in a window. 
		The values are only from unboundedPreceding until currentRow.
		The average_salary and total_salary are not over the whole department, 
		but average and total for the salary higher or equal than currentRow’s salary.


	3.	windowSpecAgg  = Window.partitionBy("department")
		#from pyspark.sql.functions import col,avg,sum,min,max,row_number 
		df.withColumn("row",F.row_number().over(windowSpec)) \
		  .withColumn("avg", F.avg(col("salary")).over(windowSpecAgg)) \
		  .withColumn("sum", F.sum(col("salary")).over(windowSpecAgg)) \
		  .withColumn("min", F.min(col("salary")).over(windowSpecAgg)) \
		  .withColumn("max", F.max(col("salary")).over(windowSpecAgg)) \
		  .where(col("row")==1).select("department","avg","sum","min","max") \
		  .show()

			+----------+------+-----+----+----+
			|department|   avg|  sum| min| max|
			+----------+------+-----+----+----+
			|     Sales|3760.0|18800|3000|4600|
			|   Finance|3400.0|10200|3000|3900|
			| Marketing|2500.0| 5000|2000|3000|
			+----------+------+-----+----+----+

'''
df.withColumn("row",F.row_number().over(window_func)) \
  .withColumn("avg", F.avg("salary").over(windowSpecAgg)) \
  .withColumn("sum", F.sum("salary").over(windowSpecAgg)) \
  .withColumn("min", F.min("salary").over(windowSpecAgg)) \
  .withColumn("max", F.max("salary").over(windowSpecAgg)) \
  .where(F.col("row")==1).select("department","avg","sum","min","max") \
  .explain()

== Physical Plan ==
*(3) Project [department#1, avg#370, sum#377L, min#385L, max#394L]
+- *(3) Filter (isnotnull(row#364) && (row#364 = 1))
   +- Window [avg(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg#370, sum(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sum#377L, min(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS min#385L, max(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS max#394L], [department#1]
      +- Window [row_number() windowspecdefinition(department#1, salary#2L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row#364], [department#1], [salary#2L ASC NULLS FIRST]
         +- *(2) Sort [department#1 ASC NULLS FIRST, salary#2L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(department#1, 200)
               +- *(1) Project [department#1, salary#2L]
                  +- Scan ExistingRDD[employee_name#0,department#1,salary#2L]

F.avg("salary").over(windowSpecAgg),F.sum("salary").over(windowSpecAgg),F.min("salary").over(windowSpecAgg),F.max("salary").over(windowSpecAgg)

df.select(F.row_number().over(window_func).alias("row"),F.avg("salary").over(windowSpecAgg).alias("avg"),F.sum("salary").over(windowSpecAgg).alias("sum"),F.min("salary").over(windowSpecAgg).alias("min"),F.max("salary").over(windowSpecAgg).alias("max")).where(F.col("row")==1).show()

>>> df.select(F.row_number().over(window_func).alias("row"),F.avg("salary").over(windowSpecAgg).alias("avg"),F.sum("salary").over(windowSpecAgg).alias("sum"),F.min("salary").over(windowSpecAgg).alias("min"),F.max("salary").over(windowSpecAgg).alias("max")).where(F.col("row")==1).explain()
== Physical Plan ==
*(3) Project [row#455, avg#457, sum#459L, min#461L, max#463L]
+- *(3) Filter (isnotnull(row#455) && (row#455 = 1))
   +- Window [avg(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS avg#457, sum(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS sum#459L, min(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS min#461L, max(salary#2L) windowspecdefinition(department#1, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS max#463L], [department#1]
      +- Window [row_number() windowspecdefinition(department#1, salary#2L ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row#455], [department#1], [salary#2L ASC NULLS FIRST]
         +- *(2) Sort [department#1 ASC NULLS FIRST, salary#2L ASC NULLS FIRST], false, 0
            +- Exchange hashpartitioning(department#1, 200)
               +- *(1) Project [department#1, salary#2L]
                  +- Scan ExistingRDD[employee_name#0,department#1,salary#2L]
'''


3. Analytics:
***************
 Lag:
 ----

 		windowSpecAgg  = Window.partitionBy("department").orderBy("salary")
		#df.withColumn("row",F.lag("salary",1).over(windowSpecAgg)).show(truncate=False)
		>>> df.withColumn("row",F.lag("salary",1).over(windowSpecAgg)).show(truncate=False)
		+-------------+----------+------+----+
		|employee_name|department|salary|row |
		+-------------+----------+------+----+
		|James        |Sales     |3000  |null|
		|James        |Sales     |3000  |3000|
		|Robert       |Sales     |4100  |3000|
		|Saif         |Sales     |4100  |4100|
		|Michael      |Sales     |4600  |4100|
		|Maria        |Finance   |3000  |null|
		|Scott        |Finance   |3300  |3000|
		|Jen          |Finance   |3900  |3300|
		|Kumar        |Marketing |2000  |null|
		|Jeff         |Marketing |3000  |2000|
		+-------------+----------+------+----+

		windowSpecAgg  = Window.partitionBy("department")
		df.withColumn("row",F.lag("salary",1).over(windowSpecAgg)).show(truncate=False)
		Error: pyspark.sql.utils.AnalysisException: u'Window function lag(salary#2L, 1, null) requires window to be ordered, please add ORDER BY clause. For example SELECT lag(salary#2L, 1, null)(value_expr) OVER (PARTITION BY window_partition ORDER BY window_ordering) from table;'


>>> df.withColumn("row",F.lag("salary",1,0).over(windowSpecAgg)).show(truncate=False)
+-------------+----------+------+----+
|employee_name|department|salary|row |
+-------------+----------+------+----+
|James        |Sales     |3000  |0   |
|James        |Sales     |3000  |3000|
|Saif         |Sales     |4100  |3000|
|Robert       |Sales     |4100  |4100|
|Michael      |Sales     |4600  |4100|
|Maria        |Finance   |3000  |0   |
|Scott        |Finance   |3300  |3000|
|Jen          |Finance   |3900  |3300|
|Kumar        |Marketing |2000  |0   |
|Jeff         |Marketing |3000  |2000|
+-------------+----------+------+----+






RangeFrame:
**********
We can use range functions to change frame boundary.

. Create with Window.partitionBy on one or more columns
. It usually has orderBy so that the data in the frame is ordered.
. Then followed by rangeBetween or rowsBetween
. Each row will have a corresponding frame
. Frame boundary can be controlled by rangeBetween or rowsBetween
. Aggregate/Window functions can be applied on each row+frame to generate a single value


1. rowsBetween
-----------------
rowsBetween(start, end)
rangeBetween(start, end)

both functions accept two parameters, [start, end] all inclusive. 
The parameters value can be Window.unboundedPreceding,Window.unboundedFollowing, and Window.currentRow. 
Or a value relative to Window.currentRow, either negtive or positive.

##rowsBetween get the frame boundary based on the row index in the window compared to currentRow. 
##here are a few examples and it’s meaning.
.rowsBetween(Window.currentRow,1)                                   -->  current_row and next_row                                                               
.rowsBetween(Window.currentRow,2)                                   -->  current_row and next_2_rows                        
.rowsBetween(-1, Window.currentRow)                                 -->  previous_row and current_row                          
.rowsBetween(-2, Window.currentRow)                                 -->  previous_2_row and current_row                          
.rowsBetween(-1, 1)                                                 -->  previous_row, current_row , next_row          
.rowsBetween(Window.unboundedpreceding, Window.currentRow)			-->  all_previous_row and current_row
.rowsBetween(Window.currentRow, Window.unboundedFollowing)			-->  current_row and next_following_all_rows
.rowsBetween(Window.unboundedpreceding, Window.unboundedFollowing)	-->  all_rows_in_window		

Window.currentRow=0
Window.unboundedpreceding =long.MinValue
Window.unboundedFollowing =long.MaxValue

Exmp:
 	windowSpecAgg  = Window.partitionBy("department").rowsBetween(Window.currentRow,1)  
	df.select(F.collect_list("salary").over(windowSpecAgg), F.sum("salary").over(windowSpecAgg)).show(truncate=False)

	>>> df.select("*", F.collect_list("salary").over(windowSpecAgg).alias("collect_list"), F.sum("salary").over(windowSpecAgg).alias("sum")).show(truncate=False)
	+-------------+----------+------+------------+----+
	|employee_name|department|salary|collect_list|sum |
	+-------------+----------+------+------------+----+
	|James        |Sales     |3000  |[3000, 4600]|7600|
	|Michael      |Sales     |4600  |[4600, 4100]|8700|
	|Robert       |Sales     |4100  |[4100, 3000]|7100|
	|James        |Sales     |3000  |[3000, 4100]|7100|
	|Saif         |Sales     |4100  |[4100]      |4100|
	|Maria        |Finance   |3000  |[3000, 3300]|6300|
	|Scott        |Finance   |3300  |[3300, 3900]|7200|
	|Jen          |Finance   |3900  |[3900]      |3900|
	|Jeff         |Marketing |3000  |[3000, 2000]|5000|
	|Kumar        |Marketing |2000  |[2000]      |2000|
	+-------------+----------+------+------------+----+


	windowSpecAgg  = Window.partitionBy("department").rowsBetween(-2,Window.currentRow) 
	>>> df.select("*", F.collect_list("salary").over(windowSpecAgg).alias("collect_list"), F.sum("salary").over(windowSpecAgg).alias("sum")).show(truncate=False)
	+-------------+----------+------+------------------+-----+
	|employee_name|department|salary|collect_list      |sum  |
	+-------------+----------+------+------------------+-----+
	|Saif         |Sales     |4100  |[4100]            |4100 |
	|James        |Sales     |3000  |[4100, 3000]      |7100 |
	|Michael      |Sales     |4600  |[4100, 3000, 4600]|11700|
	|Robert       |Sales     |4100  |[3000, 4600, 4100]|11700|
	|James        |Sales     |3000  |[4600, 4100, 3000]|11700|
	|Maria        |Finance   |3000  |[3000]            |3000 |
	|Scott        |Finance   |3300  |[3000, 3300]      |6300 |
	|Jen          |Finance   |3900  |[3000, 3300, 3900]|10200|
	|Jeff         |Marketing |3000  |[3000]            |3000 |
	|Kumar        |Marketing |2000  |[3000, 2000]      |5000 |
	+-------------+----------+------+------------------+-----+


	windowSpecAgg  = Window.partitionBy("department").rowsBetween(Window.unboundedpreceding, Window.unboundedFollowing)
	>>> windowSpecAgg  = Window.partitionBy("department").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
	>>> df.select("*", F.collect_list("salary").over(windowSpecAgg).alias("collect_list"), F.sum("salary").over(windowSpecAgg).alias("sum")).show(truncate=False)
	+-------------+----------+------+------------------------------+-----+
	|employee_name|department|salary|collect_list                  |sum  |
	+-------------+----------+------+------------------------------+-----+
	|Saif         |Sales     |4100  |[4100, 3000, 4600, 4100, 3000]|18800|
	|James        |Sales     |3000  |[4100, 3000, 4600, 4100, 3000]|18800|
	|Michael      |Sales     |4600  |[4100, 3000, 4600, 4100, 3000]|18800|
	|Robert       |Sales     |4100  |[4100, 3000, 4600, 4100, 3000]|18800|
	|James        |Sales     |3000  |[4100, 3000, 4600, 4100, 3000]|18800|
	|Scott        |Finance   |3300  |[3300, 3900, 3000]            |10200|
	|Jen          |Finance   |3900  |[3300, 3900, 3000]            |10200|
	|Maria        |Finance   |3000  |[3300, 3900, 3000]            |10200|
	|Jeff         |Marketing |3000  |[3000, 2000]                  |5000 |
	|Kumar        |Marketing |2000  |[3000, 2000]                  |5000 |
	+-------------+----------+------+------------------------------+-----+



1. rangeBetween
-----------------

Spark >= 2.3
------------
spark.sql(
    """ SELECT *, mean(some_value) OVER (
        PARTITION BY id 
        ORDER BY CAST(start AS timestamp) 
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
     ) AS mean FROM df """).show()


## +---+----------+----------+------------------+       
## | id|     start|some_value|              mean|
## +---+----------+----------+------------------+
## |  1|2015-01-01|      20.0|              20.0|
## |  1|2015-01-06|      10.0|              15.0|
## |  1|2015-01-07|      25.0|18.333333333333332|
## |  1|2015-01-12|      30.0|21.666666666666668|
## |  2|2015-01-01|       5.0|               5.0|
## |  2|2015-01-03|      30.0|              17.5|
## |  2|2015-02-01|      20.0|              20.0|
## +---+----------+----------+------------------+


Spark < 2.3
------------

from pyspark.sql.window import Window
from pyspark.sql.functions import mean, col
# Hive timestamp is interpreted as UNIX timestamp in seconds*
days = lambda i: i * 86400 

w = (Window()
   .partitionBy(col("id"))
   .orderBy(col("start").cast("timestamp").cast("long"))
   .rangeBetween(-days(7), 0))

df.select(col("*"), mean("some_value").over(w).alias("mean")).show()


Timeseries Data:
*****************
As an example, let’s say I want to calculate the average purchase price over the past 30 days for each single purchase. 
From the example below on line 13, 17.5 = ( 5 + 30) /2 since the two purchases were within 30 days. 
Also we see a 40 = 40 / 1 , because the vacuum was the only product purchased in its look back period of 30 days.


days = lambda i: i * 86400 # 86400 seconds in a day  

w0 = Window.partitionBy('name')
df.withColumn('unix_time',F.col('date').cast('timestamp').cast('long'))\
  .withColumn('30day_moving_avg', F.avg('price').over(w0.orderBy(F.col('unix_time')).rangeBetween(-days(30),0)))\
  .withColumn('45day_moving_stdv',F.stddev('price').over(w0.orderBy(F.col('unix_time')).rangeBetween(-days(30),days(15))))\
  .show()

'''
+----+----------+---------+-----+----------+----------------+------------------+
|name|      date|  product|price| unix_time|30day_moving_avg| 45day_moving_stdv|
+----+----------+---------+-----+----------+----------------+------------------+
|Alex|2018-02-18|   Gloves|    5|1518933600|             5.0| 17.67766952966369|
|Alex|2018-03-03|  Brushes|   30|1520056800|            17.5| 17.67766952966369|
|Alex|2018-04-02|   Ladder|   20|1522645200|            25.0|7.0710678118654755|
|Alex|2018-06-22|    Stool|   20|1529643600|            20.0|               NaN|
|Alex|2018-07-12|   Bucket|    5|1531371600|            12.5|10.606601717798213|
|Alex|2018-09-26|Sandpaper|   10|1537938000|            10.0| 49.49747468305833|
|Alex|2018-10-10|    Paint|   80|1539147600|            45.0| 49.49747468305833|
|Alex|2018-12-09|   Vacuum|   40|1544335200|            40.0|               NaN|
+----+----------+---------+-----+----------+----------------+------------------+
'''