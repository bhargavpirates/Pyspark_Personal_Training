CREATE TABLE mytable ( 
         name string,
         city string,
         employee_id int ) 
PARTITIONED BY (year STRING, month STRING, day STRING) 
CLUSTERED BY (employee_id) INTO 256 BUCKETS

Q: Partitioning:
A:	Partitioning data is often used for distributing load horizontally, this has performance benefit, and helps in organizing data in a logical fashion.
	
	So, you have to be careful when partitioning, because if you for instance partition by employee_id and you have millions of employees, 
	youll end up having millions of directories in your file system. The term 'cardinality' refers to the number of possible value a field can have. 
	For instance, if you have a 'country' field, the countries in the world are about 300, so cardinality would be ~300. 
	For a field like 'timestamp_ms', which changes every millisecond, cardinality can be billions. 
	In general, when choosing a field for partitioning, it should not have a high cardinality, because you'll end up with way too many directories in your file system.

	PRO: partitions  dramatically cut the amount of data you are querying. 
		 if you want to query only from a certain date forward, the partitioning by year/month/day is going to dramatically cut the amount of IO

Q: Clustering: 
	Clustering aka bucketing on the other hand, will result with a fixed number of files, since you do specify the number of buckets. 
	What hive will do is to take the field, calculate a hash and assign a record to that bucket. 
	But what happens if you use let say 256 buckets and the field you're bucketing on has a low cardinality (for instance, it's a US state, so can be only 50 different values) ? 
	You will have 50 buckets with data, and 206 buckets with no data.

	PRO: bucketing can speed up joins with other tables that have exactly the same bucketing, 
		 so in my example, if you are joining two tables on the same employee_id, hive can do the join bucket by bucket

NOTE:
******
a.  So, bucketing works well when the field has high cardinality and data is evenly distributed among buckets. 
	Partitioning works best when the cardinality of the partitioning field is not too high.

b.	Also, you can partition on multiple fields, with an order (year/month/day is a good example), 
	while you can bucket on only one field.



======================================================================================================================================

Assume that Input File (100 GB) is loaded into temp-hive-table and it contains bank data from across different geographies.

Hive table without Partition:
******************************
Insert into Hive table Select * from temp-hive-table

/hive-table-path/part-00000-1  (part size ~ hdfs block size)
/hive-table-path/part-00000-2
....
/hive-table-path/part-00000-n


Hive table with Partition:
***************************
Insert into Hive table partition(country) Select * from temp-hive-table

/hive-table-path/country=US/part-00000-1       (file size ~ 10 GB)
/hive-table-path/country=Canada/part-00000-2   (file size ~ 20 GB)
....
/hive-table-path/country=UK/part-00000-n       (file size ~ 5 GB)


Hive table with Partition and Bucketing:
****************************************
Insert into Hive table partition(country) Select * from temp-hive-table

/hive-table-path/country=US/part-00000-1       (file size ~ 2 GB)
/hive-table-path/country=US/part-00000-2       (file size ~ 2 GB)
/hive-table-path/country=US/part-00000-3       (file size ~ 2 GB)
/hive-table-path/country=US/part-00000-4       (file size ~ 2 GB)
/hive-table-path/country=US/part-00000-5       (file size ~ 2 GB)

/hive-table-path/country=Canada/part-00000-1   (file size ~ 4 GB)
/hive-table-path/country=Canada/part-00000-2   (file size ~ 4 GB)
/hive-table-path/country=Canada/part-00000-3   (file size ~ 4 GB)
/hive-table-path/country=Canada/part-00000-4   (file size ~ 4 GB)
/hive-table-path/country=Canada/part-00000-5   (file size ~ 4 GB)

....
/hive-table-path/country=UK/part-00000-1       (file size ~ 1 GB)
/hive-table-path/country=UK/part-00000-2       (file size ~ 1 GB)
/hive-table-path/country=UK/part-00000-3       (file size ~ 1 GB)
/hive-table-path/country=UK/part-00000-4       (file size ~ 1 GB)
/hive-table-path/country=UK/part-00000-5       (file size ~ 1 GB)