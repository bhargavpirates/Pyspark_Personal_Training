Date	Flag

1-Mar	
4-Mar	
5-Mar	bad
7-Mar	bad
10-Mar	
12-Mar	
13-Mar	bad

data=['1-Mar','4-Mar','5-Mar','7-Mar','10-Mar','12-Mar','13-Mar']
schema=['date']
df=spark.createDataFrame(data,schema=schema)


data=[[1],[4],[5],[7],[10],[12],[13]]
schema=['date']
df=spark.createDataFrame(data,schema=schema)

+----+
|date|
+----+
|   1|
|   4|
|   5|
|   7|
|  10|
|  12|
|  13|
+----+



df.createOrReplaceTempView("newdate")

sql("""
select date,flag from
	(select  date,flag,row_number() over(partition by date order by flag desc)  rnn from	
		(select d1_date,d2_date as date, case when(rn>=3) then "BAD" else "" end as flag
	  		from 
			(select d1.date as d1_date,d2.date as d2_date, row_number() over(partition by d1.date order by d2.date) as rn
				from 
				newdate d1
				join newdate d2
				on    (d1.date + 4) >= d2.date and d1.date<=d2.date
				order by d1.date,d2.date
			)a
		)b
	)c
 where rnn=1 order by date
""").show()