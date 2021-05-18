CREATE TABLE dummy_new_lat3
(
  id BIGINT,
  name STRING,
  pets ARRAY <string>
)

insert into dummy_new_lat3 select 2, "apple", array("a", "d");
insert into dummy_new_lat3 select 2, "bat", array("c", "d");

+--------------------+----------------------+----------------------+
| dummy_new_lat3.id  | dummy_new_lat3.name  | dummy_new_lat3.pets  |
+--------------------+----------------------+----------------------+
| 1                  | apple                | ["a","b"]            |
| 2                  | bat                  | ["c","d"]            |
+--------------------+----------------------+----------------------+


***********************************************************
from pyspark.sql import functions as F 

schema = ['id','name','pets']
data=[[1,'apple',["a","b"]],[2,'bat  ',["c","d"]]]
df=spark.createDataFrame(data,schema)
df.show()
*************************************************************
>>> df.select(F.posexplode("pets").alias("position","col_val"),"*").select("id","name","col_val").show()
+---+-----+-------+
| id| name|col_val|
+---+-----+-------+
|  1|apple|      a|
|  1|apple|      b|
|  2|bat  |      c|
|  2|bat  |      d|
+---+-----+-------+
*************************************************************



select * from dummy_new_lat3  
lateral view posexplode(pets) pn as pos_stdate, stdate ;

+--------------------+----------------------+----------------------+----------------+------------+
| dummy_new_lat3.id  | dummy_new_lat3.name  | dummy_new_lat3.pets  | pn.pos_stdate  | pn.stdate  |
+--------------------+----------------------+----------------------+----------------+------------+
| 1                  | apple                | ["a","b"]            | 0              | a          |
| 1                  | apple                | ["a","b"]            | 1              | b          |
| 2                  | bat                  | ["c","d"]            | 0              | c          |
| 2                  | bat                  | ["c","d"]            | 1              | d          |
+--------------------+----------------------+----------------------+----------------+------------+


select * from dummy_new_lat3  
lateral view posexplode(pets)  stdate ;

+--------------------+----------------------+----------------------+-------------+-------------+
| dummy_new_lat3.id  | dummy_new_lat3.name  | dummy_new_lat3.pets  | stdate.pos  | stdate.val  |
+--------------------+----------------------+----------------------+-------------+-------------+
| 1                  | apple                | ["a","b"]            | 0           | a           |
| 1                  | apple                | ["a","b"]            | 1           | b           |
| 2                  | bat                  | ["c","d"]            | 0           | c           |
| 2                  | bat                  | ["c","d"]            | 1           | d           |
+--------------------+----------------------+----------------------+-------------+---------- ---+




select explode(pets)  from dummy_new_lat3;

+------+
| col  |
+------+
| a    |
| b    |
| c    |
| d    |
+------+



select * from dummy_new_lat3  
lateral view explode(pets)  stdate ;

+--------------------+----------------------+----------------------+-------------+
| dummy_new_lat3.id  | dummy_new_lat3.name  | dummy_new_lat3.pets  | stdate.col  |
+--------------------+----------------------+----------------------+-------------+
| 1                  | apple                | ["a","b"]            | a           |
| 1                  | apple                | ["a","b"]            | b           |
| 2                  | bat                  | ["c","d"]            | c           |
| 2                  | bat                  | ["c","d"]            | d           |
+--------------------+----------------------+----------------------+-------------+


=============================================================================================================================


CREATE TABLE dummy_new_lat4
(
  name STRING,
  phonenumbers ARRAY <string>,
  cities  ARRAY <string>
)

insert into dummy_new_lat4 select "AA", array('365-889-1234', '365-887-2232'), array('Hamilton','Burlington');
insert into dummy_new_lat4 select "BBB", array('232-998-3232', '878-998-2232'), array('Toronto', 'Stoney Creek');



schema=['name','phonenumbers','cities']
data=[["AA", ['365-889-1234', '365-887-2232'], ['Hamilton','Burlington'] ],\
["BBB", ['232-998-3232', '878-998-2232'], ['Toronto', 'Stoney Creek']]
pos_explode_df=spark.createDataFrame(data,schema)
>>> pos_explode_df.show(truncate=False)
+----+----------------------------+-----------------------+
|name|phonenumbers                |cities                 |
+----+----------------------------+-----------------------+
|AA  |[365-889-1234, 365-887-2232]|[Hamilton, Burlington] |
|BBB |[232-998-3232, 878-998-2232]|[Toronto, Stoney Creek]|
+----+----------------------------+-----------------------+

pos_explode_df.select(F.posexplode("phonenumbers").alias("ps_ph","phonenumber"), F.posexplode("cities").alias("ph_cities","city"), "*")\
.show()

pos_explode_df.withColumn("v1","v2",F.posexplode("phonenumbers")).show()


pos_explode_df.createOrReplaceTempView("address")

sql("""
select * from 
address
lateral view posexplode(phonenumbers) ph as pos_ph,ph_val
lateral view posexplode(cities) ct as pos_ct,ct_val
where pos_ph=pos_ct
""").show()





select explode(phonenumbers) from dummy_new_lat4  ;

+---------------+
|      col      |
+---------------+
| 365-889-1234  |
| 365-887-2232  |
| 232-998-3232  |
| 878-998-2232  |
+---------------+



select *,explode(phonenumbers) from dummy_new_lat4  ;

Error: Error while compiling statement: FAILED: SemanticException [Error 10081]: UDTF's are not supported outside the SELECT clause, nor nested in expressions (state=42000,code=10081)





select * from dummy_new_lat4  
lateral view posexplode(phonenumbers) ph as pos_phonenumber, phonenumber 
lateral view posexplode(cities) ctty as pos_cities, city 

+----------------------+----------------------------------+-----------------------------+----------------+---------------+-------------------+---------------+
| dummy_new_lat4.name  |   dummy_new_lat4.phonenumbers    |    dummy_new_lat4.cities    | ph.pos_stdate  |   ph.stdate   | ctty.pos_enddate  | ctty.enddate  |
+----------------------+----------------------------------+-----------------------------+----------------+---------------+-------------------+---------------+
| AA                   | ["365-889-1234","365-887-2232"]  | ["Hamilton","Burlington"]   | 0              | 365-889-1234  | 0                 | Hamilton      |
| AA                   | ["365-889-1234","365-887-2232"]  | ["Hamilton","Burlington"]   | 0              | 365-889-1234  | 1                 | Burlington    |
| AA                   | ["365-889-1234","365-887-2232"]  | ["Hamilton","Burlington"]   | 1              | 365-887-2232  | 0                 | Hamilton      |
| AA                   | ["365-889-1234","365-887-2232"]  | ["Hamilton","Burlington"]   | 1              | 365-887-2232  | 1                 | Burlington    |
| BBB                  | ["232-998-3232","878-998-2232"]  | ["Toronto","Stoney Creek"]  | 0              | 232-998-3232  | 0                 | Toronto       |
| BBB                  | ["232-998-3232","878-998-2232"]  | ["Toronto","Stoney Creek"]  | 0              | 232-998-3232  | 1                 | Stoney Creek  |
| BBB                  | ["232-998-3232","878-998-2232"]  | ["Toronto","Stoney Creek"]  | 1              | 878-998-2232  | 0                 | Toronto       |
| BBB                  | ["232-998-3232","878-998-2232"]  | ["Toronto","Stoney Creek"]  | 1              | 878-998-2232  | 1                 | Stoney Creek  |
+----------------------+----------------------------------+-----------------------------+----------------+---------------+-------------------+---------------+


select name,phonenumber,city  from dummy_new_lat4  
lateral view posexplode(phonenumbers) ph as pos_phonenumber, phonenumber 
lateral view posexplode(cities) ctty as pos_cities, city 

+-------+---------------+---------------+
| name  |  phonenumber  |     city      |
+-------+---------------+---------------+
| AA    | 365-889-1234  | Hamilton      |
| AA    | 365-889-1234  | Burlington    |
| AA    | 365-887-2232  | Hamilton      |
| AA    | 365-887-2232  | Burlington    |
| BBB   | 232-998-3232  | Toronto       |
| BBB   | 232-998-3232  | Stoney Creek  |
| BBB   | 878-998-2232  | Toronto       |
| BBB   | 878-998-2232  | Stoney Creek  |
+-------+---------------+---------------+


select name,phonenumber,city  from dummy_new_lat4  
lateral view posexplode(phonenumbers) ph as pos_phonenumber, phonenumber 
lateral view posexplode(cities) ctty as pos_cities, city
where pos_phonenumber=pos_cities

+-------+---------------+---------------+
| name  |  phonenumber  |     city      |
+-------+---------------+---------------+
| AA    | 365-889-1234  | Hamilton      |
| AA    | 365-887-2232  | Burlington    |
| BBB   | 232-998-3232  | Toronto       |
| BBB   | 878-998-2232  | Stoney Creek  |
+-------+---------------+---------------+




==============================================================================================================================


select array(phonenumbers,cities) from dummy_new_lat4;
+----------------------------------------------------+
|                        _c0                         |
+----------------------------------------------------+
| [["365-889-1234","365-887-2232"],["Hamilton","Burlington"]] |
| [["232-998-3232","878-998-2232"],["Toronto","Stoney Creek"]] |
+----------------------------------------------------+



