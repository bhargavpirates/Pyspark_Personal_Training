table1=[['Amy'],['John'],['Lisa'],['Marco'],['Phil']]
table2=[['Lisa'],['Marco'],['Phil'],['Tim'],['Vincent']]

df1=spark.createDataFrame(table1,schema=['names'])
df2=spark.createDataFrame(table2,schema=['names'])

df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNull()).explain()

  >>> df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNull()).explain()
== Physical Plan ==
*(4) Filter isnull(names#8)
+- SortMergeJoin [names#2], [names#8], LeftOuter
   :- *(1) Sort [names#2 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(names#2, 200)
   :     +- Scan ExistingRDD[names#2]
   +- *(3) Sort [names#8 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(names#8, 200)
         +- *(2) Filter isnotnull(names#8)
            +- Scan ExistingRDD[names#8]


df1.join(df2,df1.names==df2.names,how="leftanti").explain()
>>> df1.join(df2,df1.names==df2.names,how="leftanti").explain()
== Physical Plan ==
SortMergeJoin [names#2], [names#8], LeftAnti
:- *(1) Sort [names#2 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(names#2, 200)
:     +- Scan ExistingRDD[names#2]
+- *(3) Sort [names#8 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(names#8, 200)
      +- *(2) Filter isnotnull(names#8)
         +- Scan ExistingRDD[names#8]


>>> df1.join(df2,df1.names==df2.names,how="left").filter(df2.names.isNotNull()).explain()
== Physical Plan ==
*(5) SortMergeJoin [names#2], [names#8], Inner
:- *(2) Sort [names#2 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(names#2, 200)
:     +- *(1) Filter isnotnull(names#2)
:        +- Scan ExistingRDD[names#2]
+- *(4) Sort [names#8 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(names#8, 200)
      +- *(3) Filter isnotnull(names#8)
         +- Scan ExistingRDD[names#8]


df1.join(df2,df1.names==df2.names,how="leftsemi").explain()
>>> df1.join(df2,df1.names==df2.names,how="leftsemi").explain()
== Physical Plan ==
SortMergeJoin [names#2], [names#8], LeftSemi
:- *(2) Sort [names#2 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(names#2, 200)
:     +- *(1) Filter isnotnull(names#2)
:        +- Scan ExistingRDD[names#2]
+- *(4) Sort [names#8 ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(names#8, 200)
      +- *(3) Filter isnotnull(names#8)
         +- Scan ExistingRDD[names#8]

============================================================================================================================

select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= '1998-09-16'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;


== Physical Plan ==
Sort [l_returnflag#35 ASC,l_linestatus#36 ASC], true, 0
+- ConvertToUnsafe
   +- Exchange rangepartitioning(l_returnflag#35 ASC,l_linestatus#36 ASC,200), None
      +- ConvertToSafe
         +- TungstenAggregate(key=[l_returnflag#35,l_linestatus#36], functions=[(sum(l_quantity#31),mode=Final,isDistinct=false),(sum(l_extendedpr#32),mode=Final,isDistinct=false),(sum((l_extendedprice#32 * (1.0 - l_discount#33))),mode=Final,isDistinct=false),(sum(((l_extendedprice#32 * (1.0l_discount#33)) * (1.0 + l_tax#34))),mode=Final,isDistinct=false),(avg(l_quantity#31),mode=Final,isDistinct=false),(avg(l_extendedprice#32),mode=Fl,isDistinct=false),(avg(l_discount#33),mode=Final,isDistinct=false),(count(1),mode=Final,isDistinct=false)], output=[l_returnflag#35,l_linestatus,sum_qty#0,sum_base_price#1,sum_disc_price#2,sum_charge#3,avg_qty#4,avg_price#5,avg_disc#6,count_order#7L])
            +- TungstenExchange hashpartitioning(l_returnflag#35,l_linestatus#36,200), None
               +- TungstenAggregate(key=[l_returnflag#35,l_linestatus#36], functions=[(sum(l_quantity#31),mode=Partial,isDistinct=false),(sum(l_exdedprice#32),mode=Partial,isDistinct=false),(sum((l_extendedprice#32 * (1.0 - l_discount#33))),mode=Partial,isDistinct=false),(sum(((l_extendedpri32 * (1.0 - l_discount#33)) * (1.0 + l_tax#34))),mode=Partial,isDistinct=false),(avg(l_quantity#31),mode=Partial,isDistinct=false),(avg(l_extendedce#32),mode=Partial,isDistinct=false),(avg(l_discount#33),mode=Partial,isDistinct=false),(count(1),mode=Partial,isDistinct=false)], output=[l_retulag#35,l_linestatus#36,sum#64,sum#65,sum#66,sum#67,sum#68,count#69L,sum#70,count#71L,sum#72,count#73L,count#74L])
                  +- Project [l_discount#33,l_linestatus#36,l_tax#34,l_quantity#31,l_extendedprice#32,l_returnflag#35]
                     +- Filter (l_shipdate#37 <= 1998-09-16)
                        +- HiveTableScan [l_discount#33,l_linestatus#36,l_tax#34,l_quantity#31,l_extendedprice#32,l_shipdate#37,l_returnflag#35], astoreRelation default, lineitem, None


Explanation:
*************
Lets look at the structure of the SQL query you use:

SELECT
    ...  -- not aggregated columns  #1
    ...  -- aggregated columns      #2
FROM
    ...                          -- #3
WHERE
    ...                          -- #4
GROUP BY
    ...                          -- #5
ORDER BY
    ...                          -- #6

As you already suspect:
*************************
Filter (...) corresponds to predicates in WHERE clause (#4)
Project ... limits number of columns to those required by an union of (#1 and #2, and #4 / #6 if not present in SELECT)
HiveTableScan corresponds to FROM clause (#3)
Remaining parts can attributed as follows:

#2 from SELECT clause - functions field in TungstenAggregates
GROUP BY clause (#5):

TungstenExchange / hash partitioning
key field in TungstenAggregates
#6 - ORDER BY clause.

Project Tungsten in general describes a set of optimizations used by Spark DataFrames (-sets) including:
	a. explicit memory management with sun.misc.Unsafe. It means "native" (off-heap) memory usage and explicit memory allocation / freeing 
		outside GC management. These conversions correspond to ConvertToUnsafe / ConvertToSafe steps in the execution plan. 
		You can learn some interesting details about unsafe from Understanding sun.misc.Unsafe
	b. code generation - different meta-programming tricks designed to generate code that better optimized during compilation. 
		You can think of it as an internal Spark compiler which does things like rewriting nice functional code into ugly for loops.


You can learn more about Tungsten in general from Project Tungsten: Bringing Apache Spark Closer to Bare Metal. 
Apache Spark 2.0: Faster, Easier, and Smarter provides some examples of code generation.
TungstenAggregate occurs twice because data is first aggregated locally on each partition, than shuffled, and finally merged. 
If you are familiar with RDD API this process is roughly equivalent to reduceByKey.

If execution plan is not clear you can also try to convert resulting DataFrame to RDD and analyze output of toDebugString.
>>> customer_df.join(order_df,customer_df.Id==order_df.CustomerId,'leftanti').explain
<bound method DataFrame.explain of DataFrame[Id: bigint, Name: string]>
>>> customer_df.join(order_df,customer_df.Id==order_df.CustomerId,'leftanti').explain()
== Physical Plan ==
SortMergeJoin [Id#126L], [CustomerId#131L], LeftAnti
:- *(1) Sort [Id#126L ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(Id#126L, 200)
:     +- Scan ExistingRDD[Id#126L,Name#127]
+- *(3) Sort [CustomerId#131L ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(CustomerId#131L, 200)
      +- *(2) Project [CustomerId#131L]
         +- *(2) Filter isnotnull(CustomerId#131L)
            +- Scan ExistingRDD[Id#130L,CustomerId#131L]
>>> spark.sql("select c.Name as Customers from customer c where c.Id not in (select o.CustomerId from order o)").show()
+---------+
|Customers|
+---------+
|    Henry|
|    Max  |
+---------+

>>> spark.sql("select c.Name as Customers from customer c where c.Id not in (select o.CustomerId from order o)").explain()
== Physical Plan ==
*(2) Project [Name#127 AS Customers#179]
+- BroadcastNestedLoopJoin BuildRight, LeftAnti, ((Id#126L = CustomerId#131L) || isnull((Id#126L = CustomerId#131L)))
   :- Scan ExistingRDD[Id#126L,Name#127]
   +- BroadcastExchange IdentityBroadcastMode
      +- *(1) Project [CustomerId#131L]
         +- Scan ExistingRDD[Id#130L,CustomerId#131L]









======================================================================================================================================

>>> spark.sql("select Name from customer c left join order o on  c.Id=o.CustomerId  where o.id is NUll").explain()
== Physical Plan ==
*(4) Project [Name#127]
+- *(4) Filter isnull(id#130L)
   +- SortMergeJoin [Id#126L], [CustomerId#131L], LeftOuter
      :- *(1) Sort [Id#126L ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(Id#126L, 200)
      :     +- Scan ExistingRDD[Id#126L,Name#127]
      +- *(3) Sort [CustomerId#131L ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(CustomerId#131L, 200)
            +- *(2) Filter isnotnull(CustomerId#131L)
               +- Scan ExistingRDD[Id#130L,CustomerId#131L]

>>> customer_df.join(order_df,customer_df.Id==order_df.CustomerId,'leftanti').explain
<bound method DataFrame.explain of DataFrame[Id: bigint, Name: string]>

>>> customer_df.join(order_df,customer_df.Id==order_df.CustomerId,'leftanti').explain()
== Physical Plan ==
SortMergeJoin [Id#126L], [CustomerId#131L], LeftAnti
:- *(1) Sort [Id#126L ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(Id#126L, 200)
:     +- Scan ExistingRDD[Id#126L,Name#127]
+- *(3) Sort [CustomerId#131L ASC NULLS FIRST], false, 0
   +- Exchange hashpartitioning(CustomerId#131L, 200)
      +- *(2) Project [CustomerId#131L]
         +- *(2) Filter isnotnull(CustomerId#131L)
            +- Scan ExistingRDD[Id#130L,CustomerId#131L]

>>> spark.sql("select c.Name as Customers from customer c where c.Id not in (select o.CustomerId from order o)").show()
+---------+
|Customers|
+---------+
|    Henry|
|    Max  |
+---------+

>>> spark.sql("select c.Name as Customers from customer c where c.Id not in (select o.CustomerId from order o)").explain()
== Physical Plan ==
*(2) Project [Name#127 AS Customers#179]
+- BroadcastNestedLoopJoin BuildRight, LeftAnti, ((Id#126L = CustomerId#131L) || isnull((Id#126L = CustomerId#131L)))
   :- Scan ExistingRDD[Id#126L,Name#127]
   +- BroadcastExchange IdentityBroadcastMode
      +- *(1) Project [CustomerId#131L]
         +- Scan ExistingRDD[Id#130L,CustomerId#131L]


         

