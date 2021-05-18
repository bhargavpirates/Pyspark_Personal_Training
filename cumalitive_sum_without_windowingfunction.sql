Emp  Sal
A    10
A    100
B    15
C    20
c    200
D    5


OutPut:

Emp  Sal   Cum_Sal
A    10      10
B    15      25
C    20      45
D    5       50


select
    Emp, Sal, sum(Sal) over(order by Emp) as Cum_Sal
from employees



select
    e1.Emp, e1.Sal, sum(e2.Sal) as Cum_Sal
from employees e1
    inner join employees e2 on e2.Emp <= e1.Emp
group by e1.Emp, e1.Sal


emp_schema=["Emp","Sal"]
emp=[["A",10],["A",100],["B",15],["C",20],["C",200],["D",5]]

emp=[["A",10],["B",15],["C",20],["D",5]]

emp_df=spark.createDataFrame(emp,emp_schema)
emp_df.createOrReplaceTempView("emp")


emp=[["A",10],["A",20],["A",30],["A",40],["B",15],["B",5],["C",20],["C",40],["D",5]]
spark.sql("""
select
    Emp, Sal, sum(Sal) over(partition by Emp order by sal) as Cum_Sal
from emp order by Emp,Sal
""").show()


spark.sql("""
select
    e1.Emp, e1.Sal, e2.Emp, e2.Sal 
from emp e1
    inner join emp e2 on e2.Emp <= e1.Emp
""").show()

+---+---+---+---+
|Emp|Sal|Emp|Sal|
+---+---+---+---+
|  A| 10|  A| 10|
|  B| 15|  A| 10|
|  B| 15|  B| 15|
|  C| 20|  A| 10|
|  C| 20|  B| 15|
|  D|  5|  A| 10|
|  D|  5|  B| 15|
|  C| 20|  C| 20|
|  D|  5|  C| 20|
|  D|  5|  D|  5|
+---+---+---+---+


spark.sql("""
select
    e1.Emp, e1.Sal, collect_list(e2.Sal) as Cum_Sal
from emp e1
    inner join emp e2 on e2.Emp <= e1.Emp
group by e1.Emp, e1.Sal
""").show()

+---+---+---------------+
|Emp|Sal|        Cum_Sal|
+---+---+---------------+
|  B| 15|       [10, 15]|
|  C| 20|   [20, 10, 15]|
|  D|  5|[20, 5, 10, 15]|
|  A| 10|           [10]|
+---+---+---------------+



spark.sql("""
select
    e1.Emp, e2.Emp
from emp e1
    inner join emp e2 on e2.Emp < e1.Emp
order by e1.Emp
""").show()

+---+---+
|Emp|Emp|
+---+---+
|  B|  A|
|  C|  A|
|  C|  B|
|  D|  A|
|  D|  B|
|  D|  C|
+---+---+



