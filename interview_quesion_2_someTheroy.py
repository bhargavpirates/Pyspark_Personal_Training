Question:
***********
Write a SQL query to find employees who earn the top three salaries in each of the department. 
For the above tables, your SQL query should return the following rows.


Employee
+----+-------+--------+--------------+
| Id | Name  | Salary | DepartmentId |
+----+-------+--------+--------------+
| 1  | Joe   | 70000  | 1            |
| 2  | Henry | 80000  | 2            |
| 3  | Sam   | 60000  | 2            |
| 4  | Max   | 90000  | 1            |
| 5  | Janet | 69000  | 1            |
| 6  | Randy | 85000  | 1            |
+----+-------+--------+--------------+

Department
+----+----------+
| Id | Name     |
+----+----------+
| 1  | IT       |
| 2  | Sales    |
+----+----------+

Link : https://github.com/shawlu95/Beyond-LeetCode-SQL/tree/master/LeetCode/185_Department_Top_Three_Salaries

Answer:
*******

select ename,Name,Salary from 
	(select  e.Name as ename,d.Name,e.Salary,dense_rank() over(partition by e.DepartmentId order by salary desc ) d_rank   
	from Employee e join Department d on e.DepartmentId=d.Id ) 
where  d_rank<=3

-- MS SQL: window function version
WITH department_ranking AS (
SELECT
  e.Name AS Employee
  ,d.Name AS Department
  ,e.Salary
  ,DENSE_RANK() OVER (PARTITION BY DepartmentId ORDER BY Salary DESC) AS rnk
FROM Employee AS e
JOIN Department AS d
ON e.DepartmentId = d.Id
)
SELECT
  Department
  ,Employee
  ,Salary
FROM department_ranking
WHERE rnk <= 3
ORDER BY Department ASC, Salary DESC;



----------------------------------------------------------------------------------------------------------------------------------

Link : https://www.stratascratch.com/blog/microsoft-data-scientist-interview-questions/

Question:
**********
Select the top 3 departments by the highest percentage of employees making over $100K in salary and have at least 10 employees.
 Output the department name and percentage of employees making over $100K. 
Sort by department with the highest percentage to the lowest.



Answer:
*******

SELECT
  t.name,
  (t.emp_xs100k / s.emp_all::FLOAT)*100 as emp_xs100k_pct
FROM (
    SELECT
      name,
      count(id) as emp_xs100K
    FROM ms_employee a
    JOIN ms_department b ON a.department_id = b.department_id
    WHERE salary > 100000
    GROUP BY name) t
LEFT JOIN (
    SELECT
      name,
      count(id) as emp_all
    FROM ms_employee a
    JOIN ms_department b on a.department_id = b.department_id
    GROUP BY name) s
  ON t.name = s.name
WHERE s.emp_all >= 10
ORDER BY emp_xs100k_pct DESC
LIMIT 3


------------------------------------------------------------------------------------------------------------------------------------

Link : https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/big%20countries.sql
select * from world where area>3000000 or population>25,000,000 
https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/classes%20more%20than%205%20students.sql
select class from table group by class having count(distinct(student))>=5
https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/consecutive%20numbers.sql
select Num from(select Id,Num, lag(Num,1,0) over(order by Id) as next_lag1, lag(Num,2,0) over(order by Id) as next_lag2  from table)where Num=next_lag1 and Num=next_lag2
'''
data1=[[1,1],[2,1],[3,1],[4,2],[5,1],[6,2],[7,2]]
df=spark.createDataFrame(data1,schema=['Id','Num'])

df.createTempView("table")
spark.sql("\select Id,Num, lag(Num,1,0) over(order by Id) as next_lag1, lag(Num,2,0) over(order by Id) as next_lag2  from table").show()
>>> spark.sql("select Id,Num, lag(Num,1,0) over(order by Id) as next_lag1, lag(Num,2,0) over(order by Id) as next_lag2  from table").show()
21/02/24 05:28:20 WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+---+---+---------+---------+
| Id|Num|next_lag1|next_lag2|
+---+---+---------+---------+
|  1|  1|        0|        0|
|  2|  1|        1|        0|
|  3|  1|        1|        1|
|  4|  2|        1|        1|
|  5|  1|        2|        1|
|  6|  2|        1|        2|
|  7|  2|        2|        1|
+---+---+---------+---------+



>>> spark.sql("""select Num from(select Id,Num, lag(Num,1,0) over(order by Id) as next_lag1, lag(Num,2,0) over(order by Id) as next_lag2  from table)
...     where Num=next_lag1 and Num=next_lag2""").show()
21/02/24 05:30:31 WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+---+
|Num|
+---+
|  1|
+---+
'''
https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/customers%20who%20never%20order.sql
select * from Customers c left join Orders o on  c.Id=o.CustomerId  where o.id is NUll

'''
Customers_schema=['Id','Name']
Customers=[[1,'Joe  '] ,[2,'Henry'] ,[3,'Sam  '] ,[4,'Max  '] ]

Orders_schema = ['Id','CustomerId']
Orders=[[1,3],[2,1]]

customer_df=spark.createDataFrame(Customers,schema=Customers_schema)
order_df=spark.createDataFrame(Orders,schema=Orders_schema)

>>> customer_df.join(order_df,customer_df.Id==order_df.CustomerId,'leftanti').show()
+---+-----+
| Id| Name|
+---+-----+
|  2|Henry|
|  4|Max  |
+---+-----+

customer_df.createTempView("customer")
order_df.createTempView("order")
spark.sql("select Name from customer c left join order o on  c.Id=o.CustomerId  where o.id is NUll")


spark.sql("select c.Name as Customers from customer c where c.Id not in (select o.CustomerId from order o)").show()

'''
https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/delete%20duplicate%20emails.sql
Write a SQL query to delete all duplicate email entries in a table named Person, keeping only unique emails based on its smallest Id.
delete b.*  from Person a, Person b where a.Email = b.Email and b.Id > a.Id;

https://github.com/Tony-DongyiLuo/leetcode-sql/blob/master/duplicate_emails.sql
Write a SQL query to find all duplicate emails in a table named Person.
select a.Email from Person a group by a.Email having count(a.Email) > 1 ;




==============================================================================================================================

# Method 1: Windows Function Application with Case used 2 times

SELECT t.visited_on, t.amount, ROUND(t.Average,2) AS average_amount 
FROM(
SELECT visited_on,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY visited_on) >= 7 THEN SUM(SUM(amount)) OVER(ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS amount, 
    CASE WHEN ROW_NUMBER() OVER (ORDER BY visited_on) >= 7 THEN AVG(SUM(amount)) OVER(ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS Average
FROM Customer
GROUP BY visited_on
ORDER BY visited_on) t
WHERE t.amount IS NOT NULL


# Method 2: Windows Function Application with Subqueries

SELECT T2.visited_on, T2.amount, T2.average_amount
FROM (SELECT visited_on,
        SUM(amount) OVER (ORDER BY visited_on ROWS 6 PRECEDING) AS amount,
        ROUND(AVG(amount) OVER (ORDER BY visited_on ROWS 6 PRECEDING),2) AS average_amount, 
        ROW_NUMBER() OVER (ORDER BY visited_on) AS r_num 
    FROM (SELECT visited_on, SUM(amount) AS amount FROM customer GROUP BY visited_on ORDER BY visited_on) AS T1) AS T2
WHERE T2.r_num >= 7



