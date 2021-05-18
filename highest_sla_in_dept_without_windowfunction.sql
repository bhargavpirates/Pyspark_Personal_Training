DeptID      EmpName   Salary
Engg        Sam       1000
Engg        Smith     2000
HR          Denis     1500
HR          Danny     3000
IT          David     2000
IT          John      3000


SELECT e1.empName, e1.empDept, e1.EmpSalary 
FROM Employee e1 
WHERE e1.empSalary IN 
(SELECT max(e2.empSalary) AS salary From Employee e2 GROUP BY e2.EmpDept HAVING e1.EmpDept = e2.EmpDept)



select a.*
 from EmpDetails a
 inner join 
 (
 select DeptID,max(Salary) as Salary
 from EmpDetails group by DeptID  
 )b 
on a.DeptID = b.DeptID and a.Salary = b.Salary




SELECT 
	*
   FROM emp 
   where 
     (DeptId,Salary) 
     in 
     (select DeptId, max(Salary) from emp group by DeptId)


emp_schema=["DeptID","EmpName","Salary"]

emp=[["Engg","Sam  ",1000],
["Engg","Smith",2000],
["HR  ","Denis",1500],
["HR  ","Danny",3000],
["IT  ","David",2000],
["IT  ","John ",3000],
["IT  ","Bhar ",3000]]

emp_df=spark.createDataFrame(emp,emp_schema)
emp_df.createOrReplaceTempView("emp")


spark.sql("""
select a.*
 from emp a
 inner join 
 (
 select DeptID,max(Salary) as Salary
 from emp group by DeptID  
 )b 
on a.DeptID = b.DeptID and a.Salary = b.Salary
""").show()

spark.sql("""
SELECT 
	*
   FROM emp 
   where 
     (DeptId,Salary) 
     in 
     (select DeptId, max(Salary) from emp group by DeptId)
""").show()



emp_schema=["DeptID","EmpName","Salary"]

spark.sql("""
SELECT e1.EmpName, e1.DeptID, e1.Salary 
FROM emp e1 
WHERE e1.Salary IN 
(SELECT max(e2.Salary) AS salary From emp e2 GROUP BY e2.DeptID HAVING e1.DeptID = e2.DeptID)
""").show()


