SELECT deptname AS department,NAME AS employee,salary FROM 
		(SELECT 
dense_rank() over(partition by deptname ORDER by a.salary desc) AS rnk,a.Name,a.salary,b.deptname
FROM EMPLOYEES a JOIN department b on a.departmentid=b.id)
WHERE rnk <=3;


select t.Request_at AS "Day",
       cast((select cast(count(*) as float) from Trips inner join USERS on Client_Id = Users_Id 
       where Request_at = t.Request_at and status != 'completed' and Banned = 'No')/(select cast(count(*) as float) from Trips inner join Users
       on Client_Id = Users_Id
       where Request_at = t.Request_at
       and Banned = 'No') as numeric(16, 2)) as "Cancellation_Rate"
from Trips t
where t.Request_at between '2013-10-01' and '2013-10-03'
group by Request_at;
