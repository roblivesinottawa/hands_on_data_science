
/* find date range */
select min(employees.startdate), max(employees.startdate)
from employees

/* emp starts by month / year */
select MONTH(employees.startdate), year(employees.startdate), count(1)
from employees
group by MONTH(employees.startdate), year(employees.startdate)
order by 2, 1

/* Active on given date */
select *
from employees
where employees.startdate >= '2008-07-01'
and (employees.enddate <= '2008-07-01'
    OR
    employees.enddate is NULL)


/* count of active on date */
select count(1)
from employees
where employees.startdate >= '2008-07-01'
and (employees.enddate <= '2008-07-01'
    OR
    employees.enddate is NULL)