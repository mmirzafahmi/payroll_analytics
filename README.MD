# Mekari Take Home Test

This take home test is a part of hiring process in Mekari for Data Engineer 2. 

## Business Case

You are given two tables:
* employees - all-time employee information 
* timesheets - daily clock-ins and clock-outs of the employees

As a Data Engineer, you are working with an analyst to figure out whether the current payroll scheme (which is a per-month basis) is reasonably cost-effective in terms of cost per hour. It needs to be analyzed per branch and a monthly basis.
For each branch, find out salary per hour based on the number of employees that work for that branch each month. For E.g. assuming Branch A has 5 people working for it in January; the salary for those 5 people in that month is Rp100,000,000, and the total hours for the same 5 employees in that month is 1000 hours. Therefore, the output should be Rp100,000 per hour.

## Tasks

**SQL**
* Create a schema and load each CSV file to employees and timesheets tables.
* Write an SQL script that reads from employees and timesheets tables, transforms, and loads the destination table.
* The script is expected to run daily in full-snapshot mode, meaning that it will read the whole table and then overwrite the result in the destination table. Note that you don’t have to implement the scheduler, just the script that will be run by the scheduler.

**Python or Java**
* Write a Python or Java code that reads from CSV files, transforms, and loads to the
destination table.
* The code is expected to run daily in incremental mode, meaning that each day it will only
read the new data and then appends the result to the destination table. Note that you
don’t have to implement the scheduler, just the script that will be run by the scheduler.

## Solutions
SQL task, I create store procedure with plpgsql to perform ETL pipeline based on business requirement. I also clean the data such as remove duplicates and filter the Null values. To execute the script, you need to have PostgreSQL 11+ and follow the instruction below:

```
psql -U <database_name> -f solutions.sql
```

or you can put the script on PostgreSQL scheduler called pgAgent by executed `STORE PROCEDURE` query first, then add the `CALL` query to pgAgent.

Python task, I do the same thing from SQL version. To execute the script, the first thing you need to install the requirements then execute the main script

```
> pip install -r requirements.txt
> python solutions.py
```

## Author
Muhammad Mirza Fahmi ([Linkedin](https://www.linkedin.com/in/mmirzafahmi))

