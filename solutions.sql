-- Create data pipeline using Store Procedure

CREATE OR REPLACE PROCEDURE get_salary_data()
LANGUAGE plpgsql    
AS $$
BEGIN
    
	-- Clean table to insert fresh data
    DROP TABLE IF EXISTS employees;
    DROP TABLE IF EXISTS timesheets;
    DROP TABLE IF EXISTS branch_salary;

    -- Define table schema
    CREATE TABLE IF NOT EXISTS employees (
        employee_id INT PRIMARY KEY,
        branch_id INT NOT NULL,
        salary INT,
        join_date DATE,
        resign_date DATE
    );

    CREATE TABLE IF NOT EXISTS timesheets (
        timesheet_id INT PRIMARY KEY,
        employee_id INT,
        date DATE,
        checkin TIME,
        checkout TIME
    );

    CREATE TABLE IF NOT EXISTS branch_salary (
        branch_id INT,
        year INT,
        month INT,
        salary_per_hour INT
    );
	
	-- Import data to postgresql from CSV file. 
	-- For windows, use public folder to store the data source to avoid permission issues
    COPY employees(employee_id, branch_id, salary, join_date, resign_date)
    FROM 'C:\\Users\\Public\\Documents\\data_source\\employees.csv'
    DELIMITER ','
    CSV HEADER;

    COPY timesheets(timesheet_id, employee_id, date, checkin, checkout)
    FROM 'C:\\Users\\Public\\Documents\\data_source\\timesheets.csv'
    DELIMITER ','
    CSV HEADER;
	
	-- When the source table already populated with data, do the data transformations
	-- then ingest it into destination table.
	INSERT INTO branch_salary
	WITH total_hours_per_employee AS (
		SELECT
		  employee_id,
		  DATE_TRUNC('month', date) AS work_date,
		  ROUND(SUM(EXTRACT(EPOCH FROM (checkout - checkin))/60/60),2) AS total_hours
		FROM public.timesheets
		WHERE (checkin IS NOT NULL) AND (checkout IS NOT NULL)
		GROUP BY 1,2
	),
	thpb AS (
		SELECT
			EXTRACT(YEAR FROM work_date) AS year,
			EXTRACT(MONTH FROM work_date) AS month,
			branch_id,
			ROUND(SUM(salary), 0) AS total_salary_per_branch,
			ROUND(SUM(total_hours), 0) AS total_hours_per_branch
		FROM public.employees AS emp
		LEFT JOIN total_hours_per_employee AS ts ON ts.employee_id=emp.employee_id
		GROUP BY 1,2,3
	),
	branch_details AS (
		SELECT
		  year,
		  month,
		  branch_id,
		  total_salary_per_branch / total_hours_per_branch AS salary_per_hour
		FROM thpb
	)
	SELECT 
		branch_id, 
		year, 
		month, 
		CAST(ROUND(salary_per_hour, 0) AS INT) AS salary_per_hour 
	FROM branch_details;

    COMMIT;
END;$$

-- Execute the procedure to start the pipeline
CALL get_salary_data();