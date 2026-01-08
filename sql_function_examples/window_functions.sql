-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Window Functions
-- 
-- This script demonstrates the usage of window functions for analytics.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.EmployeeSalaries (
    EmpID INT,
    Dept VARCHAR,
    Name VARCHAR,
    Salary INT
);

INSERT INTO FunctionExamplesDB.EmployeeSalaries (EmpID, Dept, Name, Salary) VALUES
(1, 'Sales', 'Alice', 60000),
(2, 'Sales', 'Bob', 55000),
(3, 'Sales', 'Charlie', 60000), -- Tie with Alice
(4, 'Marketing', 'Dave', 70000),
(5, 'Marketing', 'Eve', 72000),
(6, 'Engineering', 'Frank', 90000),
(7, 'Engineering', 'Grace', 85000),
(8, 'Engineering', 'Heidi', 95000);

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Window Functions
-------------------------------------------------------------------------------

-- 2.1 Ranking Functions (ROW_NUMBER, RANK, DENSE_RANK)
-- ROW_NUMBER: Unique number for each row.
-- RANK: Same rank for ties, skips numbers (1, 1, 3).
-- DENSE_RANK: Same rank for ties, no skipping (1, 1, 2).
SELECT 
    Dept,
    Name,
    Salary,
    ROW_NUMBER() OVER (PARTITION BY Dept ORDER BY Salary DESC) AS Row_Num,
    RANK() OVER (PARTITION BY Dept ORDER BY Salary DESC) AS Rank_Val,
    DENSE_RANK() OVER (PARTITION BY Dept ORDER BY Salary DESC) AS Dense_Rank_Val
FROM FunctionExamplesDB.EmployeeSalaries;

-- 2.2 Value Access Functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE)
-- LAG: Access previous row.
-- LEAD: Access next row.
-- FIRST_VALUE: First value in the window frame.
SELECT 
    Name,
    Salary,
    LAG(Salary, 1, 0) OVER (ORDER BY Salary) AS Previous_Lower_Salary,
    LEAD(Salary, 1, 0) OVER (ORDER BY Salary) AS Next_Higher_Salary,
    FIRST_VALUE(Name) OVER (ORDER BY Salary DESC) AS Highest_Paid_Employee
FROM FunctionExamplesDB.EmployeeSalaries;

-- 2.3 Cumulative Distribution and Bucketing (CUME_DIST, PERCENT_RANK, NTILE)
-- NTILE(n): Divides rows into n buckets.
-- CUME_DIST: Cumulative distribution.
SELECT 
    Name,
    Salary,
    NTILE(4) OVER (ORDER BY Salary) AS Quartile,
    CUME_DIST() OVER (ORDER BY Salary) AS Cumulative_Dist
FROM FunctionExamplesDB.EmployeeSalaries;

-- 2.4 Aggregate Window Functions
-- Running totals or moving averages using window frames.
SELECT 
    Name,
    Salary,
    SUM(Salary) OVER (ORDER BY Salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Running_Total_Global,
    AVG(Salary) OVER (PARTITION BY Dept) AS Dept_Avg_Salary,
    Salary - AVG(Salary) OVER (PARTITION BY Dept) AS Diff_From_Dept_Avg
FROM FunctionExamplesDB.EmployeeSalaries;
