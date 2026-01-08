/*
 * HR Workforce Analytics Demo
 * 
 * Scenario:
 * An HR department monitors employee performance and retention.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify top talent and flight risks.
 * 
 * Note: Assumes a catalog named 'HRDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HRDB;
CREATE FOLDER IF NOT EXISTS HRDB.Bronze;
CREATE FOLDER IF NOT EXISTS HRDB.Silver;
CREATE FOLDER IF NOT EXISTS HRDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Employee Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS HRDB.Bronze.Departments (
    DeptID INT,
    DeptName VARCHAR,
    ManagerID INT
);

CREATE TABLE IF NOT EXISTS HRDB.Bronze.Employees (
    EmpID INT,
    FirstName VARCHAR,
    DeptID INT,
    HireDate DATE,
    Salary DOUBLE,
    Status VARCHAR -- 'Active', 'Terminated'
);

CREATE TABLE IF NOT EXISTS HRDB.Bronze.PerformanceReviews (
    ReviewID INT,
    EmpID INT,
    ReviewDate DATE,
    Rating DOUBLE, -- 1.0 to 5.0
    Comments VARCHAR
);

-- 1.2 Populate Bronze Tables
INSERT INTO HRDB.Bronze.Departments (DeptID, DeptName, ManagerID) VALUES
(1, 'Engineering', 101),
(2, 'Sales', 201),
(3, 'Marketing', 301);

INSERT INTO HRDB.Bronze.Employees (EmpID, FirstName, DeptID, HireDate, Salary, Status) VALUES
(101, 'Alice', 1, '2020-01-15', 150000.00, 'Active'),
(102, 'Bob', 1, '2021-03-10', 120000.00, 'Active'),
(103, 'Charlie', 1, '2022-06-01', 110000.00, 'Terminated'),
(201, 'David', 2, '2019-11-20', 140000.00, 'Active'),
(202, 'Eve', 2, '2023-01-05', 90000.00, 'Active');

INSERT INTO HRDB.Bronze.PerformanceReviews (ReviewID, EmpID, ReviewDate, Rating, Comments) VALUES
(1, 101, '2025-01-15', 4.8, 'Excellent leadership'),
(2, 102, '2025-01-15', 3.5, 'Meets expectations'),
(3, 103, '2024-06-01', 2.0, 'Performance issues'),
(4, 202, '2025-01-15', 4.5, 'Rising star');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Talent Management
-------------------------------------------------------------------------------

-- 2.1 View: Employee_History
-- Enriches employee data with latest performance rating.
CREATE OR REPLACE VIEW HRDB.Silver.Employee_History AS
SELECT
    e.EmpID,
    e.FirstName,
    d.DeptName,
    e.Salary,
    e.Status,
    r.Rating
FROM HRDB.Bronze.Employees e
JOIN HRDB.Bronze.Departments d ON e.DeptID = d.DeptID
LEFT JOIN HRDB.Bronze.PerformanceReviews r ON e.EmpID = r.EmpID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Workforce Insights
-------------------------------------------------------------------------------

-- 3.1 View: Dept_Performance
-- Aggregates ratings and turnover by department.
CREATE OR REPLACE VIEW HRDB.Gold.Dept_Performance AS
SELECT
    DeptName,
    COUNT(EmpID) AS Headcount,
    AVG(Salary) AS AvgSalary,
    AVG(Rating) AS AvgPerformance,
    SUM(CASE WHEN Status = 'Terminated' THEN 1 ELSE 0 END) AS Terminations
FROM HRDB.Silver.Employee_History
GROUP BY DeptName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Top Talent):
"List all active employees from HRDB.Silver.Employee_History with a Rating > 4.0."

PROMPT 2 (Budgeting):
"Which department has the highest AvgSalary in HRDB.Gold.Dept_Performance?"
*/
