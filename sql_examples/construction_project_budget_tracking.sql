/*
 * Construction Project Budget Tracking Demo
 * 
 * Scenario:
 * Tracking job costs, subcontractor expenses, and budget variance by project phase.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Construction;
CREATE FOLDER IF NOT EXISTS RetailDB.Construction.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Construction.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Construction.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Construction.Bronze.Expenses (
    ExpenseID INT,
    ProjectID INT,
    Phase VARCHAR, -- Foundation, Framing, Electrical
    Amount DOUBLE,
    Category VARCHAR, -- Material, Labor, Subcontractor
    ExpenseDate DATE
);

INSERT INTO RetailDB.Construction.Bronze.Expenses VALUES
(1, 101, 'Foundation', 50000.00, 'Material', '2025-01-01'),
(2, 101, 'Foundation', 30000.00, 'Labor', '2025-01-02'),
(3, 101, 'Framing', 20000.00, 'Material', '2025-01-15'),
(4, 101, 'Framing', 45000.00, 'Labor', '2025-01-20'), -- High labor
(5, 102, 'Electrical', 15000.00, 'Subcontractor', '2025-01-05'),
(6, 102, 'Electrical', 5000.00, 'Material', '2025-01-06'),
(7, 101, 'Plumbing', 12000.00, 'Subcontractor', '2025-02-01'),
(8, 103, 'Foundation', 60000.00, 'Material', '2025-01-10'),
(9, 103, 'Foundation', 35000.00, 'Labor', '2025-01-12'),
(10, 102, 'HVAC', 25000.00, 'Subcontractor', '2025-01-25'),
(11, 101, 'Roofing', 18000.00, 'Labor', '2025-02-10');

CREATE TABLE IF NOT EXISTS RetailDB.Construction.Bronze.Budgets (
    ProjectID INT,
    Phase VARCHAR,
    BudgetAmount DOUBLE
);

INSERT INTO RetailDB.Construction.Bronze.Budgets VALUES
(101, 'Foundation', 75000.00),
(101, 'Framing', 60000.00),
(101, 'Plumbing', 15000.00),
(101, 'Roofing', 20000.00),
(102, 'Electrical', 22000.00),
(102, 'HVAC', 30000.00),
(103, 'Foundation', 90000.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Construction.Silver.CostAnalysis AS
SELECT 
    e.ProjectID,
    e.Phase,
    SUM(e.Amount) AS TotalCost,
    MAX(b.BudgetAmount) AS BudgetLimit
FROM RetailDB.Construction.Bronze.Expenses e
LEFT JOIN RetailDB.Construction.Bronze.Budgets b 
    ON e.ProjectID = b.ProjectID AND e.Phase = b.Phase
GROUP BY e.ProjectID, e.Phase;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Construction.Gold.BudgetVariance AS
SELECT 
    ProjectID,
    Phase,
    TotalCost,
    BudgetLimit,
    (TotalCost - BudgetLimit) AS Variance,
    CASE 
        WHEN TotalCost > BudgetLimit THEN 'Over Budget'
        ELSE 'Under Budget'
    END AS Status
FROM RetailDB.Construction.Silver.CostAnalysis;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all phases in RetailDB.Construction.Gold.BudgetVariance that are 'Over Budget'."
*/
