/*
 * Government Infrastructure Maintenance Demo
 * 
 * Scenario:
 * Managing maintenance work orders for public assets like roads and bridges.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.Infrastructure;
CREATE FOLDER IF NOT EXISTS RetailDB.Infrastructure.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Infrastructure.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Infrastructure.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Infrastructure.Bronze.WorkOrders (
    OrderID INT,
    AssetType VARCHAR, -- Road, Bridge, Park, Building
    Description VARCHAR,
    Priority VARCHAR, -- High, Medium, Low
    ReportDate DATE,
    CompletionDate DATE,
    Cost DOUBLE
);

INSERT INTO RetailDB.Infrastructure.Bronze.WorkOrders VALUES
(1, 'Road', 'Pothole Repair On Main St', 'High', '2025-01-01', '2025-01-02', 500.00),
(2, 'Bridge', 'Structural Inspection', 'High', '2025-01-01', '2025-01-10', 5000.00),
(3, 'Park', 'Graffiti Removal', 'Low', '2025-01-02', '2025-01-15', 200.00),
(4, 'Road', 'Resurfacing', 'Medium', '2025-01-03', '2025-01-20', 15000.00),
(5, 'Building', 'HVAC Repair', 'High', '2025-01-04', '2025-01-05', 1200.00),
(6, 'Road', 'Sign Replacement', 'Low', '2025-01-05', '2025-01-08', 300.00),
(7, 'Park', 'Tree Trimming', 'Medium', '2025-01-06', '2025-01-12', 800.00),
(8, 'Bridge', 'Rust Treatment', 'Medium', '2025-01-07', NULL, NULL), -- In Progress
(9, 'Road', 'Pothole Repair 5th Ave', 'High', '2025-01-08', '2025-01-09', 450.00),
(10, 'Building', 'Roof Leak', 'High', '2025-01-09', NULL, NULL), -- Open
(11, 'Road', 'Striping', 'Low', '2025-01-10', NULL, NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Infrastructure.Silver.OrderAnalysis AS
SELECT 
    OrderID,
    AssetType,
    Priority,
    Cost,
    CASE 
        WHEN CompletionDate IS NOT NULL THEN 'Completed'
        ELSE 'Open'
    END AS Status,
    CASE 
        WHEN CompletionDate IS NOT NULL THEN DATE_DIFF(CAST(CompletionDate AS TIMESTAMP), CAST(ReportDate AS TIMESTAMP))
        ELSE NULL 
    END AS DaysToComplete
FROM RetailDB.Infrastructure.Bronze.WorkOrders;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Infrastructure.Gold.AssetSpend AS
SELECT 
    AssetType,
    COUNT(*) AS TotalOrders,
    SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenOrders,
    SUM(Cost) AS TotalSpend
FROM RetailDB.Infrastructure.Silver.OrderAnalysis
GROUP BY AssetType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the TotalSpend for 'Road' maintenance in RetailDB.Infrastructure.Gold.AssetSpend?"
*/
