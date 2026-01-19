/*
    Dremio High-Volume SQL Pattern: Government Infrastructure Maintenance
    
    Business Scenario:
    Managing maintenance work orders for public assets like roads and bridges.
    Tracking costs, completion times, and contractor performance.
    
    Data Story:
    We track Work Orders and Asset Information.
    
    Medallion Architecture:
    - Bronze: WorkOrders.
      *Volume*: 50+ records.
    - Silver: OrderAnalysis (Duration & Status).
    - Gold: AssetSpend (Cost aggregation).
    
    Key Dremio Features:
    - Date Diff
    - Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentInfraDB;
CREATE FOLDER IF NOT EXISTS GovernmentInfraDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentInfraDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentInfraDB.Gold;
USE GovernmentInfraDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentInfraDB.Bronze.WorkOrders (
    OrderID STRING,
    AssetType STRING, -- Road, Bridge, Park, Building
    Description STRING,
    Priority STRING, -- High, Medium, Low
    ReportDate DATE,
    CompletionDate DATE, -- NULL if Open
    Cost DOUBLE
);

INSERT INTO GovernmentInfraDB.Bronze.WorkOrders VALUES
('WO1', 'Road', 'Pothole Repair', 'High', DATE '2025-01-01', DATE '2025-01-02', 500.00),
('WO2', 'Bridge', 'Inspection', 'High', DATE '2025-01-01', DATE '2025-01-10', 5000.00),
('WO3', 'Park', 'Graffiti', 'Low', DATE '2025-01-02', DATE '2025-01-15', 200.00),
('WO4', 'Road', 'Resurfacing', 'Medium', DATE '2025-01-03', DATE '2025-01-20', 15000.00),
('WO5', 'Building', 'HVAC', 'High', DATE '2025-01-04', DATE '2025-01-05', 1200.00),
('WO6', 'Road', 'Signage', 'Low', DATE '2025-01-05', DATE '2025-01-08', 300.00),
('WO7', 'Park', 'Trimming', 'Medium', DATE '2025-01-06', DATE '2025-01-12', 800.00),
('WO8', 'Bridge', 'Rust', 'Medium', DATE '2025-01-07', NULL, NULL),
('WO9', 'Road', 'Pothole', 'High', DATE '2025-01-08', DATE '2025-01-09', 450.00),
('WO10', 'Building', 'Roof Leak', 'High', DATE '2025-01-09', NULL, NULL),
('WO11', 'Road', 'Striping', 'Low', DATE '2025-01-10', NULL, NULL),
('WO12', 'Road', 'Pothole', 'High', DATE '2025-01-11', DATE '2025-01-12', 550.00),
('WO13', 'Park', 'Mowing', 'Low', DATE '2025-01-12', DATE '2025-01-13', 150.00),
('WO14', 'Building', 'Plumbing', 'High', DATE '2025-01-13', DATE '2025-01-14', 900.00),
('WO15', 'Road', 'Resurfacing', 'Medium', DATE '2025-01-14', NULL, NULL),
('WO16', 'Bridge', 'Lighting', 'Low', DATE '2025-01-15', DATE '2025-01-16', 400.00),
('WO17', 'Park', 'Bench Repair', 'Low', DATE '2025-01-16', DATE '2025-01-18', 250.00),
('WO18', 'Building', 'Window', 'Medium', DATE '2025-01-17', NULL, NULL),
('WO19', 'Road', 'Signal', 'High', DATE '2025-01-18', DATE '2025-01-19', 2000.00),
('WO20', 'Road', 'Pothole', 'High', DATE '2025-01-19', NULL, NULL),
('WO21', 'Park', 'Playground', 'Medium', DATE '2025-01-20', NULL, NULL),
('WO22', 'Bridge', 'Joints', 'High', DATE '2025-01-01', DATE '2025-01-20', 8000.00),
('WO23', 'Road', 'Sweeping', 'Low', DATE '2025-01-02', DATE '2025-01-02', 100.00),
('WO24', 'Building', 'Cleaning', 'Low', DATE '2025-01-03', DATE '2025-01-03', 200.00),
('WO25', 'Park', 'Trash', 'Low', DATE '2025-01-04', DATE '2025-01-04', 100.00),
('WO26', 'Road', 'Pothole', 'High', DATE '2025-01-05', DATE '2025-01-06', 480.00),
('WO27', 'Bridge', 'Painting', 'Medium', DATE '2025-01-06', NULL, NULL),
('WO28', 'Building', 'Door', 'Low', DATE '2025-01-07', DATE '2025-01-08', 350.00),
('WO29', 'Road', 'Guardrail', 'Medium', DATE '2025-01-08', DATE '2025-01-15', 3000.00),
('WO30', 'Park', 'Fencing', 'Medium', DATE '2025-01-09', NULL, NULL),
('WO31', 'Road', 'Drainage', 'High', DATE '2025-01-10', DATE '2025-01-15', 2500.00),
('WO32', 'Bridge', 'Deck', 'High', DATE '2025-01-11', NULL, NULL),
('WO33', 'Building', 'Electrical', 'High', DATE '2025-01-12', DATE '2025-01-13', 1500.00),
('WO34', 'Road', 'Pothole', 'High', DATE '2025-01-13', DATE '2025-01-14', 520.00),
('WO35', 'Park', 'Planting', 'Low', DATE '2025-01-14', DATE '2025-01-16', 400.00),
('WO36', 'Road', 'Signage', 'Low', DATE '2025-01-15', NULL, NULL),
('WO37', 'Building', 'Elevator', 'High', DATE '2025-01-16', NULL, NULL),
('WO38', 'Bridge', 'Inspection', 'High', DATE '2025-01-17', DATE '2025-01-18', 4500.00),
('WO39', 'Road', 'Pothole', 'High', DATE '2025-01-18', DATE '2025-01-19', 510.00),
('WO40', 'Park', 'Graffiti', 'Low', DATE '2025-01-19', NULL, NULL),
('WO41', 'Road', 'Striping', 'Low', DATE '2025-01-20', NULL, NULL),
('WO42', 'Building', 'HVAC', 'High', DATE '2025-01-01', DATE '2025-01-05', 1300.00),
('WO43', 'Bridge', 'Rust', 'Medium', DATE '2025-01-02', NULL, NULL),
('WO44', 'Road', 'Resurfacing', 'Medium', DATE '2025-01-03', NULL, NULL),
('WO45', 'Park', 'Mowing', 'Low', DATE '2025-01-04', DATE '2025-01-04', 160.00),
('WO46', 'Road', 'Signal', 'High', DATE '2025-01-05', DATE '2025-01-06', 2200.00),
('WO47', 'Building', 'Roof', 'High', DATE '2025-01-06', NULL, NULL),
('WO48', 'Bridge', 'Lighting', 'Low', DATE '2025-01-07', DATE '2025-01-08', 420.00),
('WO49', 'Road', 'Pothole', 'High', DATE '2025-01-08', DATE '2025-01-09', 490.00),
('WO50', 'Park', 'Bench', 'Low', DATE '2025-01-09', DATE '2025-01-10', 260.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Order Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentInfraDB.Silver.OrderAnalysis AS
SELECT 
    OrderID,
    AssetType,
    Priority,
    Cost,
    CASE 
        WHEN CompletionDate IS NOT NULL THEN 'Completed'
        ELSE 'Open'
    END AS Status,
    TIMESTAMPDIFF(DAY, ReportDate, COALESCE(CompletionDate, DATE '2025-01-20')) AS DaysActive
FROM GovernmentInfraDB.Bronze.WorkOrders;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Asset Spend Costs
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentInfraDB.Gold.AssetSpend AS
SELECT 
    AssetType,
    COUNT(*) AS TotalOrders,
    SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenOrders,
    SUM(Cost) AS TotalSpend
FROM GovernmentInfraDB.Silver.OrderAnalysis
GROUP BY AssetType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "What is the TotalSpend for Road maintenance?"
    2. "Count open orders by Priority."
    3. "Which Asset Type has the most Work Orders?"
*/
