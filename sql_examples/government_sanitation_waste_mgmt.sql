/*
    Dremio High-Volume SQL Pattern: Government Sanitation & Waste Management
    
    Business Scenario:
    Optimizing trash collection routes and tracking recycling rates by district.
    Measuring "Diversion Rate" (Recycle/Compost vs Total Waste).
    
    Data Story:
    We track Collection events and waste distribution.
    
    Medallion Architecture:
    - Bronze: Collections.
      *Volume*: 50+ records.
    - Silver: DiversionMetrics (Recycle vs Trash).
    - Gold: DistrictDiversion (Rate calculation).
    
    Key Dremio Features:
    - Conditional Logic
    - Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentSanitationDB;
CREATE FOLDER IF NOT EXISTS GovernmentSanitationDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentSanitationDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentSanitationDB.Gold;
USE GovernmentSanitationDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentSanitationDB.Bronze.Collections (
    RouteID INT,
    District STRING,
    WasteType STRING, -- Trash, Recycle, Compost
    WeightTons DOUBLE,
    CollectionDate DATE,
    TruckID INT
);

INSERT INTO GovernmentSanitationDB.Bronze.Collections VALUES
(101, 'North', 'Trash', 5.5, DATE '2025-01-01', 50),
(101, 'North', 'Recycle', 2.0, DATE '2025-01-01', 51),
(102, 'South', 'Trash', 8.0, DATE '2025-01-01', 52),
(102, 'South', 'Recycle', 1.5, DATE '2025-01-01', 53),
(103, 'East', 'Trash', 6.2, DATE '2025-01-02', 50),
(103, 'East', 'Recycle', 3.5, DATE '2025-01-02', 51),
(104, 'West', 'Trash', 4.0, DATE '2025-01-02', 52),
(104, 'West', 'Compost', 1.0, DATE '2025-01-02', 54),
(101, 'North', 'Trash', 5.2, DATE '2025-01-08', 50),
(101, 'North', 'Recycle', 2.2, DATE '2025-01-08', 51),
(102, 'South', 'Trash', 7.8, DATE '2025-01-08', 52),
(102, 'South', 'Recycle', 1.8, DATE '2025-01-08', 53),
(103, 'East', 'Trash', 6.0, DATE '2025-01-09', 50),
(103, 'East', 'Recycle', 3.2, DATE '2025-01-09', 51),
(104, 'West', 'Trash', 3.8, DATE '2025-01-09', 52),
(104, 'West', 'Compost', 1.2, DATE '2025-01-09', 54),
(101, 'North', 'Trash', 5.0, DATE '2025-01-15', 50),
(101, 'North', 'Recycle', 2.5, DATE '2025-01-15', 51),
(102, 'South', 'Trash', 8.2, DATE '2025-01-15', 52),
(102, 'South', 'Recycle', 1.6, DATE '2025-01-15', 53),
(103, 'East', 'Trash', 6.5, DATE '2025-01-16', 50),
(103, 'East', 'Recycle', 3.8, DATE '2025-01-16', 51),
(104, 'West', 'Trash', 4.2, DATE '2025-01-16', 52),
(104, 'West', 'Compost', 1.1, DATE '2025-01-16', 54),
(101, 'North', 'Trash', 5.8, DATE '2025-01-22', 50),
(101, 'North', 'Recycle', 2.1, DATE '2025-01-22', 51),
(102, 'South', 'Trash', 7.5, DATE '2025-01-22', 52),
(102, 'South', 'Recycle', 1.7, DATE '2025-01-22', 53),
(103, 'East', 'Trash', 6.1, DATE '2025-01-23', 50),
(103, 'East', 'Recycle', 3.4, DATE '2025-01-23', 51),
(104, 'West', 'Trash', 4.1, DATE '2025-01-23', 52),
(104, 'West', 'Compost', 1.0, DATE '2025-01-23', 54),
(101, 'North', 'Compost', 0.5, DATE '2025-01-01', 55),
(102, 'South', 'Compost', 0.2, DATE '2025-01-01', 55),
(103, 'East', 'Compost', 0.8, DATE '2025-01-02', 55),
(101, 'North', 'Compost', 0.6, DATE '2025-01-08', 55),
(102, 'South', 'Compost', 0.3, DATE '2025-01-08', 55),
(103, 'East', 'Compost', 0.9, DATE '2025-01-09', 55),
(101, 'North', 'Compost', 0.7, DATE '2025-01-15', 55),
(102, 'South', 'Compost', 0.4, DATE '2025-01-15', 55),
(103, 'East', 'Compost', 1.0, DATE '2025-01-16', 55),
(101, 'North', 'Compost', 0.6, DATE '2025-01-22', 55),
(102, 'South', 'Compost', 0.3, DATE '2025-01-22', 55),
(103, 'East', 'Compost', 0.9, DATE '2025-01-23', 55),
(104, 'West', 'Recycle', 1.5, DATE '2025-01-02', 56), -- Extra truck
(104, 'West', 'Recycle', 1.6, DATE '2025-01-09', 56),
(104, 'West', 'Recycle', 1.7, DATE '2025-01-16', 56),
(104, 'West', 'Recycle', 1.5, DATE '2025-01-23', 56),
(101, 'North', 'Trash', 5.5, DATE '2025-01-29', 50),
(102, 'South', 'Trash', 8.0, DATE '2025-01-29', 52);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Diversion Logic
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSanitationDB.Silver.DiversionMetrics AS
SELECT 
    District,
    CollectionDate,
    WeightTons,
    WasteType,
    CASE 
        WHEN WasteType IN ('Recycle', 'Compost') THEN 1 
        ELSE 0 
    END AS IsDiverted
FROM GovernmentSanitationDB.Bronze.Collections;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: District Diversion Rate
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSanitationDB.Gold.DistrictDiversion AS
SELECT 
    District,
    SUM(WeightTons) AS TotalWasteGenerated,
    SUM(CASE WHEN IsDiverted = 1 THEN WeightTons ELSE 0 END) AS DivertedTons,
    (SUM(CASE WHEN IsDiverted = 1 THEN WeightTons ELSE 0 END) / SUM(WeightTons)) * 100 AS DiversionRate
FROM GovernmentSanitationDB.Silver.DiversionMetrics
GROUP BY District;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Calculate the DiversionRate for the North district."
    2. "Which district produces the most Trash?"
    3. "Show total waste generated by District."
*/
