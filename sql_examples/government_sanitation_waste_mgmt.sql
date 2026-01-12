/*
 * Government Sanitation & Waste Management Demo
 * 
 * Scenario:
 * Optimizing trash collection routes and tracking recycling rates by district.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Sanitation;
CREATE FOLDER IF NOT EXISTS RetailDB.Sanitation.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Sanitation.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Sanitation.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Sanitation.Bronze.Collections (
    RouteID INT,
    District VARCHAR,
    WasteType VARCHAR, -- Trash, Recycle, Compost
    WeightTons DOUBLE,
    CollectionDate DATE,
    TruckID INT
);

INSERT INTO RetailDB.Sanitation.Bronze.Collections VALUES
(101, 'North', 'Trash', 5.5, '2025-01-01', 50),
(101, 'North', 'Recycle', 2.0, '2025-01-01', 51),
(102, 'South', 'Trash', 8.0, '2025-01-01', 52),
(102, 'South', 'Recycle', 1.5, '2025-01-01', 53),
(103, 'East', 'Trash', 6.2, '2025-01-02', 50),
(103, 'East', 'Recycle', 3.5, '2025-01-02', 51),
(104, 'West', 'Trash', 4.0, '2025-01-02', 52),
(104, 'West', 'Compost', 1.0, '2025-01-02', 54),
(101, 'North', 'Trash', 5.2, '2025-01-08', 50),
(101, 'North', 'Recycle', 2.2, '2025-01-08', 51),
(102, 'South', 'Trash', 7.8, '2025-01-08', 52);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Sanitation.Silver.DiversionMetrics AS
SELECT 
    District,
    CollectionDate,
    WeightTons,
    WasteType,
    CASE 
        WHEN WasteType IN ('Recycle', 'Compost') THEN 1 
        ELSE 0 
    END AS IsDiverted
FROM RetailDB.Sanitation.Bronze.Collections;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Sanitation.Gold.DistrictDiversion AS
SELECT 
    District,
    SUM(WeightTons) AS TotalWasteGenerated,
    SUM(CASE WHEN IsDiverted = 1 THEN WeightTons ELSE 0 END) AS DivertedTons,
    (SUM(CASE WHEN IsDiverted = 1 THEN WeightTons ELSE 0 END) / SUM(WeightTons)) * 100 AS DiversionRate
FROM RetailDB.Sanitation.Silver.DiversionMetrics
GROUP BY District;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Calculate the DiversionRate for the 'North' district using RetailDB.Sanitation.Gold.DistrictDiversion."
*/
