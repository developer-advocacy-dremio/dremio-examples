/*
 * ESG Carbon Footprint Tracking Demo
 * 
 * Scenario:
 * Tracking emission scopes, energy intensity, and sustainability targets.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB;
CREATE FOLDER IF NOT EXISTS RetailDB.ESG;
CREATE FOLDER IF NOT EXISTS RetailDB.ESG.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.ESG.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.ESG.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.ESG.Bronze.Emissions (
    FacilityID VARCHAR,
    Year INT,
    Scope1_Tons INT, -- Direct
    Scope2_Tons INT, -- Indirect (Electricity)
    Scope3_Tons INT, -- Supply Chain
    ProductionUnits INT
);

INSERT INTO RetailDB.ESG.Bronze.Emissions VALUES
('FAC-A', 2023, 500, 300, 1000, 10000),
('FAC-A', 2024, 480, 290, 950, 11000), -- Improved efficiency
('FAC-B', 2023, 1200, 800, 2000, 25000),
('FAC-B', 2024, 1250, 810, 2100, 26000),
('FAC-C', 2023, 300, 100, 500, 5000),
('FAC-C', 2024, 280, 90, 480, 5200),
('FAC-D', 2023, 600, 400, 1200, 8000),
('FAC-D', 2024, 550, 350, 1100, 8500),
('FAC-E', 2023, 900, 600, 1500, 15000),
('FAC-E', 2024, 880, 580, 1450, 15500);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.ESG.Silver.IntensityMetrics AS
SELECT 
    FacilityID,
    Year,
    (Scope1_Tons + Scope2_Tons + Scope3_Tons) AS TotalEmissions,
    ProductionUnits,
    CAST((Scope1_Tons + Scope2_Tons + Scope3_Tons) AS DOUBLE) / ProductionUnits AS CarbonIntensity
FROM RetailDB.ESG.Bronze.Emissions;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.ESG.Gold.SustainabilityProgress AS
SELECT 
    FacilityID,
    SUM(CASE WHEN Year = 2024 THEN TotalEmissions ELSE 0 END) AS Emissions_2024,
    SUM(CASE WHEN Year = 2023 THEN TotalEmissions ELSE 0 END) AS Emissions_2023,
    (SUM(CASE WHEN Year = 2024 THEN CarbonIntensity ELSE 0 END) - 
     SUM(CASE WHEN Year = 2023 THEN CarbonIntensity ELSE 0 END)) AS IntensityChange
FROM RetailDB.ESG.Silver.IntensityMetrics
GROUP BY FacilityID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify facilities with a negative IntensityChange (improvement) in RetailDB.ESG.Gold.SustainabilityProgress."
*/
