/*
 * Environmental: Wildfire Risk Modeling
 * 
 * Scenario:
 * Correlating vegetation moisture content with weather forecasts to predict fire risk potential.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EcoRiskDB;
CREATE FOLDER IF NOT EXISTS EcoRiskDB.Conservation;
CREATE FOLDER IF NOT EXISTS EcoRiskDB.Conservation.Bronze;
CREATE FOLDER IF NOT EXISTS EcoRiskDB.Conservation.Silver;
CREATE FOLDER IF NOT EXISTS EcoRiskDB.Conservation.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Environmental Data
-------------------------------------------------------------------------------

-- VegetationIndex Table (Satellite Data)
CREATE TABLE IF NOT EXISTS EcoRiskDB.Conservation.Bronze.VegetationIndex (
    ZoneID VARCHAR,
    ZoneName VARCHAR,
    NDVI_Score DOUBLE, -- Normalized Difference Vegetation Index (Greenness)
    MoistureContentPercent DOUBLE, -- < 30% is dry fuel
    ObservationDate DATE
);

INSERT INTO EcoRiskDB.Conservation.Bronze.VegetationIndex VALUES
('Z-001', 'North Canyon', 0.4, 25.0, '2025-08-01'), -- Dry
('Z-002', 'South Ridge', 0.7, 60.0, '2025-08-01'),
('Z-003', 'East Valley', 0.5, 35.0, '2025-08-01'),
('Z-004', 'West Slope', 0.2, 10.0, '2025-08-01'), -- Very Dry
('Z-005', 'Central Park', 0.8, 70.0, '2025-08-01'),
('Z-006', 'Highlands', 0.3, 20.0, '2025-08-01'), -- Dry
('Z-007', 'Lowlands', 0.6, 50.0, '2025-08-01'),
('Z-008', 'River Basin', 0.9, 90.0, '2025-08-01'),
('Z-009', 'Old Forest', 0.5, 40.0, '2025-08-01'),
('Z-010', 'Scrublands', 0.2, 15.0, '2025-08-01'), -- Very Dry
('Z-011', 'Pine Hill', 0.3, 22.0, '2025-08-01'),
('Z-012', 'Oak Grove', 0.6, 55.0, '2025-08-01'),
('Z-013', 'Cedar Point', 0.4, 30.0, '2025-08-01'),
('Z-014', 'Maple Creek', 0.7, 65.0, '2025-08-01'),
('Z-015', 'Birch Wood', 0.5, 45.0, '2025-08-01'),
('Z-016', 'Elm Street', 0.8, 75.0, '2025-08-01'),
('Z-017', 'Ash Grove', 0.4, 28.0, '2025-08-01'), -- Dry
('Z-018', 'Fir Ridge', 0.3, 18.0, '2025-08-01'), -- Very Dry
('Z-019', 'Spruce Valley', 0.6, 52.0, '2025-08-01'),
('Z-020', 'Redwood Stand', 0.9, 85.0, '2025-08-01'),
('Z-021', 'Sequoia Park', 0.8, 80.0, '2025-08-01'),
('Z-022', 'Cypress Swamp', 0.9, 95.0, '2025-08-01'),
('Z-023', 'Desert Edge', 0.1, 5.0, '2025-08-01'), -- Extremely Dry
('Z-024', 'Cactus Plain', 0.1, 8.0, '2025-08-01'), -- Extremely Dry
('Z-025', 'Grassland', 0.4, 25.0, '2025-08-01'), -- Dry fuel
('Z-026', 'Meadow', 0.6, 50.0, '2025-08-01'),
('Z-027', 'Pasture', 0.5, 40.0, '2025-08-01'),
('Z-028', 'Orchard', 0.7, 60.0, '2025-08-01'),
('Z-029', 'Vineyard', 0.6, 55.0, '2025-08-01'),
('Z-030', 'Farm Field', 0.5, 30.0, '2025-08-01'), -- Harvest dried?
('Z-031', 'Rocky Top', 0.1, 10.0, '2025-08-01'),
('Z-032', 'Sandy Beach', 0.1, 5.0, '2025-08-01'),
('Z-033', 'Cliffside', 0.2, 12.0, '2025-08-01'),
('Z-034', 'Peak Summit', 0.1, 15.0, '2025-08-01'),
('Z-035', 'Alpine Meadow', 0.6, 50.0, '2025-08-01'),
('Z-036', 'Tundra', 0.4, 40.0, '2025-08-01'),
('Z-037', 'Wetland', 0.9, 90.0, '2025-08-01'),
('Z-038', 'Marsh', 0.9, 92.0, '2025-08-01'),
('Z-039', 'Bog', 0.9, 95.0, '2025-08-01'),
('Z-040', 'Fen', 0.8, 88.0, '2025-08-01'),
('Z-041', 'Mangrove', 0.9, 90.0, '2025-08-01'),
('Z-042', 'Salt Flat', 0.1, 5.0, '2025-08-01'),
('Z-043', 'Dune', 0.1, 5.0, '2025-08-01'),
('Z-044', 'Oasis', 0.8, 75.0, '2025-08-01'),
('Z-045', 'Savanna', 0.3, 20.0, '2025-08-01'), -- Dry
('Z-046', 'Steppe', 0.3, 22.0, '2025-08-01'), -- Dry
('Z-047', 'Prairie', 0.4, 30.0, '2025-08-01'),
('Z-048', 'Chaparral', 0.3, 15.0, '2025-08-01'), -- High Fire Risk
('Z-049', 'Taiga', 0.5, 45.0, '2025-08-01'),
('Z-050', 'Rainforest', 0.9, 95.0, '2025-08-01');

-- WeatherForecast Table
CREATE TABLE IF NOT EXISTS EcoRiskDB.Conservation.Bronze.WeatherForecast (
    ForecastID INT,
    ZoneID VARCHAR,
    TempCelsius DOUBLE,
    WindSpeedKmH DOUBLE,
    HumidityPercent DOUBLE,
    LightningRisk BOOLEAN,
    ForecastDate DATE
);

INSERT INTO EcoRiskDB.Conservation.Bronze.WeatherForecast VALUES
(1, 'Z-001', 35.0, 40.0, 15.0, true, '2025-08-01'), -- Danger!
(2, 'Z-002', 25.0, 10.0, 50.0, false, '2025-08-01'),
(3, 'Z-003', 28.0, 15.0, 40.0, false, '2025-08-01'),
(4, 'Z-004', 38.0, 50.0, 10.0, true, '2025-08-01'), -- Extreme Danger!
(5, 'Z-005', 22.0, 5.0, 60.0, false, '2025-08-01'),
(6, 'Z-006', 32.0, 30.0, 20.0, true, '2025-08-01'), -- Danger
(7, 'Z-007', 26.0, 12.0, 45.0, false, '2025-08-01'),
(8, 'Z-008', 20.0, 5.0, 70.0, false, '2025-08-01'),
(9, 'Z-009', 29.0, 18.0, 35.0, false, '2025-08-01'),
(10, 'Z-010', 36.0, 45.0, 12.0, true, '2025-08-01'), -- Extreme Danger
(11, 'Z-011', 33.0, 25.0, 25.0, false, '2025-08-01'),
(12, 'Z-012', 27.0, 10.0, 48.0, false, '2025-08-01'),
(13, 'Z-013', 30.0, 20.0, 30.0, false, '2025-08-01'),
(14, 'Z-014', 24.0, 8.0, 55.0, false, '2025-08-01'),
(15, 'Z-015', 28.0, 15.0, 42.0, false, '2025-08-01'),
(16, 'Z-016', 23.0, 6.0, 65.0, false, '2025-08-01'),
(17, 'Z-017', 31.0, 28.0, 22.0, true, '2025-08-01'), -- Danger
(18, 'Z-018', 34.0, 35.0, 18.0, true, '2025-08-01'), -- Danger
(19, 'Z-019', 26.0, 10.0, 50.0, false, '2025-08-01'),
(20, 'Z-020', 21.0, 5.0, 75.0, false, '2025-08-01'),
(21, 'Z-021', 22.0, 6.0, 70.0, false, '2025-08-01'),
(22, 'Z-022', 25.0, 8.0, 80.0, false, '2025-08-01'),
(23, 'Z-023', 40.0, 20.0, 5.0, false, '2025-08-01'), -- Hot & Dry, no fuel?
(24, 'Z-024', 41.0, 25.0, 5.0, false, '2025-08-01'),
(25, 'Z-025', 30.0, 35.0, 20.0, true, '2025-08-01'), -- Fast spreading grass fire risk
(26, 'Z-026', 25.0, 10.0, 45.0, false, '2025-08-01'),
(27, 'Z-027', 28.0, 15.0, 40.0, false, '2025-08-01'),
(28, 'Z-028', 26.0, 10.0, 50.0, false, '2025-08-01'),
(29, 'Z-029', 27.0, 12.0, 45.0, false, '2025-08-01'),
(30, 'Z-030', 32.0, 25.0, 25.0, false, '2025-08-01'),
(31, 'Z-031', 20.0, 40.0, 30.0, true, '2025-08-01'),
(32, 'Z-032', 25.0, 15.0, 60.0, false, '2025-08-01'),
(33, 'Z-033', 22.0, 20.0, 50.0, false, '2025-08-01'),
(34, 'Z-034', 15.0, 50.0, 40.0, true, '2025-08-01'),
(35, 'Z-035', 18.0, 15.0, 50.0, false, '2025-08-01'),
(36, 'Z-036', 10.0, 20.0, 60.0, false, '2025-08-01'),
(37, 'Z-037', 25.0, 10.0, 80.0, false, '2025-08-01'),
(38, 'Z-038', 26.0, 10.0, 82.0, false, '2025-08-01'),
(39, 'Z-039', 24.0, 8.0, 85.0, false, '2025-08-01'),
(40, 'Z-040', 23.0, 9.0, 80.0, false, '2025-08-01'),
(41, 'Z-041', 28.0, 15.0, 85.0, false, '2025-08-01'),
(42, 'Z-042', 35.0, 20.0, 10.0, false, '2025-08-01'),
(43, 'Z-043', 36.0, 25.0, 10.0, false, '2025-08-01'),
(44, 'Z-044', 30.0, 10.0, 40.0, false, '2025-08-01'),
(45, 'Z-045', 38.0, 30.0, 15.0, true, '2025-08-01'), -- Danger
(46, 'Z-046', 35.0, 30.0, 18.0, true, '2025-08-01'), -- Danger
(47, 'Z-047', 30.0, 35.0, 25.0, true, '2025-08-01'),
(48, 'Z-048', 40.0, 50.0, 10.0, true, '2025-08-01'), -- Extreme
(49, 'Z-049', 15.0, 15.0, 50.0, false, '2025-08-01'),
(50, 'Z-050', 30.0, 5.0, 90.0, false, '2025-08-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Fire Potential Index (FPI)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EcoRiskDB.Conservation.Silver.FireRiskAnalysis AS
SELECT 
    v.ZoneID,
    v.ZoneName,
    v.MoistureContentPercent,
    w.TempCelsius,
    w.WindSpeedKmH,
    w.HumidityPercent,
    w.LightningRisk,
    -- Simplified Fire Potential: High Temp (>30), Low Humidity (<20), High Wind (>30) OR Lightning + Dry Fuel (<25% moisture)
    CASE 
        WHEN w.TempCelsius > 30 AND w.HumidityPercent < 20 AND w.WindSpeedKmH > 30 THEN 'Extreme'
        WHEN w.LightningRisk = true AND v.MoistureContentPercent < 25 THEN 'High'
        WHEN v.MoistureContentPercent < 20 THEN 'Moderate'
        ELSE 'Low'
    END AS RiskLevel
FROM EcoRiskDB.Conservation.Bronze.VegetationIndex v
JOIN EcoRiskDB.Conservation.Bronze.WeatherForecast w ON v.ZoneID = w.ZoneID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Alert Dashboard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EcoRiskDB.Conservation.Gold.RiskMap AS
SELECT 
    RiskLevel,
    COUNT(ZoneID) AS ZoneCount,
    -- Dremio has list_agg, or we can just count. LISTAGG is good for prompts.
    LISTAGG(ZoneName, ', ') WITHIN GROUP (ORDER BY ZoneName) AS ImpactedZones
FROM EcoRiskDB.Conservation.Silver.FireRiskAnalysis
GROUP BY RiskLevel;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all zones designated as 'Extreme' RiskLevel from the EcoRiskDB.Conservation.Gold.RiskMap view."

PROMPT 2:
"Identify zones with LightningRisk set to true where MoistureContentPercent is below 30% using the Silver Layer."

PROMPT 3:
"Show the weather conditions (Temp, Wind, Humidity) for 'North Canyon' zone."
*/
