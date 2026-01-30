/*
 * Energy: Wind Farm Telemetry Analysis
 * 
 * Scenario:
 * Monitoring wind turbine efficiency and power output against theoretical curves.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EnergyDB;
CREATE FOLDER IF NOT EXISTS EnergyDB.Renewables;
CREATE FOLDER IF NOT EXISTS EnergyDB.Renewables.Bronze;
CREATE FOLDER IF NOT EXISTS EnergyDB.Renewables.Silver;
CREATE FOLDER IF NOT EXISTS EnergyDB.Renewables.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Telemetry
-------------------------------------------------------------------------------

-- TurbineAsset Table
CREATE TABLE IF NOT EXISTS EnergyDB.Renewables.Bronze.Turbines (
    TurbineID VARCHAR,
    FarmLocation VARCHAR,
    Model VARCHAR,
    MaxCapacityMW DOUBLE,
    InstallDate DATE
);

INSERT INTO EnergyDB.Renewables.Bronze.Turbines VALUES
('T-001', 'NorthSea-01', 'Vestas V164', 8.0, '2020-01-01'),
('T-002', 'NorthSea-01', 'Vestas V164', 8.0, '2020-01-01'),
('T-003', 'NorthSea-01', 'Vestas V164', 8.0, '2020-01-01'),
('T-004', 'NorthSea-01', 'Vestas V164', 8.0, '2020-01-01'),
('T-005', 'NorthSea-01', 'Vestas V164', 8.0, '2020-01-01'),
('T-006', 'TexasPlains', 'GE Haliade-X', 12.0, '2021-06-01'),
('T-007', 'TexasPlains', 'GE Haliade-X', 12.0, '2021-06-01'),
('T-008', 'TexasPlains', 'GE Haliade-X', 12.0, '2021-06-01'),
('T-009', 'TexasPlains', 'GE Haliade-X', 12.0, '2021-06-01'),
('T-010', 'TexasPlains', 'GE Haliade-X', 12.0, '2021-06-01'),
('T-011', 'SaharaWind', 'Siemens SG', 6.0, '2019-03-15'),
('T-012', 'SaharaWind', 'Siemens SG', 6.0, '2019-03-15'),
('T-013', 'SaharaWind', 'Siemens SG', 6.0, '2019-03-15'),
('T-014', 'SaharaWind', 'Siemens SG', 6.0, '2019-03-15'),
('T-015', 'SaharaWind', 'Siemens SG', 6.0, '2019-03-15'),
('T-016', 'BalticBreeze', 'Vestas V164', 8.0, '2020-05-20'),
('T-017', 'BalticBreeze', 'Vestas V164', 8.0, '2020-05-20'),
('T-018', 'BalticBreeze', 'Vestas V164', 8.0, '2020-05-20'),
('T-019', 'BalticBreeze', 'Vestas V164', 8.0, '2020-05-20'),
('T-020', 'BalticBreeze', 'Vestas V164', 8.0, '2020-05-20'),
('T-021', 'CapeCod', 'GE Haliade-X', 12.0, '2022-01-10'),
('T-022', 'CapeCod', 'GE Haliade-X', 12.0, '2022-01-10'),
('T-023', 'CapeCod', 'GE Haliade-X', 12.0, '2022-01-10'),
('T-024', 'CapeCod', 'GE Haliade-X', 12.0, '2022-01-10'),
('T-025', 'CapeCod', 'GE Haliade-X', 12.0, '2022-01-10'),
('T-026', 'Patagonia', 'Nordex N149', 4.5, '2023-02-01'),
('T-027', 'Patagonia', 'Nordex N149', 4.5, '2023-02-01'),
('T-028', 'Patagonia', 'Nordex N149', 4.5, '2023-02-01'),
('T-029', 'Patagonia', 'Nordex N149', 4.5, '2023-02-01'),
('T-030', 'Patagonia', 'Nordex N149', 4.5, '2023-02-01'),
('T-031', 'GobiDesert', 'Goldwind GW', 3.0, '2018-08-08'),
('T-032', 'GobiDesert', 'Goldwind GW', 3.0, '2018-08-08'),
('T-033', 'GobiDesert', 'Goldwind GW', 3.0, '2018-08-08'),
('T-034', 'GobiDesert', 'Goldwind GW', 3.0, '2018-08-08'),
('T-035', 'GobiDesert', 'Goldwind GW', 3.0, '2018-08-08'),
('T-036', 'ScottishHighlands', 'Siemens SG', 6.0, '2019-11-01'),
('T-037', 'ScottishHighlands', 'Siemens SG', 6.0, '2019-11-01'),
('T-038', 'ScottishHighlands', 'Siemens SG', 6.0, '2019-11-01'),
('T-039', 'ScottishHighlands', 'Siemens SG', 6.0, '2019-11-01'),
('T-040', 'ScottishHighlands', 'Siemens SG', 6.0, '2019-11-01'),
('T-041', 'Tasmania', 'Vestas V150', 4.2, '2021-04-15'),
('T-042', 'Tasmania', 'Vestas V150', 4.2, '2021-04-15'),
('T-043', 'Tasmania', 'Vestas V150', 4.2, '2021-04-15'),
('T-044', 'Tasmania', 'Vestas V150', 4.2, '2021-04-15'),
('T-045', 'Tasmania', 'Vestas V150', 4.2, '2021-04-15'),
('T-046', 'Newfoundland', 'Enercon E-126', 7.5, '2020-09-30'),
('T-047', 'Newfoundland', 'Enercon E-126', 7.5, '2020-09-30'),
('T-048', 'Newfoundland', 'Enercon E-126', 7.5, '2020-09-30'),
('T-049', 'Newfoundland', 'Enercon E-126', 7.5, '2020-09-30'),
('T-050', 'Newfoundland', 'Enercon E-126', 7.5, '2020-09-30');

-- PowerLogs Table (Sampled hourly)
CREATE TABLE IF NOT EXISTS EnergyDB.Renewables.Bronze.PowerLogs (
    LogID INT,
    TurbineID VARCHAR,
    WindSpeed_ms DOUBLE,
    PowerOutputMW DOUBLE,
    RotorSpeedRPM DOUBLE,
    Timestamp TIMESTAMP
);

INSERT INTO EnergyDB.Renewables.Bronze.PowerLogs VALUES
(1, 'T-001', 12.0, 7.5, 12.0, '2025-06-01 12:00:00'),
(2, 'T-002', 11.5, 7.2, 11.8, '2025-06-01 12:00:00'),
(3, 'T-003', 12.2, 7.8, 12.1, '2025-06-01 12:00:00'),
(4, 'T-004', 5.0, 1.0, 5.0, '2025-06-01 12:00:00'), -- Low wind
(5, 'T-005', 0.0, 0.0, 0.0, '2025-06-01 12:00:00'), -- Maintenance?
(6, 'T-006', 15.0, 11.5, 10.0, '2025-06-01 12:00:00'),
(7, 'T-007', 14.8, 11.2, 9.8, '2025-06-01 12:00:00'),
(8, 'T-008', 15.2, 11.8, 10.1, '2025-06-01 12:00:00'),
(9, 'T-009', 25.0, 0.0, 0.0, '2025-06-01 12:00:00'), -- Cut-out speed achieved (too fast)
(10, 'T-010', 14.5, 11.0, 9.5, '2025-06-01 12:00:00'),
(11, 'T-011', 8.0, 4.0, 14.0, '2025-06-01 12:00:00'),
(12, 'T-012', 8.2, 4.1, 14.2, '2025-06-01 12:00:00'),
(13, 'T-013', 7.5, 3.5, 13.0, '2025-06-01 12:00:00'),
(14, 'T-014', 12.0, 2.0, 5.0, '2025-06-01 12:00:00'), -- Underperformance! High wind, low power.
(15, 'T-015', 8.1, 4.05, 14.1, '2025-06-01 12:00:00'),
(16, 'T-016', 10.0, 5.5, 10.0, '2025-06-01 12:00:00'),
(17, 'T-017', 10.1, 5.6, 10.1, '2025-06-01 12:00:00'),
(18, 'T-018', 9.9, 5.4, 9.9, '2025-06-01 12:00:00'),
(19, 'T-019', 10.2, 5.7, 10.2, '2025-06-01 12:00:00'),
(20, 'T-020', 10.0, 5.5, 10.0, '2025-06-01 12:00:00'),
(21, 'T-021', 13.0, 10.0, 8.5, '2025-06-01 12:00:00'),
(22, 'T-022', 13.1, 10.1, 8.6, '2025-06-01 12:00:00'),
(23, 'T-023', 12.9, 9.9, 8.4, '2025-06-01 12:00:00'),
(24, 'T-024', 2.0, 0.0, 1.0, '2025-06-01 12:00:00'), -- Cut-in not reached
(25, 'T-025', 13.0, 10.0, 8.5, '2025-06-01 12:00:00'),
(26, 'T-026', 9.0, 3.0, 16.0, '2025-06-01 12:00:00'),
(27, 'T-027', 9.1, 3.1, 16.2, '2025-06-01 12:00:00'),
(28, 'T-028', 9.2, 3.2, 16.4, '2025-06-01 12:00:00'),
(29, 'T-029', 9.0, 3.0, 16.0, '2025-06-01 12:00:00'),
(30, 'T-030', 9.0, 1.0, 5.0, '2025-06-01 12:00:00'), -- Underperformance
(31, 'T-031', 6.0, 1.0, 11.0, '2025-06-01 12:00:00'),
(32, 'T-032', 6.1, 1.1, 11.2, '2025-06-01 12:00:00'),
(33, 'T-033', 6.2, 1.2, 11.4, '2025-06-01 12:00:00'),
(34, 'T-034', 6.0, 1.0, 11.0, '2025-06-01 12:00:00'),
(35, 'T-035', 6.0, 0.1, 1.0, '2025-06-01 12:00:00'), -- Issue
(36, 'T-036', 11.0, 5.0, 13.0, '2025-06-01 12:00:00'),
(37, 'T-037', 11.1, 5.1, 13.2, '2025-06-01 12:00:00'),
(38, 'T-038', 11.2, 5.2, 13.4, '2025-06-01 12:00:00'),
(39, 'T-039', 11.0, 5.0, 13.0, '2025-06-01 12:00:00'),
(40, 'T-040', 11.0, 5.0, 13.0, '2025-06-01 12:00:00'),
(41, 'T-041', 7.0, 2.0, 10.0, '2025-06-01 12:00:00'),
(42, 'T-042', 7.1, 2.1, 10.2, '2025-06-01 12:00:00'),
(43, 'T-043', 7.2, 2.2, 10.4, '2025-06-01 12:00:00'),
(44, 'T-044', 7.0, 2.0, 10.0, '2025-06-01 12:00:00'),
(45, 'T-045', 7.0, 2.0, 10.0, '2025-06-01 12:00:00'),
(46, 'T-046', 8.5, 4.0, 9.0, '2025-06-01 12:00:00'),
(47, 'T-047', 8.6, 4.1, 9.2, '2025-06-01 12:00:00'),
(48, 'T-048', 8.7, 4.2, 9.4, '2025-06-01 12:00:00'),
(49, 'T-049', 8.5, 4.0, 9.0, '2025-06-01 12:00:00'),
(50, 'T-050', 8.5, 0.5, 2.0, '2025-06-01 12:00:00'); -- Underperformance

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficiency Calculation
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EnergyDB.Renewables.Silver.TurbineEfficiency AS
SELECT 
    l.TurbineID,
    t.FarmLocation,
    t.Model,
    l.WindSpeed_ms,
    l.PowerOutputMW,
    t.MaxCapacityMW,
    (l.PowerOutputMW / t.MaxCapacityMW) * 100 AS CapacityFactorPct,
    l.Timestamp
FROM EnergyDB.Renewables.Bronze.PowerLogs l
JOIN EnergyDB.Renewables.Bronze.Turbines t ON l.TurbineID = t.TurbineID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Underperformance Alerts
-------------------------------------------------------------------------------

-- Flag turbines where wind is sufficient (>5m/s) but power is low (<10% capacity)
CREATE OR REPLACE VIEW EnergyDB.Renewables.Gold.UnderperformingAssets AS
SELECT 
    TurbineID,
    FarmLocation,
    WindSpeed_ms,
    PowerOutputMW,
    CapacityFactorPct,
    Timestamp,
    'Check Gearbox/Blade' AS RecommendedAction
FROM EnergyDB.Renewables.Silver.TurbineEfficiency
WHERE WindSpeed_ms > 5.0 AND CapacityFactorPct < 10.0;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all underperforming turbines in the 'SaharaWind' farm using the EnergyDB.Renewables.Gold.UnderperformingAssets view."

PROMPT 2:
"Calculate the average Power Output for 'Vestas V164' model turbines from the Silver layer."

PROMPT 3:
"Show the highest Wind Speed recorded where Power Output was 0."
*/
