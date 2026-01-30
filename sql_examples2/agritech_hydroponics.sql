/*
 * AgriTech: Hydroponic Vertical Farming
 * 
 * Scenario:
 * Monitoring nutrient solution pH and LED light cycles in controlled environment agriculture.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS VerticalFarmDB;
CREATE FOLDER IF NOT EXISTS VerticalFarmDB.Operations;
CREATE FOLDER IF NOT EXISTS VerticalFarmDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS VerticalFarmDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS VerticalFarmDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Sensor Data
-------------------------------------------------------------------------------

-- HydroUnits Table
CREATE TABLE IF NOT EXISTS VerticalFarmDB.Operations.Bronze.HydroUnits (
    UnitID VARCHAR,
    CropType VARCHAR, -- Lettuce, Basil, Strawberry
    RackLevel INT,
    InstallDate DATE
);

INSERT INTO VerticalFarmDB.Operations.Bronze.HydroUnits VALUES
('U-001', 'Lettuce', 1, '2025-01-01'),
('U-002', 'Lettuce', 2, '2025-01-01'),
('U-003', 'Lettuce', 3, '2025-01-01'),
('U-004', 'Basil', 1, '2025-01-15'),
('U-005', 'Basil', 2, '2025-01-15'),
('U-006', 'Strawberry', 1, '2025-02-01'),
('U-007', 'Strawberry', 2, '2025-02-01'),
('U-008', 'Spinach', 1, '2025-02-10'),
('U-009', 'Spinach', 2, '2025-02-10'),
('U-010', 'Kale', 1, '2025-02-15'),
('U-011', 'Lettuce', 1, '2025-01-01'),
('U-012', 'Lettuce', 2, '2025-01-01'),
('U-013', 'Lettuce', 3, '2025-01-01'),
('U-014', 'Basil', 1, '2025-01-15'),
('U-015', 'Basil', 2, '2025-01-15'),
('U-016', 'Strawberry', 1, '2025-02-01'),
('U-017', 'Strawberry', 2, '2025-02-01'),
('U-018', 'Spinach', 1, '2025-02-10'),
('U-019', 'Spinach', 2, '2025-02-10'),
('U-020', 'Kale', 1, '2025-02-15'),
('U-021', 'Lettuce', 1, '2025-01-01'),
('U-022', 'Lettuce', 2, '2025-01-01'),
('U-023', 'Lettuce', 3, '2025-01-01'),
('U-024', 'Basil', 1, '2025-01-15'),
('U-025', 'Basil', 2, '2025-01-15'),
('U-026', 'Strawberry', 1, '2025-02-01'),
('U-027', 'Strawberry', 2, '2025-02-01'),
('U-028', 'Spinach', 1, '2025-02-10'),
('U-029', 'Spinach', 2, '2025-02-10'),
('U-030', 'Kale', 1, '2025-02-15'),
('U-031', 'Lettuce', 1, '2025-01-01'),
('U-032', 'Lettuce', 2, '2025-01-01'),
('U-033', 'Lettuce', 3, '2025-01-01'),
('U-034', 'Basil', 1, '2025-01-15'),
('U-035', 'Basil', 2, '2025-01-15'),
('U-036', 'Strawberry', 1, '2025-02-01'),
('U-037', 'Strawberry', 2, '2025-02-01'),
('U-038', 'Spinach', 1, '2025-02-10'),
('U-039', 'Spinach', 2, '2025-02-10'),
('U-040', 'Kale', 1, '2025-02-15'),
('U-041', 'Lettuce', 1, '2025-01-01'),
('U-042', 'Lettuce', 2, '2025-01-01'),
('U-043', 'Lettuce', 3, '2025-01-01'),
('U-044', 'Basil', 1, '2025-01-15'),
('U-045', 'Basil', 2, '2025-01-15'),
('U-046', 'Strawberry', 1, '2025-02-01'),
('U-047', 'Strawberry', 2, '2025-02-01'),
('U-048', 'Spinach', 1, '2025-02-10'),
('U-049', 'Spinach', 2, '2025-02-10'),
('U-050', 'Kale', 1, '2025-02-15');

-- NutrientReadings Table
CREATE TABLE IF NOT EXISTS VerticalFarmDB.Operations.Bronze.NutrientReadings (
    ReadingID INT,
    UnitID VARCHAR,
    PHLevel DOUBLE, -- Ideal 5.5 - 6.5
    EC_Level DOUBLE, -- Electrical Conductivity
    WaterTempCelsius DOUBLE,
    ReadingTimestamp TIMESTAMP
);

INSERT INTO VerticalFarmDB.Operations.Bronze.NutrientReadings VALUES
(1, 'U-001', 6.0, 1.2, 20.0, '2025-06-01 08:00:00'),
(2, 'U-002', 5.8, 1.3, 20.1, '2025-06-01 08:00:00'),
(3, 'U-003', 6.2, 1.1, 19.9, '2025-06-01 08:00:00'),
(4, 'U-004', 7.0, 1.5, 21.0, '2025-06-01 08:00:00'), -- High pH
(5, 'U-005', 4.5, 1.0, 19.5, '2025-06-01 08:00:00'), -- Low pH
(6, 'U-006', 5.5, 1.8, 20.5, '2025-06-01 08:00:00'),
(7, 'U-007', 5.6, 1.9, 20.6, '2025-06-01 08:00:00'),
(8, 'U-008', 5.9, 1.4, 20.0, '2025-06-01 08:00:00'),
(9, 'U-009', 6.0, 1.4, 20.0, '2025-06-01 08:00:00'),
(10, 'U-010', 6.1, 1.6, 19.8, '2025-06-01 08:00:00'),
(11, 'U-011', 6.0, 1.2, 20.0, '2025-06-01 08:00:00'),
(12, 'U-012', 5.8, 1.3, 20.1, '2025-06-01 08:00:00'),
(13, 'U-013', 6.2, 1.1, 19.9, '2025-06-01 08:00:00'),
(14, 'U-014', 7.2, 1.5, 21.0, '2025-06-01 08:00:00'), -- High pH
(15, 'U-015', 4.8, 1.0, 19.5, '2025-06-01 08:00:00'), -- Low pH
(16, 'U-016', 5.5, 1.8, 20.5, '2025-06-01 08:00:00'),
(17, 'U-017', 5.6, 1.9, 20.6, '2025-06-01 08:00:00'),
(18, 'U-018', 5.9, 1.4, 20.0, '2025-06-01 08:00:00'),
(19, 'U-019', 6.0, 1.4, 20.0, '2025-06-01 08:00:00'),
(20, 'U-020', 6.1, 1.6, 19.8, '2025-06-01 08:00:00'),
(21, 'U-021', 6.0, 1.2, 20.0, '2025-06-01 08:00:00'),
(22, 'U-022', 5.8, 1.3, 20.1, '2025-06-01 08:00:00'),
(23, 'U-023', 6.2, 1.1, 19.9, '2025-06-01 08:00:00'),
(24, 'U-024', 6.9, 1.5, 21.0, '2025-06-01 08:00:00'), -- High pH
(25, 'U-025', 4.9, 1.0, 19.5, '2025-06-01 08:00:00'), -- Low pH
(26, 'U-026', 5.5, 1.8, 20.5, '2025-06-01 08:00:00'),
(27, 'U-027', 5.6, 1.9, 20.6, '2025-06-01 08:00:00'),
(28, 'U-028', 5.9, 1.4, 20.0, '2025-06-01 08:00:00'),
(29, 'U-029', 6.0, 1.4, 20.0, '2025-06-01 08:00:00'),
(30, 'U-030', 6.1, 1.6, 19.8, '2025-06-01 08:00:00'),
(31, 'U-031', 6.0, 1.2, 20.0, '2025-06-01 08:00:00'),
(32, 'U-032', 5.8, 1.3, 20.1, '2025-06-01 08:00:00'),
(33, 'U-033', 6.2, 1.1, 19.9, '2025-06-01 08:00:00'),
(34, 'U-034', 6.8, 1.5, 21.0, '2025-06-01 08:00:00'), -- High pH
(35, 'U-035', 5.0, 1.0, 19.5, '2025-06-01 08:00:00'), -- Low pH
(36, 'U-036', 5.5, 1.8, 20.5, '2025-06-01 08:00:00'),
(37, 'U-037', 5.6, 1.9, 20.6, '2025-06-01 08:00:00'),
(38, 'U-038', 5.9, 1.4, 20.0, '2025-06-01 08:00:00'),
(39, 'U-039', 6.0, 1.4, 20.0, '2025-06-01 08:00:00'),
(40, 'U-040', 6.1, 1.6, 19.8, '2025-06-01 08:00:00'),
(41, 'U-041', 6.0, 1.2, 20.0, '2025-06-01 08:00:00'),
(42, 'U-042', 5.8, 1.3, 20.1, '2025-06-01 08:00:00'),
(43, 'U-043', 6.2, 1.1, 19.9, '2025-06-01 08:00:00'),
(44, 'U-044', 6.7, 1.5, 21.0, '2025-06-01 08:00:00'), -- High pH
(45, 'U-045', 5.1, 1.0, 19.5, '2025-06-01 08:00:00'), -- Low pH
(46, 'U-046', 5.5, 1.8, 20.5, '2025-06-01 08:00:00'),
(47, 'U-047', 5.6, 1.9, 20.6, '2025-06-01 08:00:00'),
(48, 'U-048', 5.9, 1.4, 20.0, '2025-06-01 08:00:00'),
(49, 'U-049', 6.0, 1.4, 20.0, '2025-06-01 08:00:00'),
(50, 'U-050', 6.1, 1.6, 19.8, '2025-06-01 08:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Environment Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW VerticalFarmDB.Operations.Silver.UnitStatus AS
SELECT 
    h.UnitID,
    h.CropType,
    r.PHLevel,
    r.EC_Level,
    r.WaterTempCelsius,
    CASE 
        WHEN r.PHLevel < 5.5 THEN 'Acidic - Action Req'
        WHEN r.PHLevel > 6.5 THEN 'Alkaline - Action Req'
        ELSE 'Optimal'
    END AS PHStatus,
    r.ReadingTimestamp AS Timestamp
FROM VerticalFarmDB.Operations.Bronze.NutrientReadings r
JOIN VerticalFarmDB.Operations.Bronze.HydroUnits h ON r.UnitID = h.UnitID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Alert Dashboard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW VerticalFarmDB.Operations.Gold.ActionBoard AS
SELECT 
    UnitID,
    CropType,
    PHLevel,
    PHStatus,
    Timestamp 
FROM VerticalFarmDB.Operations.Silver.UnitStatus
WHERE PHStatus != 'Optimal';

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify all distinct crop types that currently have acidic pH levels in the Silver layer."

PROMPT 2:
"Count the total number of alerts in the VerticalFarmDB.Operations.Gold.ActionBoard view."

PROMPT 3:
"Calculate the average Water Frequency (EC Level) for 'Lettuce' units."
*/
