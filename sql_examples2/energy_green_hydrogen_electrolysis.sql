/*
 * Energy: Green Hydrogen Electrolysis Optimization
 * 
 * Scenario:
 * Optimizing hydrogen production schedules by correlating renewable energy input pricing with electrolyzer efficiency.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HydrogenDB;
CREATE FOLDER IF NOT EXISTS HydrogenDB.Production;
CREATE FOLDER IF NOT EXISTS HydrogenDB.Production.Bronze;
CREATE FOLDER IF NOT EXISTS HydrogenDB.Production.Silver;
CREATE FOLDER IF NOT EXISTS HydrogenDB.Production.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Electrolyzer & Grid Data
-------------------------------------------------------------------------------

-- ElectrolyzerStats Table
CREATE TABLE IF NOT EXISTS HydrogenDB.Production.Bronze.ElectrolyzerStats (
    UnitID VARCHAR,
    SiteID VARCHAR,
    InputPowerMW DOUBLE,
    HydrogenOutputKg DOUBLE,
    TemperatureC DOUBLE,
    ReadingTimestamp TIMESTAMP
);

INSERT INTO HydrogenDB.Production.Bronze.ElectrolyzerStats VALUES
('E-001', 'Site-A', 5.0, 100.0, 60.0, '2026-10-01 08:00:00'),
('E-001', 'Site-A', 5.1, 102.0, 61.0, '2026-10-01 09:00:00'),
('E-001', 'Site-A', 4.9, 98.0, 59.0, '2026-10-01 10:00:00'),
('E-002', 'Site-A', 4.5, 90.0, 58.0, '2026-10-01 08:00:00'), -- Low eff
('E-002', 'Site-A', 4.5, 91.0, 58.5, '2026-10-01 09:00:00'),
('E-002', 'Site-A', 4.6, 92.0, 59.0, '2026-10-01 10:00:00'),
('E-003', 'Site-B', 6.0, 120.0, 65.0, '2026-10-01 08:00:00'),
('E-003', 'Site-B', 6.0, 121.0, 65.5, '2026-10-01 09:00:00'),
('E-003', 'Site-B', 5.9, 119.0, 64.0, '2026-10-01 10:00:00'),
('E-001', 'Site-A', 2.0, 40.0, 50.0, '2026-10-01 22:00:00'), -- Overnight
('E-002', 'Site-A', 2.0, 38.0, 49.0, '2026-10-01 22:00:00'),
('E-003', 'Site-B', 2.5, 50.0, 52.0, '2026-10-01 22:00:00'),
('E-001', 'Site-A', 5.2, 105.0, 62.0, '2026-10-02 12:00:00'),
('E-002', 'Site-A', 4.8, 95.0, 59.0, '2026-10-02 12:00:00'),
('E-003', 'Site-B', 6.1, 122.0, 66.0, '2026-10-02 12:00:00'),
('E-001', 'Site-A', 5.0, 100.0, 60.0, '2026-10-02 13:00:00'),
('E-002', 'Site-A', 4.5, 90.0, 58.0, '2026-10-02 13:00:00'),
('E-003', 'Site-B', 6.0, 120.0, 65.0, '2026-10-02 13:00:00'),
('E-004', 'Site-B', 5.5, 110.0, 63.0, '2026-10-02 13:00:00'),
('E-005', 'Site-C', 3.0, 60.0, 55.0, '2026-10-02 13:00:00'),
('E-004', 'Site-B', 5.5, 110.0, 63.0, '2026-10-02 14:00:00'),
('E-005', 'Site-C', 3.0, 60.0, 55.0, '2026-10-02 14:00:00'),
('E-004', 'Site-B', 5.5, 110.0, 63.0, '2026-10-02 15:00:00'),
('E-005', 'Site-C', 3.0, 60.0, 55.0, '2026-10-02 15:00:00'),
('E-006', 'Site-C', 3.2, 64.0, 56.0, '2026-10-02 15:00:00'),
('E-007', 'Site-A', 5.1, 102.0, 61.0, '2026-10-02 15:00:00'),
('E-006', 'Site-C', 3.2, 64.0, 56.0, '2026-10-02 16:00:00'),
('E-007', 'Site-A', 5.1, 102.0, 61.0, '2026-10-02 16:00:00'),
('E-006', 'Site-C', 3.2, 64.0, 56.0, '2026-10-02 17:00:00'),
('E-007', 'Site-A', 5.1, 102.0, 61.0, '2026-10-02 17:00:00'),
('E-008', 'Site-B', 6.2, 125.0, 67.0, '2026-10-02 17:00:00'),
('E-009', 'Site-C', 3.5, 70.0, 58.0, '2026-10-02 17:00:00'),
('E-008', 'Site-B', 6.2, 125.0, 67.0, '2026-10-02 18:00:00'),
('E-009', 'Site-C', 3.5, 70.0, 58.0, '2026-10-02 18:00:00'),
('E-008', 'Site-B', 6.2, 125.0, 67.0, '2026-10-02 19:00:00'),
('E-009', 'Site-C', 3.5, 70.0, 58.0, '2026-10-02 19:00:00'),
('E-010', 'Site-A', 4.8, 96.0, 59.0, '2026-10-02 19:00:00'),
('E-011', 'Site-B', 5.8, 116.0, 64.0, '2026-10-02 19:00:00'),
('E-010', 'Site-A', 4.8, 96.0, 59.0, '2026-10-02 20:00:00'),
('E-011', 'Site-B', 5.8, 116.0, 64.0, '2026-10-02 20:00:00'),
('E-010', 'Site-A', 4.8, 96.0, 59.0, '2026-10-02 21:00:00'),
('E-011', 'Site-B', 5.8, 116.0, 64.0, '2026-10-02 21:00:00'),
('E-012', 'Site-C', 3.1, 62.0, 55.0, '2026-10-02 21:00:00'),
('E-013', 'Site-A', 5.3, 106.0, 62.0, '2026-10-02 21:00:00'),
('E-012', 'Site-C', 3.1, 62.0, 55.0, '2026-10-02 22:00:00'),
('E-013', 'Site-A', 5.3, 106.0, 62.0, '2026-10-02 22:00:00'),
('E-012', 'Site-C', 3.1, 62.0, 55.0, '2026-10-02 23:00:00'),
('E-013', 'Site-A', 5.3, 106.0, 62.0, '2026-10-02 23:00:00'),
('E-014', 'Site-B', 6.3, 126.0, 68.0, '2026-10-02 23:00:00'),
('E-015', 'Site-C', 3.6, 72.0, 59.0, '2026-10-02 23:00:00');

-- EnergyPrices Table
CREATE TABLE IF NOT EXISTS HydrogenDB.Production.Bronze.EnergyPrices (
    GridZone VARCHAR,
    PricePerMWh DOUBLE,
    RenewableMixPct DOUBLE,
    PriceTimestamp TIMESTAMP
);

INSERT INTO HydrogenDB.Production.Bronze.EnergyPrices VALUES
('Site-A', 50.0, 20.0, '2026-10-01 08:00:00'), -- Peak
('Site-A', 45.0, 25.0, '2026-10-01 09:00:00'),
('Site-A', 40.0, 30.0, '2026-10-01 10:00:00'),
('Site-B', 48.0, 22.0, '2026-10-01 08:00:00'),
('Site-B', 42.0, 28.0, '2026-10-01 09:00:00'),
('Site-B', 38.0, 35.0, '2026-10-01 10:00:00'),
('Site-A', 10.0, 80.0, '2026-10-01 22:00:00'), -- Off-peak (Wind)
('Site-B', 12.0, 75.0, '2026-10-01 22:00:00'),
('Site-A', 15.0, 70.0, '2026-10-02 12:00:00'), -- Solar peak
('Site-B', 18.0, 65.0, '2026-10-02 12:00:00'),
('Site-A', 20.0, 60.0, '2026-10-02 13:00:00'),
('Site-B', 22.0, 55.0, '2026-10-02 13:00:00'),
('Site-C', 25.0, 50.0, '2026-10-02 13:00:00'),
('Site-A', 25.0, 55.0, '2026-10-02 14:00:00'),
('Site-B', 28.0, 50.0, '2026-10-02 14:00:00'),
('Site-C', 30.0, 45.0, '2026-10-02 14:00:00'),
('Site-A', 30.0, 50.0, '2026-10-02 15:00:00'),
('Site-B', 32.0, 45.0, '2026-10-02 15:00:00'),
('Site-C', 35.0, 40.0, '2026-10-02 15:00:00'),
('Site-A', 35.0, 45.0, '2026-10-02 16:00:00'),
('Site-B', 38.0, 40.0, '2026-10-02 16:00:00'),
('Site-C', 40.0, 35.0, '2026-10-02 16:00:00'),
('Site-A', 40.0, 40.0, '2026-10-02 17:00:00'),
('Site-B', 42.0, 35.0, '2026-10-02 17:00:00'),
('Site-C', 45.0, 30.0, '2026-10-02 17:00:00'),
('Site-A', 45.0, 35.0, '2026-10-02 18:00:00'),
('Site-B', 48.0, 30.0, '2026-10-02 18:00:00'),
('Site-C', 50.0, 25.0, '2026-10-02 18:00:00'),
('Site-A', 50.0, 30.0, '2026-10-02 19:00:00'),
('Site-B', 52.0, 25.0, '2026-10-02 19:00:00'),
('Site-C', 55.0, 20.0, '2026-10-02 19:00:00'),
('Site-A', 55.0, 25.0, '2026-10-02 20:00:00'),
('Site-B', 58.0, 20.0, '2026-10-02 20:00:00'),
('Site-C', 60.0, 15.0, '2026-10-02 20:00:00'),
('Site-A', 60.0, 20.0, '2026-10-02 21:00:00'),
('Site-B', 62.0, 15.0, '2026-10-02 21:00:00'),
('Site-C', 65.0, 10.0, '2026-10-02 21:00:00'),
('Site-A', 40.0, 40.0, '2026-10-02 22:00:00'),
('Site-B', 42.0, 35.0, '2026-10-02 22:00:00'),
('Site-C', 45.0, 30.0, '2026-10-02 22:00:00'),
('Site-A', 30.0, 50.0, '2026-10-02 23:00:00'),
('Site-B', 32.0, 45.0, '2026-10-02 23:00:00'),
('Site-C', 35.0, 40.0, '2026-10-02 23:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficiency Metrics
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HydrogenDB.Production.Silver.ProductionEfficiency AS
SELECT 
    e.UnitID,
    e.SiteID,
    e.ReadingTimestamp,
    e.InputPowerMW,
    e.HydrogenOutputKg,
    -- Calculate Specific Energy Consumption (kWh/kg) -> MW * 1000 / Kg
    (e.InputPowerMW * 1000.0) / e.HydrogenOutputKg AS EnergyPerKg,
    p.PricePerMWh,
    p.RenewableMixPct
FROM HydrogenDB.Production.Bronze.ElectrolyzerStats e
LEFT JOIN HydrogenDB.Production.Bronze.EnergyPrices p 
    ON e.SiteID = p.GridZone 
    AND e.ReadingTimestamp = p.PriceTimestamp;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Cost Optimization
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HydrogenDB.Production.Gold.ProductionSchedule AS
SELECT 
    UnitID,
    SiteID,
    ReadingTimestamp,
    EnergyPerKg,
    PricePerMWh,
    RenewableMixPct,
    -- Calculate Cost per Kg H2
    (EnergyPerKg * PricePerMWh / 1000.0) AS CostPerKg, -- (kWh/kg) * ($/MWh / 1000 -> $/kWh)
    -- Decision
    CASE 
        WHEN (EnergyPerKg * PricePerMWh / 1000.0) < 1.0 AND RenewableMixPct > 50 THEN 'Maximize Production'
        WHEN (EnergyPerKg * PricePerMWh / 1000.0) > 3.0 THEN 'Idle / Min Load'
        ELSE 'Normal Operation'
    END AS OperationalDirective
FROM HydrogenDB.Production.Silver.ProductionEfficiency;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all units advised to 'Maximize Production' in the Gold schedule."

PROMPT 2:
"Calculate the average production cost per kg for 'Site-A' across all timestamps."

PROMPT 3:
"Identify timestamps where RenewableMixPct was above 70% but production was 'Idle'."
*/
