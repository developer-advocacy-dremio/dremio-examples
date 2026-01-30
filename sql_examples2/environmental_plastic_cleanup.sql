/*
 * Environmental: Ocean Plastic Cleanup Operations
 * 
 * Scenario:
 * Optimizing interception barrier placements in river mouths to stop plastic from entering the ocean based on flow and debris sensors.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS OceanCleanupDB;
CREATE FOLDER IF NOT EXISTS OceanCleanupDB.Operations;
CREATE FOLDER IF NOT EXISTS OceanCleanupDB.Operations.Bronze;
CREATE FOLDER IF NOT EXISTS OceanCleanupDB.Operations.Silver;
CREATE FOLDER IF NOT EXISTS OceanCleanupDB.Operations.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Barrier Sensors
-------------------------------------------------------------------------------

-- DebrisCollection Table
CREATE TABLE IF NOT EXISTS OceanCleanupDB.Operations.Bronze.DebrisCollection (
    BarrierID VARCHAR,
    LocationName VARCHAR,
    RiverSystem VARCHAR,
    PlasticWeightKg DOUBLE,
    OrganicWeightKg DOUBLE, -- Bycatch/Wood
    FlowRateM3S DOUBLE, -- Cubic meters per second
    Status VARCHAR, -- Operational, Maintenance, Offline
    CollectionDate DATE
);

INSERT INTO OceanCleanupDB.Operations.Bronze.DebrisCollection VALUES
('B-001', 'Citarum Mouth', 'Citarum', 500.0, 50.0, 120.0, 'Operational', '2025-07-01'),
('B-001', 'Citarum Mouth', 'Citarum', 550.0, 60.0, 125.0, 'Operational', '2025-07-02'),
('B-002', 'Klang Gate', 'Klang', 300.0, 20.0, 80.0, 'Operational', '2025-07-01'),
('B-003', 'Rio Motagua', 'Motagua', 1200.0, 200.0, 250.0, 'Operational', '2025-07-01'), -- Heavy load
('B-003', 'Rio Motagua', 'Motagua', 1100.0, 180.0, 240.0, 'Operational', '2025-07-02'),
('B-004', 'Pasig Point', 'Pasig', 800.0, 100.0, 150.0, 'Operational', '2025-07-01'),
('B-005', 'Ganges Delta', 'Ganges', 2000.0, 500.0, 500.0, 'Operational', '2025-07-01'), -- Massive
('B-006', 'Mekong 1', 'Mekong', 1500.0, 300.0, 400.0, 'Operational', '2025-07-01'),
('B-007', 'Mekong 2', 'Mekong', 1400.0, 280.0, 400.0, 'Operational', '2025-07-01'),
('B-008', 'Yangtze Exit', 'Yangtze', 3000.0, 800.0, 1000.0, 'Maintenance', '2025-07-01'), -- Offline
('B-001', 'Citarum Mouth', 'Citarum', 600.0, 55.0, 130.0, 'Operational', '2025-07-03'),
('B-002', 'Klang Gate', 'Klang', 320.0, 25.0, 85.0, 'Operational', '2025-07-03'),
('B-003', 'Rio Motagua', 'Motagua', 1300.0, 220.0, 260.0, 'Operational', '2025-07-03'),
('B-009', 'Nile Delta', 'Nile', 400.0, 100.0, 300.0, 'Operational', '2025-07-01'),
('B-010', 'Amazon Mouth', 'Amazon', 100.0, 5000.0, 20000.0, 'Operational', '2025-07-01'), -- Mostly organic
('B-011', 'Chao Phraya', 'Chao Phraya', 900.0, 150.0, 200.0, 'Operational', '2025-07-01'),
('B-012', 'Han River', 'Han', 200.0, 50.0, 100.0, 'Operational', '2025-07-01'),
('B-013', 'Buriganga', 'Buriganga', 700.0, 120.0, 90.0, 'Operational', '2025-07-01'),
('B-014', 'Ciliwung', 'Ciliwung', 600.0, 80.0, 110.0, 'Operational', '2025-07-01'),
('B-015', 'Yamuna', 'Yamuna', 1800.0, 400.0, 300.0, 'Operational', '2025-07-01'),
('B-016', 'Yellow River', 'Huang He', 2500.0, 600.0, 800.0, 'Operational', '2025-07-01'),
('B-017', 'Pearl River', 'Pearl', 2200.0, 550.0, 700.0, 'Operational', '2025-07-01'),
('B-018', 'Indus', 'Indus', 1900.0, 450.0, 600.0, 'Operational', '2025-07-01'),
('B-019', 'Hai River', 'Hai', 1600.0, 350.0, 500.0, 'Operational', '2025-07-01'),
('B-020', 'Krishna', 'Krishna', 1000.0, 200.0, 250.0, 'Operational', '2025-07-01'),
('B-021', 'Godavari', 'Godavari', 900.0, 180.0, 240.0, 'Operational', '2025-07-01'),
('B-022', 'Amur', 'Amur', 300.0, 600.0, 900.0, 'Operational', '2025-07-01'),
('B-023', 'Lena', 'Lena', 100.0, 200.0, 800.0, 'Operational', '2025-07-01'),
('B-024', 'Niger', 'Niger', 800.0, 300.0, 500.0, 'Operational', '2025-07-01'),
('B-025', 'Volga', 'Volga', 400.0, 100.0, 300.0, 'Operational', '2025-07-01'),
('B-026', 'Danube', 'Danube', 300.0, 50.0, 400.0, 'Operational', '2025-07-01'),
('B-027', 'Rhine', 'Rhine', 200.0, 40.0, 350.0, 'Operational', '2025-07-01'),
('B-028', 'Thames', 'Thames', 50.0, 10.0, 50.0, 'Operational', '2025-07-01'),
('B-029', 'Seine', 'Seine', 60.0, 15.0, 60.0, 'Operational', '2025-07-01'),
('B-030', 'Tiber', 'Tiber', 80.0, 20.0, 40.0, 'Operational', '2025-07-01'),
('B-031', 'Po', 'Po', 100.0, 30.0, 100.0, 'Operational', '2025-07-01'),
('B-032', 'Ebro', 'Ebro', 90.0, 25.0, 80.0, 'Operational', '2025-07-01'),
('B-033', 'Douro', 'Douro', 70.0, 20.0, 70.0, 'Operational', '2025-07-01'),
('B-034', 'Tagus', 'Tagus', 75.0, 22.0, 75.0, 'Operational', '2025-07-01'),
('B-035', 'Guadalquivir', 'Guadalquivir', 65.0, 18.0, 60.0, 'Operational', '2025-07-01'),
('B-036', 'Garonne', 'Garonne', 55.0, 12.0, 50.0, 'Operational', '2025-07-01'),
('B-037', 'Loire', 'Loire', 60.0, 14.0, 55.0, 'Operational', '2025-07-01'),
('B-038', 'Elbe', 'Elbe', 150.0, 30.0, 150.0, 'Operational', '2025-07-01'),
('B-039', 'Oder', 'Oder', 140.0, 28.0, 140.0, 'Operational', '2025-07-01'),
('B-040', 'Vistula', 'Vistula', 160.0, 35.0, 160.0, 'Operational', '2025-07-01'),
('B-041', 'Dnieper', 'Dnieper', 200.0, 50.0, 200.0, 'Operational', '2025-07-01'),
('B-042', 'Don', 'Don', 180.0, 40.0, 180.0, 'Operational', '2025-07-01'),
('B-043', 'Ural', 'Ural', 120.0, 30.0, 120.0, 'Operational', '2025-07-01'),
('B-044', 'Kura', 'Kura', 100.0, 25.0, 100.0, 'Operational', '2025-07-01'),
('B-045', 'Tigris', 'Tigris', 500.0, 100.0, 300.0, 'Operational', '2025-07-01'),
('B-046', 'Euphrates', 'Euphrates', 450.0, 90.0, 280.0, 'Operational', '2025-07-01'),
('B-047', 'Indus', 'Indus', 1950.0, 460.0, 610.0, 'Operational', '2025-07-02'),
('B-048', 'Yangtze Exit', 'Yangtze', 3100.0, 820.0, 1020.0, 'Operational', '2025-07-02'), -- Back online
('B-049', 'Ganges Delta', 'Ganges', 2100.0, 520.0, 510.0, 'Operational', '2025-07-02'),
('B-050', 'Mekong 1', 'Mekong', 1550.0, 310.0, 410.0, 'Operational', '2025-07-02');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficiency Metrics
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW OceanCleanupDB.Operations.Silver.BarrierEfficiency AS
SELECT 
    BarrierID,
    RiverSystem,
    DisplayDate, -- Log date
    PlasticWeightKg,
    OrganicWeightKg,
    -- Calculate Plastic Ratio
    (PlasticWeightKg / (PlasticWeightKg + OrganicWeightKg)) * 100 AS PlasticPurityPct,
    FlowRateM3S,
    Status
FROM (
    SELECT 
        BarrierID,
        RiverSystem,
        CollectionDate AS DisplayDate,
        PlasticWeightKg,
        OrganicWeightKg,
        FlowRateM3S,
        Status
    FROM OceanCleanupDB.Operations.Bronze.DebrisCollection
) sub;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Impact Dashboard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW OceanCleanupDB.Operations.Gold.RiverImpact AS
SELECT 
    RiverSystem,
    SUM(PlasticWeightKg) AS TotalPlasticInterceptedKg,
    AVG(PlasticPurityPct) AS AvgPurityPct,
    COUNT(CASE WHEN Status = 'Maintenance' THEN 1 END) AS MaintenanceEvents,
    -- Projection: 1kg plastic = ~50 bottles prevented
    SUM(PlasticWeightKg) * 50 AS BottlesPreventedEst
FROM OceanCleanupDB.Operations.Silver.BarrierEfficiency
GROUP BY RiverSystem;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Find the river system with the highest 'TotalPlasticInterceptedKg' in the Gold dashboard."

PROMPT 2:
"List all collections from the Silver layer where the FlowRate exceeded 500 m3/s."

PROMPT 3:
"Calculate the total 'BottlesPreventedEst' across all rivers."
*/
