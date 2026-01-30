/*
 * Sustainability: EV Battery Recycling
 * 
 * Scenario:
 * Tracking battery State of Health (SOH) to determine if batteries should be repurposed (Second Life) or shredded (Material Recovery).
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CircularEconDB;
CREATE FOLDER IF NOT EXISTS CircularEconDB.Recycling;
CREATE FOLDER IF NOT EXISTS CircularEconDB.Recycling.Bronze;
CREATE FOLDER IF NOT EXISTS CircularEconDB.Recycling.Silver;
CREATE FOLDER IF NOT EXISTS CircularEconDB.Recycling.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Battery Ingest
-------------------------------------------------------------------------------

-- BatteryPacks Table
CREATE TABLE IF NOT EXISTS CircularEconDB.Recycling.Bronze.BatteryPacks (
    PackID VARCHAR,
    OEM VARCHAR, -- Tesla, Ford, VW
    ModelYear INT,
    Chemistry VARCHAR, -- NMC, LFP, NCA
    IncomingWeightKg DOUBLE
);

INSERT INTO CircularEconDB.Recycling.Bronze.BatteryPacks VALUES
('P-001', 'Tesla', 2018, 'NCA', 450.0),
('P-002', 'Tesla', 2018, 'NCA', 450.0),
('P-003', 'Ford', 2020, 'NMC', 500.0),
('P-004', 'VW', 2019, 'NMC', 480.0),
('P-005', 'Nissan', 2015, 'LMO', 300.0),
('P-006', 'Tesla', 2021, 'LFP', 400.0),
('P-007', 'Ford', 2021, 'NMC', 500.0),
('P-008', 'VW', 2020, 'NMC', 480.0),
('P-009', 'Chevy', 2017, 'NMC', 420.0),
('P-010', 'Nissan', 2016, 'LMO', 300.0),
('P-011', 'Tesla', 2019, 'NCA', 450.0),
('P-012', 'Tesla', 2019, 'NCA', 450.0),
('P-013', 'Tesla', 2019, 'NCA', 450.0),
('P-014', 'Ford', 2022, 'NMC', 510.0),
('P-015', 'BMW', 2018, 'NMC', 250.0),
('P-016', 'BMW', 2019, 'NMC', 250.0),
('P-017', 'Audi', 2020, 'NMC', 600.0),
('P-018', 'Audi', 2020, 'NMC', 600.0),
('P-019', 'Porsche', 2021, 'NMC', 550.0),
('P-020', 'Tesla', 2014, 'NCA', 450.0),
('P-021', 'Tesla', 2015, 'NCA', 450.0),
('P-022', 'Tesla', 2016, 'NCA', 450.0),
('P-023', 'Toyota', 2022, 'LFP', 400.0),
('P-024', 'Toyota', 2023, 'LFP', 400.0),
('P-025', 'BYD', 2023, 'LFP', 380.0),
('P-026', 'BYD', 2023, 'LFP', 380.0),
('P-027', 'Polestar', 2022, 'NMC', 490.0),
('P-028', 'Polestar', 2022, 'NMC', 490.0),
('P-029', 'Rivian', 2022, 'NCA', 650.0),
('P-030', 'Rivian', 2023, 'NCA', 650.0),
('P-031', 'Lucid', 2022, 'NCA', 500.0),
('P-032', 'Tesla', 2012, 'NCA', 450.0),
('P-033', 'Tesla', 2013, 'NCA', 450.0),
('P-034', 'Nissan', 2012, 'LMO', 300.0),
('P-035', 'Nissan', 2013, 'LMO', 300.0),
('P-036', 'Chevy', 2020, 'NMC', 430.0),
('P-037', 'Chevy', 2021, 'NMC', 430.0),
('P-038', 'Hyundai', 2020, 'NMC', 440.0),
('P-039', 'Hyundai', 2021, 'NMC', 440.0),
('P-040', 'Kia', 2020, 'NMC', 440.0),
('P-041', 'Kia', 2021, 'NMC', 440.0),
('P-042', 'Mercedes', 2022, 'NMC', 580.0),
('P-043', 'Mercedes', 2023, 'NMC', 580.0),
('P-044', 'Jaguar', 2020, 'NMC', 520.0),
('P-045', 'Jaguar', 2021, 'NMC', 520.0),
('P-046', 'Volvo', 2021, 'NMC', 460.0),
('P-047', 'Volvo', 2022, 'NMC', 460.0),
('P-048', 'Honda', 2024, 'LFP', 390.0),
('P-049', 'Subaru', 2023, 'NMC', 450.0),
('P-050', 'Mazda', 2022, 'LFP', 350.0);

-- HealthTests Table
CREATE TABLE IF NOT EXISTS CircularEconDB.Recycling.Bronze.HealthTests (
    TestID INT,
    PackID VARCHAR,
    RemainingCapacityKWh DOUBLE,
    OriginalCapacityKWh DOUBLE,
    InternalResistanceMOhms DOUBLE,
    SafetyFlag BOOLEAN
);

INSERT INTO CircularEconDB.Recycling.Bronze.HealthTests VALUES
(1, 'P-001', 65.0, 75.0, 20.0, false),
(2, 'P-002', 60.0, 75.0, 25.0, false),
(3, 'P-003', 88.0, 98.0, 15.0, false),
(4, 'P-004', 50.0, 82.0, 40.0, false),
(5, 'P-005', 15.0, 24.0, 80.0, false), -- Heavily degraded
(6, 'P-006', 58.0, 60.0, 10.0, false), -- Almost new
(7, 'P-007', 90.0, 98.0, 12.0, false),
(8, 'P-008', 75.0, 82.0, 18.0, false),
(9, 'P-009', 40.0, 60.0, 35.0, false),
(10, 'P-010', 18.0, 24.0, 60.0, true), -- Safety risk
(11, 'P-011', 70.0, 75.0, 22.0, false),
(12, 'P-012', 45.0, 75.0, 50.0, false),
(13, 'P-013', 30.0, 75.0, 60.0, true), -- Damaged
(14, 'P-014', 95.0, 98.0, 10.0, false),
(15, 'P-015', 30.0, 42.0, 25.0, false),
(16, 'P-016', 38.0, 42.0, 15.0, false),
(17, 'P-017', 85.0, 95.0, 18.0, false),
(18, 'P-018', 40.0, 95.0, 90.0, true), -- Crash damaged?
(19, 'P-019', 80.0, 93.0, 14.0, false),
(20, 'P-020', 20.0, 85.0, 100.0, false), -- Old Model S
(21, 'P-021', 40.0, 85.0, 50.0, false),
(22, 'P-022', 75.0, 85.0, 20.0, false), -- Replacement pack?
(23, 'P-023', 70.0, 75.0, 12.0, false),
(24, 'P-024', 74.0, 75.0, 10.0, false),
(25, 'P-025', 68.0, 70.0, 11.0, false),
(26, 'P-026', 60.0, 70.0, 15.0, false),
(27, 'P-027', 72.0, 78.0, 16.0, false),
(28, 'P-028', 50.0, 78.0, 40.0, false),
(29, 'P-029', 125.0, 135.0, 10.0, false),
(30, 'P-030', 130.0, 135.0, 8.0, false),
(31, 'P-031', 110.0, 118.0, 12.0, false),
(32, 'P-032', 10.0, 85.0, 150.0, true), -- Dead
(33, 'P-033', 30.0, 85.0, 80.0, false),
(34, 'P-034', 12.0, 24.0, 90.0, false),
(35, 'P-035', 14.0, 24.0, 85.0, false),
(36, 'P-036', 55.0, 66.0, 20.0, false),
(37, 'P-037', 60.0, 66.0, 15.0, false),
(38, 'P-038', 58.0, 64.0, 18.0, false),
(39, 'P-039', 60.0, 64.0, 14.0, false),
(40, 'P-040', 57.0, 64.0, 19.0, false),
(41, 'P-041', 61.0, 64.0, 12.0, false),
(42, 'P-042', 90.0, 108.0, 15.0, false),
(43, 'P-043', 100.0, 108.0, 10.0, false),
(44, 'P-044', 80.0, 90.0, 22.0, false),
(45, 'P-045', 85.0, 90.0, 18.0, false),
(46, 'P-046', 70.0, 78.0, 20.0, false),
(47, 'P-047', 72.0, 78.0, 15.0, false),
(48, 'P-048', 60.0, 60.0, 5.0, false), -- Brand new rejected?
(49, 'P-049', 65.0, 75.0, 18.0, false),
(50, 'P-050', 50.0, 60.0, 25.0, false);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: State of Health (SOH)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CircularEconDB.Recycling.Silver.BatteryStatus AS
SELECT 
    b.PackID,
    b.OEM,
    b.Chemistry,
    t.RemainingCapacityKWh,
    t.OriginalCapacityKWh,
    (t.RemainingCapacityKWh / t.OriginalCapacityKWh) * 100 AS SOH_Percentage,
    t.SafetyFlag,
    CASE 
        WHEN t.SafetyFlag = true THEN 'Hazardous Waste Process'
        WHEN (t.RemainingCapacityKWh / t.OriginalCapacityKWh) * 100 > 80.0 THEN 'Second Life (Storage)'
        ELSE 'Material Recovery (Shred)'
    END AS Disposition
FROM CircularEconDB.Recycling.Bronze.BatteryPacks b
JOIN CircularEconDB.Recycling.Bronze.HealthTests t ON b.PackID = t.PackID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Recovery Projections
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CircularEconDB.Recycling.Gold.MaterialYield AS
SELECT 
    Disposition,
    COUNT(PackID) AS PackCount,
    SUM(CASE WHEN Chemistry = 'NMC' THEN 1 ELSE 0 END) AS NMC_Count,
    SUM(CASE WHEN Chemistry = 'LFP' THEN 1 ELSE 0 END) AS LFP_Count,
    -- Est. Lithium Recovery: ~1.5% of pack weight for NMC
    -- This is a simplified projection for the example
    SUM(CASE WHEN Disposition = 'Material Recovery (Shred)' THEN 0.015 * 450 ELSE 0 END) AS EstLithiumYieldKg
FROM CircularEconDB.Recycling.Silver.BatteryStatus
GROUP BY Disposition;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all battery packs marked for 'Second Life (Storage)' from the CircularEconDB.Recycling.Silver.BatteryStatus view."

PROMPT 2:
"Identify any packs with a SafetyFlag set to true and show their OEM."

PROMPT 3:
"Calculate the total estimated Lithium yield for 'NMC' chemistry packs designated for shredding."
*/
