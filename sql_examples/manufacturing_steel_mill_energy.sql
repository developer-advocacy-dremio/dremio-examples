/*
    Dremio High-Volume SQL Pattern: Steel Mill Energy Consumption
    
    Business Scenario:
    Steel production via Electric Arc Furnaces (EAF) is energy-intensive.
    We need to track kWh consumed per ton of steel produced to optimize
    costs and reduce carbon footprint.
    
    Data Story:
    - Bronze: HeatLogs (Per Heat/Batch), EnergyMeterReadings.
    - Silver: HeatEfficiency (kWh/Ton).
    - Gold: ShiftPerformance (Aggregated efficiency).
    
    Medallion Architecture:
    - Bronze: HeatLogs, EnergyMeterReadings.
      *Volume*: 50+ records.
    - Silver: HeatEfficiency.
    - Gold: ShiftPerformance.
    
    Key Dremio Features:
    - Window Functions (Lead/Lag can be simulated via Self-Joins or Time Aggregation)
    - SUM aggregations
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;
USE ManufacturingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.HeatLogs (
    HeatID STRING,
    FurnaceID STRING,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    SteelOutput_Tons DOUBLE,
    Grade STRING
);

INSERT INTO ManufacturingDB.Bronze.HeatLogs VALUES
('H001', 'EAF-1', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 08:50:00', 100.0, '304SS'),
('H002', 'EAF-1', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:55:00', 102.0, '304SS'),
('H003', 'EAF-1', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:45:00', 98.0, '316L'),
('H004', 'EAF-1', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 11:50:00', 101.0, '304SS'),
('H005', 'EAF-1', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:55:00', 105.0, 'Carbon'),
('H006', 'EAF-2', TIMESTAMP '2025-01-20 08:15:00', TIMESTAMP '2025-01-20 09:05:00', 110.0, 'Carbon'),
('H007', 'EAF-2', TIMESTAMP '2025-01-20 09:15:00', TIMESTAMP '2025-01-20 10:10:00', 108.0, 'Carbon'),
('H008', 'EAF-2', TIMESTAMP '2025-01-20 10:20:00', TIMESTAMP '2025-01-20 11:15:00', 112.0, 'Carbon'),
('H009', 'EAF-2', TIMESTAMP '2025-01-20 11:25:00', TIMESTAMP '2025-01-20 12:20:00', 109.0, '304SS'),
('H010', 'EAF-2', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 13:25:00', 111.0, '316L'),
('H011', 'EAF-1', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:50:00', 100.0, '304SS'),
('H012', 'EAF-1', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 14:55:00', 102.0, '304SS'),
('H013', 'EAF-1', TIMESTAMP '2025-01-20 15:00:00', TIMESTAMP '2025-01-20 15:45:00', 98.0, '316L'),
('H014', 'EAF-1', TIMESTAMP '2025-01-20 16:00:00', TIMESTAMP '2025-01-20 16:50:00', 101.0, '304SS'),
('H015', 'EAF-1', TIMESTAMP '2025-01-20 17:00:00', TIMESTAMP '2025-01-20 17:55:00', 105.0, 'Carbon'),
('H016', 'EAF-2', TIMESTAMP '2025-01-20 13:30:00', TIMESTAMP '2025-01-20 14:20:00', 110.0, 'Carbon'),
('H017', 'EAF-2', TIMESTAMP '2025-01-20 14:30:00', TIMESTAMP '2025-01-20 15:25:00', 108.0, 'Carbon'),
('H018', 'EAF-2', TIMESTAMP '2025-01-20 15:35:00', TIMESTAMP '2025-01-20 16:30:00', 112.0, 'Carbon'),
('H019', 'EAF-2', TIMESTAMP '2025-01-20 16:40:00', TIMESTAMP '2025-01-20 17:35:00', 109.0, '304SS'),
('H020', 'EAF-2', TIMESTAMP '2025-01-20 17:45:00', TIMESTAMP '2025-01-20 18:40:00', 111.0, '316L'),
('H021', 'EAF-1', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 18:50:00', 100.0, '304SS'),
('H022', 'EAF-1', TIMESTAMP '2025-01-20 19:00:00', TIMESTAMP '2025-01-20 19:55:00', 102.0, '304SS'),
('H023', 'EAF-1', TIMESTAMP '2025-01-20 20:00:00', TIMESTAMP '2025-01-20 20:45:00', 98.0, '316L'),
('H024', 'EAF-1', TIMESTAMP '2025-01-20 21:00:00', TIMESTAMP '2025-01-20 21:50:00', 101.0, '304SS'),
('H025', 'EAF-1', TIMESTAMP '2025-01-20 22:00:00', TIMESTAMP '2025-01-20 22:55:00', 105.0, 'Carbon');

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.EnergyMeterReadings (
    MeterID STRING, -- M-EAF-1, M-EAF-2
    ReadingTime TIMESTAMP,
    Cumulative_kWh DOUBLE
);

-- Simulating readings at start/end of heats (simplified join logic)
INSERT INTO ManufacturingDB.Bronze.EnergyMeterReadings VALUES
('M-EAF-1', TIMESTAMP '2025-01-20 08:00:00', 10000.0), -- Start H1
('M-EAF-1', TIMESTAMP '2025-01-20 08:50:00', 15000.0), -- End H1 (+5000)
('M-EAF-1', TIMESTAMP '2025-01-20 09:00:00', 15000.0),
('M-EAF-1', TIMESTAMP '2025-01-20 09:55:00', 20500.0), -- +5500
('M-EAF-1', TIMESTAMP '2025-01-20 10:00:00', 20500.0),
('M-EAF-1', TIMESTAMP '2025-01-20 10:45:00', 25000.0), -- +4500
('M-EAF-1', TIMESTAMP '2025-01-20 11:00:00', 25000.0),
('M-EAF-1', TIMESTAMP '2025-01-20 11:50:00', 30100.0), -- +5100
('M-EAF-1', TIMESTAMP '2025-01-20 12:00:00', 30100.0),
('M-EAF-1', TIMESTAMP '2025-01-20 12:55:00', 35500.0), -- +5400
('M-EAF-2', TIMESTAMP '2025-01-20 08:15:00', 50000.0),
('M-EAF-2', TIMESTAMP '2025-01-20 09:05:00', 56000.0), -- +6000
('M-EAF-2', TIMESTAMP '2025-01-20 09:15:00', 56000.0),
('M-EAF-2', TIMESTAMP '2025-01-20 10:10:00', 62000.0), -- +6000
('M-EAF-2', TIMESTAMP '2025-01-20 10:20:00', 62000.0),
('M-EAF-2', TIMESTAMP '2025-01-20 11:15:00', 68500.0), -- +6500
('M-EAF-2', TIMESTAMP '2025-01-20 11:25:00', 68500.0),
('M-EAF-2', TIMESTAMP '2025-01-20 12:20:00', 74000.0), -- +5500
('M-EAF-2', TIMESTAMP '2025-01-20 12:30:00', 74000.0),
('M-EAF-2', TIMESTAMP '2025-01-20 13:25:00', 80000.0), -- +6000
('M-EAF-1', TIMESTAMP '2025-01-20 13:00:00', 35500.0),
('M-EAF-1', TIMESTAMP '2025-01-20 13:50:00', 40500.0), -- +5000
('M-EAF-1', TIMESTAMP '2025-01-20 14:00:00', 40500.0),
('M-EAF-1', TIMESTAMP '2025-01-20 14:55:00', 45700.0), -- +5200
('M-EAF-1', TIMESTAMP '2025-01-20 15:00:00', 45700.0),
('M-EAF-1', TIMESTAMP '2025-01-20 15:45:00', 50200.0); -- +4500

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Heat Efficiency
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Silver.HeatEfficiency AS
SELECT
    h.HeatID,
    h.FurnaceID,
    h.Grade,
    h.SteelOutput_Tons,
    e_start.Cumulative_kWh AS StartkWh,
    e_end.Cumulative_kWh AS EndkWh,
    (e_end.Cumulative_kWh - e_start.Cumulative_kWh) AS Consumed_kWh,
    (e_end.Cumulative_kWh - e_start.Cumulative_kWh) / h.SteelOutput_Tons AS kWh_Per_Ton
FROM ManufacturingDB.Bronze.HeatLogs h
JOIN ManufacturingDB.Bronze.EnergyMeterReadings e_start 
    ON h.FurnaceID = REPLACE(e_start.MeterID, 'M-', '') 
    AND h.StartTime = e_start.ReadingTime
JOIN ManufacturingDB.Bronze.EnergyMeterReadings e_end
    ON h.FurnaceID = REPLACE(e_end.MeterID, 'M-', '')
    AND h.EndTime = e_end.ReadingTime;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Shift Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.ShiftPerformance AS
SELECT
    FurnaceID,
    Grade,
    COUNT(*) AS HeatsRun,
    AVG(kWh_Per_Ton) AS AvgEnergyIntensity,
    SUM(SteelOutput_Tons) AS TotalOutput
FROM ManufacturingDB.Silver.HeatEfficiency
GROUP BY FurnaceID, Grade;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Steel Grade consumes the most energy per ton?"
    2. "Compare EAF-1 vs EAF-2 average energy intensity."
    3. "Calculate total tons produced by furnace."
*/
