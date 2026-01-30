/*
 * PropTech: Smart Building HVAC Digital Twin
 * 
 * Scenario:
 * Optimizing HVAC energy usage in a skyscraper by correlating real-time room occupancy with temperature sensors.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SmartBuildingDB;
CREATE FOLDER IF NOT EXISTS SmartBuildingDB.Facilities;
CREATE FOLDER IF NOT EXISTS SmartBuildingDB.Facilities.Bronze;
CREATE FOLDER IF NOT EXISTS SmartBuildingDB.Facilities.Silver;
CREATE FOLDER IF NOT EXISTS SmartBuildingDB.Facilities.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: IoT Sensor Data
-------------------------------------------------------------------------------

-- RoomOccupancy Table
CREATE TABLE IF NOT EXISTS SmartBuildingDB.Facilities.Bronze.RoomOccupancy (
    SensorID VARCHAR,
    RoomID VARCHAR,
    FloorLevel INT,
    OccupantsDetected INT,
    ObservationTimestamp TIMESTAMP
);

INSERT INTO SmartBuildingDB.Facilities.Bronze.RoomOccupancy VALUES
('Occ-001', 'Conf-1A', 1, 0, '2025-06-15 09:00:00'),
('Occ-002', 'Banquet-1B', 1, 50, '2025-06-15 09:00:00'), -- Event
('Occ-003', 'Office-2A', 2, 5, '2025-06-15 09:00:00'),
('Occ-004', 'Office-2B', 2, 8, '2025-06-15 09:00:00'),
('Occ-005', 'Lobby-1', 1, 25, '2025-06-15 09:00:00'),
('Occ-006', 'Office-3A', 3, 0, '2025-06-15 09:00:00'), -- Empty
('Occ-007', 'Office-3B', 3, 2, '2025-06-15 09:00:00'),
('Occ-008', 'Conf-1A', 1, 10, '2025-06-15 10:00:00'), -- Meeting started
('Occ-009', 'Banquet-1B', 1, 150, '2025-06-15 10:00:00'), -- Full
('Occ-010', 'Office-2A', 2, 5, '2025-06-15 10:00:00'),
('Occ-011', 'Office-2B', 2, 8, '2025-06-15 10:00:00'),
('Occ-012', 'Conf-4A', 4, 0, '2025-06-15 09:00:00'),
('Occ-013', 'Conf-4B', 4, 0, '2025-06-15 09:00:00'),
('Occ-014', 'Office-5A', 5, 20, '2025-06-15 09:00:00'),
('Occ-015', 'Office-5B', 5, 22, '2025-06-15 09:00:00'),
('Occ-016', 'Cafeteria', 1, 5, '2025-06-15 09:00:00'),
('Occ-017', 'Gym', 1, 2, '2025-06-15 09:00:00'),
('Occ-018', 'Office-6A', 6, 0, '2025-06-15 09:00:00'),
('Occ-019', 'Office-6B', 6, 0, '2025-06-15 09:00:00'),
('Occ-020', 'Office-7A', 7, 30, '2025-06-15 09:00:00'),
('Occ-021', 'Office-7B', 7, 28, '2025-06-15 09:00:00'),
('Occ-022', 'ServerRoom', 0, 0, '2025-06-15 09:00:00'), -- Basement
('Occ-023', 'Office-8A', 8, 15, '2025-06-15 09:00:00'),
('Occ-024', 'Office-8B', 8, 12, '2025-06-15 09:00:00'),
('Occ-025', 'Conf-2C', 2, 0, '2025-06-15 09:00:00'),
('Occ-026', 'Conf-3C', 3, 0, '2025-06-15 09:00:00'),
('Occ-027', 'Cafeteria', 1, 100, '2025-06-15 12:00:00'), -- Lunch rush
('Occ-028', 'Gym', 1, 15, '2025-06-15 12:00:00'),
('Occ-029', 'Conf-1A', 1, 0, '2025-06-15 12:00:00'),
('Occ-030', 'Banquet-1B', 1, 0, '2025-06-15 12:00:00'),
('Occ-031', 'Office-9A', 9, 10, '2025-06-15 09:00:00'),
('Occ-032', 'Office-9B', 9, 8, '2025-06-15 09:00:00'),
('Occ-033', 'Office-10A', 10, 40, '2025-06-15 09:00:00'), -- Packed
('Occ-034', 'Office-10B', 10, 35, '2025-06-15 09:00:00'),
('Occ-035', 'Storage', 0, 1, '2025-06-15 09:00:00'),
('Occ-036', 'Office-2A', 2, 0, '2025-06-15 18:00:00'), -- End of day
('Occ-037', 'Office-2B', 2, 1, '2025-06-15 18:00:00'),
('Occ-038', 'Lobby-1', 1, 5, '2025-06-15 18:00:00'),
('Occ-039', 'Office-3A', 3, 0, '2025-06-15 18:00:00'),
('Occ-040', 'Office-3B', 3, 0, '2025-06-15 18:00:00'),
('Occ-041', 'Office-11A', 11, 20, '2025-06-15 09:00:00'),
('Occ-042', 'Office-11B', 11, 18, '2025-06-15 09:00:00'),
('Occ-043', 'Office-12A', 12, 5, '2025-06-15 09:00:00'),
('Occ-044', 'Office-12B', 12, 4, '2025-06-15 09:00:00'),
('Occ-045', 'RoofDeck', 13, 0, '2025-06-15 09:00:00'),
('Occ-046', 'RoofDeck', 13, 30, '2025-06-15 17:00:00'), -- Happy hour
('Occ-047', 'Office-5A', 5, 2, '2025-06-15 18:00:00'),
('Occ-048', 'Office-7A', 7, 5, '2025-06-15 18:00:00'),
('Occ-049', 'ServerRoom', 0, 0, '2025-06-15 18:00:00'),
('Occ-050', 'Gym', 1, 20, '2025-06-15 18:00:00');

-- HvacTelemetry Table
CREATE TABLE IF NOT EXISTS SmartBuildingDB.Facilities.Bronze.HvacTelemetry (
    UnitID VARCHAR,
    RoomID VARCHAR,
    TargetTemp DOUBLE,
    CurrentTemp DOUBLE,
    FanSpeedLevel INT, -- 0-100
    EnergyRateKw DOUBLE,
    TelemetryTimestamp TIMESTAMP
);

INSERT INTO SmartBuildingDB.Facilities.Bronze.HvacTelemetry VALUES
('HVAC-01', 'Conf-1A', 22.0, 21.0, 20, 1.5, '2025-06-15 09:00:00'), -- Cooling empty room?
('HVAC-02', 'Banquet-1B', 20.0, 24.0, 100, 5.0, '2025-06-15 09:00:00'), -- Working hard
('HVAC-03', 'Office-2A', 22.0, 22.0, 30, 2.0, '2025-06-15 09:00:00'),
('HVAC-04', 'Office-2B', 22.0, 22.5, 30, 2.0, '2025-06-15 09:00:00'),
('HVAC-05', 'Lobby-1', 23.0, 23.0, 50, 3.0, '2025-06-15 09:00:00'),
('HVAC-06', 'Office-3A', 22.0, 21.0, 30, 2.0, '2025-06-15 09:00:00'), -- Cooling empty room
('HVAC-07', 'Office-3B', 22.0, 22.0, 20, 1.5, '2025-06-15 09:00:00'),
('HVAC-08', 'Banquet-1B', 20.0, 26.0, 100, 5.2, '2025-06-15 10:00:00'), -- Struggling
('HVAC-09', 'Conf-4A', 22.0, 22.0, 25, 2.0, '2025-06-15 09:00:00'), -- Empty but running
('HVAC-10', 'Office-5A', 22.0, 23.0, 60, 3.5, '2025-06-15 09:00:00'),
('HVAC-11', 'Office-5B', 22.0, 23.5, 65, 3.6, '2025-06-15 09:00:00'),
('HVAC-12', 'ServerRoom', 18.0, 18.5, 80, 4.5, '2025-06-15 09:00:00'), -- Critical
('HVAC-13', 'Office-6A', 22.0, 21.0, 20, 1.5, '2025-06-15 09:00:00'), -- Waste
('HVAC-14', 'Office-6B', 22.0, 21.0, 20, 1.5, '2025-06-15 09:00:00'), -- Waste
('HVAC-15', 'Office-7A', 22.0, 24.0, 80, 4.0, '2025-06-15 09:00:00'),
('HVAC-16', 'Office-7B', 22.0, 24.5, 85, 4.2, '2025-06-15 09:00:00'),
('HVAC-17', 'Gym', 20.0, 21.0, 40, 2.5, '2025-06-15 09:00:00'),
('HVAC-18', 'Cafeteria', 22.0, 25.0, 90, 4.8, '2025-06-15 12:00:00'), -- Heat load from people
('HVAC-19', 'Office-10A', 22.0, 25.0, 90, 4.5, '2025-06-15 09:00:00'),
('HVAC-20', 'Office-10B', 22.0, 25.0, 90, 4.5, '2025-06-15 09:00:00'),
('HVAC-21', 'Conf-1A', 22.0, 22.0, 30, 2.0, '2025-06-15 10:00:00'),
('HVAC-22', 'Office-11A', 22.0, 22.5, 50, 3.0, '2025-06-15 09:00:00'),
('HVAC-23', 'Office-11B', 22.0, 22.5, 50, 3.0, '2025-06-15 09:00:00'),
('HVAC-24', 'Office-12A', 22.0, 21.5, 25, 1.8, '2025-06-15 09:00:00'),
('HVAC-25', 'Office-12B', 22.0, 21.5, 25, 1.8, '2025-06-15 09:00:00'),
('HVAC-26', 'RoofDeck', 25.0, 28.0, 0, 0.5, '2025-06-15 09:00:00'), -- Off
('HVAC-27', 'RoofDeck', 24.0, 26.0, 100, 6.0, '2025-06-15 17:00:00'), -- Outdoor cooling? Waste/Lux?
('HVAC-28', 'Office-2A', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'), -- Mode adjustment?
('HVAC-29', 'Office-2B', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-30', 'Lobby-1', 23.0, 23.0, 30, 2.0, '2025-06-15 18:00:00'),
('HVAC-31', 'Office-3A', 22.0, 21.0, 20, 1.5, '2025-06-15 18:00:00'), -- Still cooling empty room
('HVAC-32', 'Office-3B', 22.0, 21.0, 20, 1.5, '2025-06-15 18:00:00'),
('HVAC-33', 'Office-9A', 22.0, 22.0, 30, 2.0, '2025-06-15 09:00:00'),
('HVAC-34', 'Office-9B', 22.0, 22.0, 30, 2.0, '2025-06-15 09:00:00'),
('HVAC-35', 'Storage', 24.0, 23.0, 10, 0.5, '2025-06-15 09:00:00'),
('HVAC-36', 'Conf-2C', 22.0, 21.0, 25, 2.0, '2025-06-15 09:00:00'), -- Empty waste
('HVAC-37', 'Conf-3C', 22.0, 21.0, 25, 2.0, '2025-06-15 09:00:00'), -- Empty waste
('HVAC-38', 'Office-8A', 22.0, 23.0, 40, 2.5, '2025-06-15 09:00:00'),
('HVAC-39', 'Office-8B', 22.0, 23.0, 40, 2.5, '2025-06-15 09:00:00'),
('HVAC-40', 'Office-5A', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-41', 'Office-7A', 24.0, 23.0, 20, 1.0, '2025-06-15 18:00:00'),
('HVAC-42', 'ServerRoom', 18.0, 18.5, 80, 4.5, '2025-06-15 18:00:00'), -- Constant load
('HVAC-43', 'Gym', 20.0, 21.0, 50, 3.0, '2025-06-15 18:00:00'),
('HVAC-44', 'Conf-4B', 22.0, 21.0, 25, 2.0, '2025-06-15 09:00:00'), -- Empty waste
('HVAC-45', 'Office-6A', 24.0, 22.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-46', 'Office-6B', 24.0, 22.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-47', 'Cafeteria', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-48', 'Banquet-1B', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-49', 'Conf-1A', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00'),
('HVAC-50', 'Office-9A', 24.0, 23.0, 10, 0.5, '2025-06-15 18:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Efficiency Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SmartBuildingDB.Facilities.Silver.EnergyEfficiency AS
SELECT 
    h.UnitID,
    h.RoomID,
    h.EnergyRateKw,
    o.OccupantsDetected,
    h.CurrentTemp,
    h.TargetTemp,
    -- Detection Logic: High Energy (>1.5kW) used in Empty Room (0 occupants)
    CASE 
        WHEN o.OccupantsDetected = 0 AND h.EnergyRateKw > 1.5 THEN 'Wasted Energy'
        WHEN o.OccupantsDetected > 20 AND h.CurrentTemp > h.TargetTemp + 2 THEN 'Overloaded'
        ELSE 'Optimized'
    END AS EfficiencyStatus,
    h.TelemetryTimestamp AS EventTimestamp
FROM SmartBuildingDB.Facilities.Bronze.HvacTelemetry h
JOIN SmartBuildingDB.Facilities.Bronze.RoomOccupancy o 
    ON h.RoomID = o.RoomID 
    AND h.TelemetryTimestamp = o.ObservationTimestamp;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Waste Report
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SmartBuildingDB.Facilities.Gold.WasteReduction AS
SELECT 
    RoomID,
    SUM(EnergyRateKw) AS TotalKwBurn,
    COUNT(*) AS WasteEvents,
    -- Assuming 1 hour intervals for example simplicity
    SUM(EnergyRateKw) * 0.15 AS EstimatedCostSavingsPerHr -- $0.15/kWh
FROM SmartBuildingDB.Facilities.Silver.EnergyEfficiency
WHERE EfficiencyStatus = 'Wasted Energy'
GROUP BY RoomID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify the top 3 rooms with the highest accumulated 'EstimatedCostSavingsPerHr' in the Gold waste report."

PROMPT 2:
"List all HVAC units currently flagged as 'Overloaded' in the Silver layer, including their current temperature."

PROMPT 3:
"Show the trend of EnergyRateKw for 'Banquet-1B' throughout the day."
*/
