/*
 * Logistics: Drone Delivery Airspace Management
 * 
 * Scenario:
 * Deconflicting low-altitude flight paths and monitoring battery reserves for autonomous delivery drones.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AeroLogisticsDB;
CREATE FOLDER IF NOT EXISTS AeroLogisticsDB.Airspace;
CREATE FOLDER IF NOT EXISTS AeroLogisticsDB.Airspace.Bronze;
CREATE FOLDER IF NOT EXISTS AeroLogisticsDB.Airspace.Silver;
CREATE FOLDER IF NOT EXISTS AeroLogisticsDB.Airspace.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Drone Telemetry
-------------------------------------------------------------------------------

-- FlightTelemetry Table
CREATE TABLE IF NOT EXISTS AeroLogisticsDB.Airspace.Bronze.FlightTelemetry (
    DroneID VARCHAR,
    CorridorID VARCHAR, -- N-S-01, E-W-02
    AltitudeMeters DOUBLE,
    BatteryPct DOUBLE,
    Status VARCHAR, -- In-Flight, Hovering, Landed
    SnapshotTimestamp TIMESTAMP
);

INSERT INTO AeroLogisticsDB.Airspace.Bronze.FlightTelemetry VALUES
('D-001', 'N-S-01', 50.0, 80.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-002', 'N-S-01', 55.0, 75.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-003', 'E-W-02', 45.0, 10.0, 'In-Flight', '2025-09-01 14:00:00'), -- Critical Battery
('D-004', 'E-W-02', 60.0, 90.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-005', 'N-S-01', 50.0, 85.0, 'Hovering', '2025-09-01 14:00:00'), -- Collision risk with D-001 at same alt
('D-006', 'N-S-02', 40.0, 50.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-007', 'N-S-02', 80.0, 60.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-008', 'E-W-01', 50.0, 40.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-009', 'E-W-01', 50.0, 30.0, 'Hovering', '2025-09-01 14:00:00'),
('D-010', 'N-S-03', 30.0, 95.0, 'Landed', '2025-09-01 14:00:00'),
('D-011', 'N-S-01', 52.0, 78.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-012', 'N-S-01', 120.0, 80.0, 'In-Flight', '2025-09-01 14:00:00'), -- Violation > 100m
('D-013', 'E-W-03', 45.0, 8.0, 'In-Flight', '2025-09-01 14:00:00'), -- Critical
('D-014', 'E-W-03', 55.0, 70.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-015', 'N-S-04', 60.0, 60.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-016', 'N-S-04', 60.0, 55.0, 'In-Flight', '2025-09-01 14:00:00'), -- Conflict
('D-017', 'E-W-04', 70.0, 90.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-018', 'E-W-04', 75.0, 85.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-019', 'N-S-05', 40.0, 20.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-020', 'N-S-05', 42.0, 22.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-021', 'E-W-05', 50.0, 80.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-022', 'E-W-05', 50.0, 75.0, 'In-Flight', '2025-09-01 14:00:00'), -- Conflict
('D-023', 'N-S-01', 50.0, 80.0, 'In-Flight', '2025-09-01 14:10:00'),
('D-024', 'N-S-01', 110.0, 70.0, 'In-Flight', '2025-09-01 14:00:00'), -- Violation
('D-025', 'N-S-02', 30.0, 90.0, 'Landed', '2025-09-01 14:00:00'),
('D-026', 'E-W-02', 45.0, 15.0, 'In-Flight', '2025-09-01 14:05:00'), -- Low
('D-027', 'E-W-02', 60.0, 85.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-028', 'N-S-03', 50.0, 88.0, 'In-Flight', '2025-09-01 14:00:00'),
('D-029', 'N-S-03', 50.0, 87.0, 'In-Flight', '2025-09-01 14:00:00'), -- Conflict
('D-030', 'E-W-03', 30.0, 5.0, 'In-Flight', '2025-09-01 14:05:00'), -- Critical
('D-031', 'E-W-03', 55.0, 65.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-032', 'N-S-04', 60.0, 50.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-033', 'N-S-04', 62.0, 48.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-034', 'E-W-04', 70.0, 85.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-035', 'E-W-04', 72.0, 80.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-036', 'N-S-05', 40.0, 18.0, 'In-Flight', '2025-09-01 14:05:00'), -- Low
('D-037', 'N-S-05', 40.0, 20.0, 'In-Flight', '2025-09-01 14:05:00'), -- Conflict & Low
('D-038', 'E-W-05', 50.0, 75.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-039', 'E-W-05', 52.0, 70.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-040', 'N-S-01', 50.0, 75.0, 'In-Flight', '2025-09-01 14:15:00'),
('D-041', 'N-S-01', 55.0, 70.0, 'In-Flight', '2025-09-01 14:15:00'),
('D-042', 'E-W-01', 50.0, 35.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-043', 'E-W-01', 105.0, 40.0, 'In-Flight', '2025-09-01 14:05:00'), -- Violation
('D-044', 'N-S-02', 40.0, 45.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-045', 'N-S-02', 80.0, 55.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-046', 'E-W-02', 45.0, 12.0, 'In-Flight', '2025-09-01 14:10:00'), -- Low
('D-047', 'E-W-02', 60.0, 80.0, 'In-Flight', '2025-09-01 14:10:00'),
('D-048', 'N-S-03', 50.0, 85.0, 'In-Flight', '2025-09-01 14:05:00'),
('D-049', 'N-S-03', 50.0, 84.0, 'In-Flight', '2025-09-01 14:05:00'), -- Conflict
('D-050', 'E-W-03', 30.0, 4.0, 'Hovering', '2025-09-01 14:10:00'); -- Crit landing

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Airspace Hazards
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AeroLogisticsDB.Airspace.Silver.FlightStatus AS
SELECT 
    DroneID,
    CorridorID,
    AltitudeMeters,
    BatteryPct,
    SnapshotTimestamp,
    CASE 
        WHEN AltitudeMeters > 100 THEN 'Altitude Violation'
        WHEN BatteryPct < 15.0 THEN 'Low Battery Warning'
        ELSE 'Normal'
    END AS FlightCondition
FROM AeroLogisticsDB.Airspace.Bronze.FlightTelemetry
WHERE Status IN ('In-Flight', 'Hovering');

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Conflict Map
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AeroLogisticsDB.Airspace.Gold.ConflictAlerts AS
SELECT 
    CorridorID,
    SnapshotTimestamp,
    COUNT(DroneID) AS DroneCount,
    -- Conflict detection simplified: Multiple drones in same corridor at similar time
    -- Real scenario would check altitude separation delta < 5m
    CASE 
        WHEN COUNT(DroneID) > 2 THEN 'Congestion Risk' 
        ELSE 'Clear' 
    END AS CorridorStatus,
    SUM(CASE WHEN FlightCondition = 'Low Battery Warning' THEN 1 ELSE 0 END) AS EmergencyLandingReqs
FROM AeroLogisticsDB.Airspace.Silver.FlightStatus
GROUP BY CorridorID, SnapshotTimestamp;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Find all drones in the Silver layer currently flagged with an 'Altitude Violation'."

PROMPT 2:
"Identify corridors with 'Congestion Risk' from the Gold layer."

PROMPT 3:
"List drones with less than 15% battery remaining in the 'E-W-03' corridor."
*/
