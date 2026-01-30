/*
 * Aerospace: Orbital Debris Tracking
 * 
 * Scenario:
 * Tracking space junk trajectories and calculating collision probabilities with active satellites.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SpaceTrafficDB;
CREATE FOLDER IF NOT EXISTS SpaceTrafficDB.Orbit;
CREATE FOLDER IF NOT EXISTS SpaceTrafficDB.Orbit.Bronze;
CREATE FOLDER IF NOT EXISTS SpaceTrafficDB.Orbit.Silver;
CREATE FOLDER IF NOT EXISTS SpaceTrafficDB.Orbit.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Radar Telemetry
-------------------------------------------------------------------------------

-- ObjectTelemetry Table
CREATE TABLE IF NOT EXISTS SpaceTrafficDB.Orbit.Bronze.ObjectTelemetry (
    ObjectID VARCHAR,
    ObjectType VARCHAR, -- Satellite, Debris, SpentStage
    AltitudeMinKM DOUBLE,
    AltitudeMaxKM DOUBLE,
    InclinationDeg DOUBLE,
    VelocityKPS DOUBLE, -- km/s
    LastSeenTimestamp TIMESTAMP
);

INSERT INTO SpaceTrafficDB.Orbit.Bronze.ObjectTelemetry VALUES
('SAT-001', 'Satellite', 500.0, 510.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-101', 'Debris', 500.0, 505.0, 53.0, 7.6, '2026-08-01 12:00:00'), -- Collision path
('SAT-002', 'Satellite', 800.0, 810.0, 98.0, 7.4, '2026-08-01 12:00:00'),
('DEB-102', 'Debris', 400.0, 410.0, 45.0, 7.7, '2026-08-01 12:00:00'),
('STG-201', 'SpentStage', 300.0, 350.0, 20.0, 7.8, '2026-08-01 12:00:00'),
('SAT-003', 'Satellite', 1200.0, 1220.0, 60.0, 7.2, '2026-08-01 12:00:00'),
('DEB-103', 'Debris', 1200.0, 1205.0, 60.1, 7.2, '2026-08-01 12:00:00'), -- Close
('SAT-004', 'Satellite', 550.0, 560.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-104', 'Debris', 550.0, 550.0, 90.0, 7.6, '2026-08-01 12:00:00'), -- Crossing orbit
('SAT-005', 'Satellite', 700.0, 710.0, 98.0, 7.5, '2026-08-01 12:00:00'),
('DEB-105', 'Debris', 690.0, 720.0, 98.0, 7.5, '2026-08-01 12:00:00'),
('SAT-006', 'Satellite', 400.0, 410.0, 51.6, 7.7, '2026-08-01 12:00:00'),
('DEB-106', 'Debris', 400.0, 400.0, 51.6, 7.8, '2026-08-01 12:00:00'),
('SAT-007', 'Satellite', 600.0, 610.0, 40.0, 7.5, '2026-08-01 12:00:00'),
('DEB-107', 'Debris', 1500.0, 1600.0, 10.0, 7.0, '2026-08-01 12:00:00'),
('SAT-008', 'Satellite', 20000.0, 20200.0, 55.0, 3.9, '2026-08-01 12:00:00'), -- GNSS
('DEB-108', 'Debris', 20000.0, 20100.0, 56.0, 3.9, '2026-08-01 12:00:00'),
('SAT-009', 'Satellite', 36000.0, 36000.0, 0.0, 3.1, '2026-08-01 12:00:00'), -- GEO
('DEB-109', 'Debris', 35900.0, 36100.0, 0.1, 3.1, '2026-08-01 12:00:00'),
('SAT-010', 'Satellite', 500.0, 500.0, 45.0, 7.6, '2026-08-01 12:00:00'),
('DEB-110', 'Debris', 500.0, 501.0, 45.0, 7.61, '2026-08-01 12:00:00'), -- Converging
('SAT-011', 'Satellite', 450.0, 460.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-111', 'Debris', 450.0, 450.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('SAT-012', 'Satellite', 850.0, 860.0, 98.0, 7.4, '2026-08-01 12:00:00'),
('DEB-112', 'Debris', 850.0, 855.0, 90.0, 7.4, '2026-08-01 12:00:00'),
('SAT-013', 'Satellite', 900.0, 910.0, 70.0, 7.3, '2026-08-01 12:00:00'),
('DEB-113', 'Debris', 200.0, 250.0, 20.0, 7.9, '2026-08-01 12:00:00'),
('SAT-014', 'Satellite', 1000.0, 1010.0, 80.0, 7.3, '2026-08-01 12:00:00'),
('DEB-114', 'Debris', 1000.0, 1005.0, 80.0, 7.35, '2026-08-01 12:00:00'), -- Fast closing
('SAT-015', 'Satellite', 1100.0, 1120.0, 30.0, 7.2, '2026-08-01 12:00:00'),
('DEB-115', 'Debris', 1100.0, 1100.0, 30.0, 7.2, '2026-08-01 12:00:00'),
('SAT-016', 'Satellite', 525.0, 530.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-116', 'Debris', 525.0, 525.0, 54.0, 7.6, '2026-08-01 12:00:00'),
('SAT-017', 'Satellite', 575.0, 580.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-117', 'Debris', 575.0, 575.0, 53.1, 7.6, '2026-08-01 12:00:00'),
('SAT-018', 'Satellite', 625.0, 630.0, 98.0, 7.5, '2026-08-01 12:00:00'),
('DEB-118', 'Debris', 625.0, 625.0, 97.0, 7.5, '2026-08-01 12:00:00'),
('SAT-019', 'Satellite', 750.0, 760.0, 98.0, 7.5, '2026-08-01 12:00:00'),
('DEB-119', 'Debris', 750.0, 755.0, 99.0, 7.5, '2026-08-01 12:00:00'),
('SAT-020', 'Satellite', 1500.0, 1510.0, 50.0, 7.1, '2026-08-01 12:00:00'),
('DEB-120', 'Debris', 1500.0, 1500.0, 50.0, 7.2, '2026-08-01 12:00:00'),
('SAT-021', 'Satellite', 480.0, 490.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-121', 'Debris', 480.0, 485.0, 53.0, 7.7, '2026-08-01 12:00:00'), -- High speed pass
('SAT-022', 'Satellite', 510.0, 520.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-122', 'Debris', 510.0, 510.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('SAT-023', 'Satellite', 540.0, 550.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-123', 'Debris', 540.0, 545.0, 53.2, 7.6, '2026-08-01 12:00:00'),
('SAT-024', 'Satellite', 560.0, 570.0, 53.0, 7.6, '2026-08-01 12:00:00'),
('DEB-124', 'Debris', 560.0, 560.0, 53.0, 7.62, '2026-08-01 12:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Collision Risks
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SpaceTrafficDB.Orbit.Silver.ConjunctionAnalysis AS
SELECT 
    s.ObjectID AS SatelliteID,
    d.ObjectID AS DebrisID,
    s.AltitudeMinKM AS SatAlt,
    d.AltitudeMinKM AS DebrisAlt,
    ABS(s.AltitudeMinKM - d.AltitudeMinKM) AS AltDeltaKM,
    ABS(s.InclinationDeg - d.InclinationDeg) AS IncDeltaDeg,
    -- Simple risk logic
    CASE 
        WHEN ABS(s.AltitudeMinKM - d.AltitudeMinKM) < 5.0 AND ABS(s.InclinationDeg - d.InclinationDeg) < 1.0 THEN 'High Risk'
        WHEN ABS(s.AltitudeMinKM - d.AltitudeMinKM) < 20.0 THEN 'Monitor'
        ELSE 'Safe'
    END AS RiskLevel,
    s.LastSeenTimestamp AS AnalysisTime
FROM SpaceTrafficDB.Orbit.Bronze.ObjectTelemetry s
JOIN SpaceTrafficDB.Orbit.Bronze.ObjectTelemetry d ON s.ObjectType = 'Satellite' AND d.ObjectType != 'Satellite';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Maneuver Directives
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SpaceTrafficDB.Orbit.Gold.EvasiveManeuvers AS
SELECT 
    SatelliteID,
    DebrisID,
    AltDeltaKM,
    RiskLevel,
    CASE 
        WHEN RiskLevel = 'High Risk' THEN 'Burn Thrusters - Raise Orbit 10km'
        WHEN RiskLevel = 'Monitor' THEN 'Increase Radar Tracking Rate'
        ELSE 'No Action'
    END AS ActionRequired
FROM SpaceTrafficDB.Orbit.Silver.ConjunctionAnalysis
WHERE RiskLevel IN ('High Risk', 'Monitor');

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify all satellites requiring 'Burn Thrusters' in the Gold maneuver view."

PROMPT 2:
"List all debris objects within 5km altitude difference of 'SAT-001'."

PROMPT 3:
"Count the number of 'High Risk' conjunctions currently detected."
*/
