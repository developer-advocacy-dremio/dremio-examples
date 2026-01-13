/*
 * Electric Vehicle (EV) Charging Station Demo
 * 
 * Scenario:
 * Monitoring charger availability, energy consumption, and peak usage hours.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize grid load and maintenance schedules.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS EnergyDB;
CREATE FOLDER IF NOT EXISTS EnergyDB.Bronze;
CREATE FOLDER IF NOT EXISTS EnergyDB.Silver;
CREATE FOLDER IF NOT EXISTS EnergyDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EnergyDB.Bronze.Chargers (
    ChargerID VARCHAR,
    Location VARCHAR,
    MaxOutputKW INT,
    InstallDate DATE
);

CREATE TABLE IF NOT EXISTS EnergyDB.Bronze.ChargingSessions (
    SessionID INT,
    ChargerID VARCHAR,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP,
    KWhDelivered DOUBLE,
    Fee DOUBLE
);

INSERT INTO EnergyDB.Bronze.Chargers VALUES
('C-001', 'Downtown Garage', 150, '2023-01-01'),
('C-002', 'Mall Parking', 50, '2023-05-15');

INSERT INTO EnergyDB.Bronze.ChargingSessions VALUES
(1, 'C-001', '2025-08-01 08:00:00', '2025-08-01 08:45:00', 45.5, 15.00),
(2, 'C-001', '2025-08-01 09:00:00', '2025-08-01 09:30:00', 30.0, 10.00),
(3, 'C-002', '2025-08-01 12:00:00', '2025-08-01 14:00:00', 20.0, 8.00); -- Slow charger

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EnergyDB.Silver.ChargerUtilization AS
SELECT 
    c.ChargerID,
    c.Location,
    s.StartTime,
    s.KWhDelivered,
    TIMESTAMPDIFF(MINUTE, s.StartTime, s.EndTime) AS DurationMinutes,
    (s.KWhDelivered / (TIMESTAMPDIFF(MINUTE, s.StartTime, s.EndTime) / 60.0)) AS AvgPowerKW
FROM EnergyDB.Bronze.ChargingSessions s
JOIN EnergyDB.Bronze.Chargers c ON s.ChargerID = c.ChargerID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW EnergyDB.Gold.LocationPerformance AS
SELECT 
    Location,
    COUNT(SessionID) AS TotalSessions,
    SUM(KWhDelivered) AS TotalEnergyDelivered,
    SUM(Fee) AS TotalRevenue,
    AVG(DurationMinutes) AS AvgSessionLength
FROM EnergyDB.Silver.ChargerUtilization s
JOIN EnergyDB.Bronze.ChargingSessions cs ON 1=1 -- Implicit join via view logic, simplified for demo
GROUP BY Location;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Location produced the most TotalRevenue in EnergyDB.Gold.LocationPerformance?"
*/
