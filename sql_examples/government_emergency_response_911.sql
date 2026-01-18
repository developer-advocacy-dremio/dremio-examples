/*
    Dremio High-Volume SQL Pattern: Government Emergency Response (911)
    
    Business Scenario:
    Analyzing incident response times and resource deployment efficiency.
    Optimizing dispatch to lower arrival times.
    
    Data Story:
    We track Incident Timestamps and Zones.
    
    Medallion Architecture:
    - Bronze: Incidents.
      *Volume*: 50+ records.
    - Silver: ResponseTimes (Delta calculation).
    - Gold: ZoneEfficiency (Avg response by zone).
    
    Key Dremio Features:
    - Timestamp Diff
    - Group By Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentEmergencyDB;
CREATE FOLDER IF NOT EXISTS GovernmentEmergencyDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentEmergencyDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentEmergencyDB.Gold;
USE GovernmentEmergencyDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentEmergencyDB.Bronze.Incidents (
    IncidentID STRING,
    CallTime TIMESTAMP,
    DispatchTime TIMESTAMP,
    ArrivalTime TIMESTAMP,
    Type STRING, -- Fire, Medical, Police
    Zone STRING
);

INSERT INTO GovernmentEmergencyDB.Bronze.Incidents VALUES
('I1', TIMESTAMP '2025-01-01 01:00:00', TIMESTAMP '2025-01-01 01:02:00', TIMESTAMP '2025-01-01 01:10:00', 'Medical', 'North'),
('I2', TIMESTAMP '2025-01-01 02:30:00', TIMESTAMP '2025-01-01 02:35:00', TIMESTAMP '2025-01-01 02:45:00', 'Police', 'South'),
('I3', TIMESTAMP '2025-01-01 03:00:00', TIMESTAMP '2025-01-01 03:01:00', TIMESTAMP '2025-01-01 03:05:00', 'Fire', 'West'),
('I4', TIMESTAMP '2025-01-01 05:00:00', TIMESTAMP '2025-01-01 05:03:00', TIMESTAMP '2025-01-01 05:15:00', 'Medical', 'East'),
('I5', TIMESTAMP '2025-01-01 06:15:00', TIMESTAMP '2025-01-01 06:16:00', TIMESTAMP '2025-01-01 06:20:00', 'Medical', 'North'),
('I6', TIMESTAMP '2025-01-01 08:00:00', TIMESTAMP '2025-01-01 08:05:00', TIMESTAMP '2025-01-01 08:12:00', 'Police', 'South'),
('I7', TIMESTAMP '2025-01-01 10:00:00', TIMESTAMP '2025-01-01 10:02:00', TIMESTAMP '2025-01-01 10:08:00', 'Fire', 'North'),
('I8', TIMESTAMP '2025-01-01 12:00:00', TIMESTAMP '2025-01-01 12:03:00', TIMESTAMP '2025-01-01 12:15:00', 'Medical', 'West'),
('I9', TIMESTAMP '2025-01-01 14:00:00', TIMESTAMP '2025-01-01 14:05:00', TIMESTAMP '2025-01-01 14:20:00', 'Police', 'East'),
('I10', TIMESTAMP '2025-01-01 16:00:00', TIMESTAMP '2025-01-01 16:01:00', TIMESTAMP '2025-01-01 16:06:00', 'Medical', 'South'),
('I11', TIMESTAMP '2025-01-01 18:00:00', TIMESTAMP '2025-01-01 18:04:00', TIMESTAMP '2025-01-01 18:10:00', 'Fire', 'East'),
('I12', TIMESTAMP '2025-01-02 01:00:00', TIMESTAMP '2025-01-02 01:03:00', TIMESTAMP '2025-01-02 01:12:00', 'Medical', 'North'),
('I13', TIMESTAMP '2025-01-02 03:00:00', TIMESTAMP '2025-01-02 03:05:00', TIMESTAMP '2025-01-02 03:15:00', 'Police', 'South'),
('I14', TIMESTAMP '2025-01-02 05:00:00', TIMESTAMP '2025-01-02 05:02:00', TIMESTAMP '2025-01-02 05:08:00', 'Fire', 'West'),
('I15', TIMESTAMP '2025-01-02 07:00:00', TIMESTAMP '2025-01-02 07:04:00', TIMESTAMP '2025-01-02 07:18:00', 'Medical', 'East'),
('I16', TIMESTAMP '2025-01-02 09:00:00', TIMESTAMP '2025-01-02 09:01:00', TIMESTAMP '2025-01-02 09:05:00', 'Medical', 'North'),
('I17', TIMESTAMP '2025-01-02 11:00:00', TIMESTAMP '2025-01-02 11:06:00', TIMESTAMP '2025-01-02 11:15:00', 'Police', 'South'),
('I18', TIMESTAMP '2025-01-02 13:00:00', TIMESTAMP '2025-01-02 13:02:00', TIMESTAMP '2025-01-02 13:10:00', 'Fire', 'North'),
('I19', TIMESTAMP '2025-01-02 15:00:00', TIMESTAMP '2025-01-02 15:05:00', TIMESTAMP '2025-01-02 15:20:00', 'Medical', 'West'),
('I20', TIMESTAMP '2025-01-02 17:00:00', TIMESTAMP '2025-01-02 17:03:00', TIMESTAMP '2025-01-02 17:12:00', 'Police', 'East'),
('I21', TIMESTAMP '2025-01-02 19:00:00', TIMESTAMP '2025-01-02 19:02:00', TIMESTAMP '2025-01-02 19:09:00', 'Medical', 'South'),
('I22', TIMESTAMP '2025-01-02 21:00:00', TIMESTAMP '2025-01-02 21:05:00', TIMESTAMP '2025-01-02 21:15:00', 'Fire', 'East'),
('I23', TIMESTAMP '2025-01-03 02:00:00', TIMESTAMP '2025-01-03 02:03:00', TIMESTAMP '2025-01-03 02:11:00', 'Medical', 'North'),
('I24', TIMESTAMP '2025-01-03 04:00:00', TIMESTAMP '2025-01-03 04:06:00', TIMESTAMP '2025-01-03 04:16:00', 'Police', 'South'),
('I25', TIMESTAMP '2025-01-03 06:00:00', TIMESTAMP '2025-01-03 06:01:00', TIMESTAMP '2025-01-03 06:07:00', 'Fire', 'West'),
('I26', TIMESTAMP '2025-01-03 08:00:00', TIMESTAMP '2025-01-03 08:04:00', TIMESTAMP '2025-01-03 08:20:00', 'Medical', 'East'),
('I27', TIMESTAMP '2025-01-03 10:00:00', TIMESTAMP '2025-01-03 10:02:00', TIMESTAMP '2025-01-03 10:06:00', 'Medical', 'North'),
('I28', TIMESTAMP '2025-01-03 12:00:00', TIMESTAMP '2025-01-03 12:05:00', TIMESTAMP '2025-01-03 12:15:00', 'Police', 'South'),
('I29', TIMESTAMP '2025-01-03 14:00:00', TIMESTAMP '2025-01-03 14:01:00', TIMESTAMP '2025-01-03 14:08:00', 'Fire', 'North'),
('I30', TIMESTAMP '2025-01-03 16:00:00', TIMESTAMP '2025-01-03 16:04:00', TIMESTAMP '2025-01-03 16:16:00', 'Medical', 'West'),
('I31', TIMESTAMP '2025-01-03 18:00:00', TIMESTAMP '2025-01-03 18:03:00', TIMESTAMP '2025-01-03 18:18:00', 'Police', 'East'),
('I32', TIMESTAMP '2025-01-03 20:00:00', TIMESTAMP '2025-01-03 20:01:00', TIMESTAMP '2025-01-03 20:07:00', 'Medical', 'South'),
('I33', TIMESTAMP '2025-01-03 22:00:00', TIMESTAMP '2025-01-03 22:05:00', TIMESTAMP '2025-01-03 22:15:00', 'Fire', 'East'),
('I34', TIMESTAMP '2025-01-04 01:30:00', TIMESTAMP '2025-01-04 01:33:00', TIMESTAMP '2025-01-04 01:40:00', 'Medical', 'North'),
('I35', TIMESTAMP '2025-01-04 03:30:00', TIMESTAMP '2025-01-04 03:36:00', TIMESTAMP '2025-01-04 03:46:00', 'Police', 'South'),
('I36', TIMESTAMP '2025-01-04 05:30:00', TIMESTAMP '2025-01-04 05:31:00', TIMESTAMP '2025-01-04 05:06:00', 'Fire', 'West'),
('I37', TIMESTAMP '2025-01-04 07:30:00', TIMESTAMP '2025-01-04 07:34:00', TIMESTAMP '2025-01-04 07:44:00', 'Medical', 'East'),
('I38', TIMESTAMP '2025-01-04 09:30:00', TIMESTAMP '2025-01-04 09:32:00', TIMESTAMP '2025-01-04 09:37:00', 'Medical', 'North'),
('I39', TIMESTAMP '2025-01-04 11:30:00', TIMESTAMP '2025-01-04 11:36:00', TIMESTAMP '2025-01-04 11:45:00', 'Police', 'South'),
('I40', TIMESTAMP '2025-01-04 13:30:00', TIMESTAMP '2025-01-04 13:32:00', TIMESTAMP '2025-01-04 13:40:00', 'Fire', 'North'),
('I41', TIMESTAMP '2025-01-04 15:30:00', TIMESTAMP '2025-01-04 15:33:00', TIMESTAMP '2025-01-04 15:45:00', 'Medical', 'West'),
('I42', TIMESTAMP '2025-01-04 17:30:00', TIMESTAMP '2025-01-04 17:35:00', TIMESTAMP '2025-01-04 17:50:00', 'Police', 'East'),
('I43', TIMESTAMP '2025-01-04 19:30:00', TIMESTAMP '2025-01-04 19:31:00', TIMESTAMP '2025-01-04 19:39:00', 'Medical', 'South'),
('I44', TIMESTAMP '2025-01-04 21:30:00', TIMESTAMP '2025-01-04 21:33:00', TIMESTAMP '2025-01-04 21:40:00', 'Fire', 'East'),
('I45', TIMESTAMP '2025-01-05 02:00:00', TIMESTAMP '2025-01-05 02:03:00', TIMESTAMP '2025-01-05 02:10:00', 'Medical', 'North'),
('I46', TIMESTAMP '2025-01-05 04:00:00', TIMESTAMP '2025-01-05 04:05:00', TIMESTAMP '2025-01-05 04:15:00', 'Police', 'South'),
('I47', TIMESTAMP '2025-01-05 06:00:00', TIMESTAMP '2025-01-05 06:01:00', TIMESTAMP '2025-01-05 06:08:00', 'Fire', 'West'),
('I48', TIMESTAMP '2025-01-05 08:00:00', TIMESTAMP '2025-01-05 08:04:00', TIMESTAMP '2025-01-05 08:18:00', 'Medical', 'East'),
('I49', TIMESTAMP '2025-01-05 10:00:00', TIMESTAMP '2025-01-05 10:02:00', TIMESTAMP '2025-01-05 10:07:00', 'Medical', 'North'),
('I50', TIMESTAMP '2025-01-05 12:00:00', TIMESTAMP '2025-01-05 12:05:00', TIMESTAMP '2025-01-05 12:12:00', 'Police', 'South');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Response Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEmergencyDB.Silver.ResponseTimes AS
SELECT 
    IncidentID,
    Type,
    Zone,
    TIMESTAMPDIFF(SECOND, CallTime, DispatchTime) AS DispatchDelaySeconds,
    TIMESTAMPDIFF(SECOND, DispatchTime, ArrivalTime) AS TravelTimeSeconds,
    TIMESTAMPDIFF(SECOND, CallTime, ArrivalTime) AS TotalResponseSeconds
FROM GovernmentEmergencyDB.Bronze.Incidents;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Zone Efficiency
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentEmergencyDB.Gold.ZoneEfficiency AS
SELECT 
    Zone,
    Type,
    AVG(TotalResponseSeconds) / 60.0 AS AvgResponseMinutes,
    COUNT(*) AS IncidentCount
FROM GovernmentEmergencyDB.Silver.ResponseTimes
GROUP BY Zone, Type;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Calculate the average response time for Medical incidents."
    2. "Which Zone has the fastest response time for Fire?"
    3. "Show incident counts by Zone."
*/
