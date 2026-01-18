/*
    Dremio High-Volume SQL Pattern: Government Fire Incident Response
    
    Business Scenario:
    Fire Departments strive for low "Response Times" (Dispatch to Arrival) to save lives and property.
    Analyzing response times by Incident Grade helps improve station coverage.
    
    Data Story:
    We track Dispatch Logs and Incident Severity.
    
    Medallion Architecture:
    - Bronze: Dispatches, IncidentTypes.
      *Volume*: 50+ records.
    - Silver: ResponseMetrics (Calculating Seconds to Arrive).
    - Gold: BattalionPerformance (Avg Response vs Goal).
    
    Key Dremio Features:
    - TIMESTAMPDIFF(SECOND, ...)
    - Averaging
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentFireDB;
CREATE FOLDER IF NOT EXISTS GovernmentFireDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentFireDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentFireDB.Gold;
USE GovernmentFireDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentFireDB.Bronze.IncidentTypes (
    TypeCode STRING,
    Description STRING,
    TargetResponseSeconds INT
);

INSERT INTO GovernmentFireDB.Bronze.IncidentTypes VALUES
('10-75', 'Working Fire', 240), -- 4 mins
('MED', 'Medical Emergency', 300), -- 5 mins
('ALARM', 'Automatic Alarm', 600), -- 10 mins
('HAZ', 'Hazmat Incident', 480);

CREATE OR REPLACE TABLE GovernmentFireDB.Bronze.Dispatches (
    DispatchID STRING,
    BattalionID STRING,
    TypeCode STRING,
    DispatchTime TIMESTAMP,
    OnSceneTime TIMESTAMP,
    PropertyLossEst DOUBLE
);

-- Bulk Dispatches (50 Records)
INSERT INTO GovernmentFireDB.Bronze.Dispatches VALUES
('D1001', 'BAT_1', '10-75', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 08:03:30', 50000.0), -- Met
('D1002', 'BAT_2', 'MED', TIMESTAMP '2025-01-20 08:15:00', TIMESTAMP '2025-01-20 08:21:00', 0.0), -- Missed (>5m)
('D1003', 'BAT_3', 'ALARM', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 09:08:00', 0.0), -- Met
('D1004', 'BAT_1', 'HAZ', TIMESTAMP '2025-01-20 09:30:00', TIMESTAMP '2025-01-20 09:40:00', 1000.0), -- Missed
('D1005', 'BAT_2', '10-75', TIMESTAMP '2025-01-20 10:00:00', TIMESTAMP '2025-01-20 10:04:00', 75000.0), -- Met
('D1006', 'BAT_3', 'MED', TIMESTAMP '2025-01-20 10:30:00', TIMESTAMP '2025-01-20 10:34:00', 0.0), -- Met
('D1007', 'BAT_1', 'ALARM', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 11:15:00', 0.0), -- Missed (Traffic?)
('D1008', 'BAT_2', 'HAZ', TIMESTAMP '2025-01-20 11:30:00', TIMESTAMP '2025-01-20 11:36:00', 500.0), -- Met
('D1009', 'BAT_3', '10-75', TIMESTAMP '2025-01-20 12:00:00', TIMESTAMP '2025-01-20 12:05:00', 25000.0), -- Missed
('D1010', 'BAT_1', 'MED', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 12:33:00', 0.0),
('D1011', 'BAT_2', 'ALARM', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 13:09:00', 0.0),
('D1012', 'BAT_3', 'HAZ', TIMESTAMP '2025-01-20 13:30:00', TIMESTAMP '2025-01-20 13:38:00', 0.0),
('D1013', 'BAT_1', '10-75', TIMESTAMP '2025-01-20 14:00:00', TIMESTAMP '2025-01-20 14:02:00', 15000.0), -- Fast
('D1014', 'BAT_2', 'MED', TIMESTAMP '2025-01-20 14:30:00', TIMESTAMP '2025-01-20 14:36:00', 0.0),
('D1015', 'BAT_3', 'ALARM', TIMESTAMP '2025-01-20 15:00:00', TIMESTAMP '2025-01-20 15:05:00', 0.0),
('D1016', 'BAT_1', 'HAZ', TIMESTAMP '2025-01-20 15:30:00', TIMESTAMP '2025-01-20 15:35:00', 2000.0),
('D1017', 'BAT_2', '10-75', TIMESTAMP '2025-01-20 16:00:00', TIMESTAMP '2025-01-20 16:04:30', 40000.0), -- Missed slightly
('D1018', 'BAT_3', 'MED', TIMESTAMP '2025-01-20 16:30:00', TIMESTAMP '2025-01-20 16:35:00', 0.0),
('D1019', 'BAT_1', 'ALARM', TIMESTAMP '2025-01-20 17:00:00', TIMESTAMP '2025-01-20 17:08:00', 0.0),
('D1020', 'BAT_2', 'HAZ', TIMESTAMP '2025-01-20 17:30:00', TIMESTAMP '2025-01-20 17:40:00', 5000.0), -- Missed (Rush hour)
('D1021', 'BAT_3', '10-75', TIMESTAMP '2025-01-20 18:00:00', TIMESTAMP '2025-01-20 18:03:00', 60000.0),
('D1022', 'BAT_1', 'MED', TIMESTAMP '2025-01-20 18:30:00', TIMESTAMP '2025-01-20 18:37:00', 0.0),
('D1023', 'BAT_2', 'ALARM', TIMESTAMP '2025-01-20 19:00:00', TIMESTAMP '2025-01-20 19:06:00', 0.0),
('D1024', 'BAT_3', 'HAZ', TIMESTAMP '2025-01-20 19:30:00', TIMESTAMP '2025-01-20 19:35:00', 0.0),
('D1025', 'BAT_1', '10-75', TIMESTAMP '2025-01-20 20:00:00', TIMESTAMP '2025-01-20 20:05:00', 30000.0),
('D1026', 'BAT_2', 'MED', TIMESTAMP '2025-01-20 20:30:00', TIMESTAMP '2025-01-20 20:34:00', 0.0),
('D1027', 'BAT_3', 'ALARM', TIMESTAMP '2025-01-20 21:00:00', TIMESTAMP '2025-01-20 21:12:00', 0.0), -- Missed
('D1028', 'BAT_1', 'HAZ', TIMESTAMP '2025-01-20 21:30:00', TIMESTAMP '2025-01-20 21:37:00', 0.0),
('D1029', 'BAT_2', '10-75', TIMESTAMP '2025-01-20 22:00:00', TIMESTAMP '2025-01-20 22:03:00', 20000.0),
('D1030', 'BAT_3', 'MED', TIMESTAMP '2025-01-20 22:30:00', TIMESTAMP '2025-01-20 22:35:00', 0.0),
('D1031', 'BAT_1', 'ALARM', TIMESTAMP '2025-01-20 23:00:00', TIMESTAMP '2025-01-20 23:07:00', 0.0),
('D1032', 'BAT_2', 'HAZ', TIMESTAMP '2025-01-20 23:30:00', TIMESTAMP '2025-01-20 23:36:00', 0.0),
('D1033', 'BAT_3', '10-75', TIMESTAMP '2025-01-21 00:00:00', TIMESTAMP '2025-01-21 00:04:00', 90000.0),
('D1034', 'BAT_1', 'MED', TIMESTAMP '2025-01-21 00:30:00', TIMESTAMP '2025-01-21 00:33:00', 0.0),
('D1035', 'BAT_2', 'ALARM', TIMESTAMP '2025-01-21 01:00:00', TIMESTAMP '2025-01-21 01:05:00', 0.0),
('D1036', 'BAT_3', 'HAZ', TIMESTAMP '2025-01-21 01:30:00', TIMESTAMP '2025-01-21 01:39:00', 0.0), -- Missed
('D1037', 'BAT_1', '10-75', TIMESTAMP '2025-01-21 02:00:00', TIMESTAMP '2025-01-21 02:03:00', 10000.0),
('D1038', 'BAT_2', 'MED', TIMESTAMP '2025-01-21 02:30:00', TIMESTAMP '2025-01-21 02:38:00', 0.0),
('D1039', 'BAT_3', 'ALARM', TIMESTAMP '2025-01-21 03:00:00', TIMESTAMP '2025-01-21 03:09:00', 0.0),
('D1040', 'BAT_1', 'HAZ', TIMESTAMP '2025-01-21 03:30:00', TIMESTAMP '2025-01-21 03:36:00', 0.0),
('D1041', 'BAT_2', '10-75', TIMESTAMP '2025-01-21 04:00:00', TIMESTAMP '2025-01-21 04:04:00', 35000.0),
('D1042', 'BAT_3', 'MED', TIMESTAMP '2025-01-21 04:30:00', TIMESTAMP '2025-01-21 04:34:00', 0.0),
('D1043', 'BAT_1', 'ALARM', TIMESTAMP '2025-01-21 05:00:00', TIMESTAMP '2025-01-21 05:08:00', 0.0),
('D1044', 'BAT_2', 'HAZ', TIMESTAMP '2025-01-21 05:30:00', TIMESTAMP '2025-01-21 05:37:00', 0.0),
('D1045', 'BAT_3', '10-75', TIMESTAMP '2025-01-21 06:00:00', TIMESTAMP '2025-01-21 06:05:00', 12000.0),
('D1046', 'BAT_1', 'MED', TIMESTAMP '2025-01-21 06:30:00', TIMESTAMP '2025-01-21 06:32:00', 0.0),
('D1047', 'BAT_2', 'ALARM', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 07:07:00', 0.0),
('D1048', 'BAT_3', 'HAZ', TIMESTAMP '2025-01-21 07:30:00', TIMESTAMP '2025-01-21 07:35:00', 0.0),
('D1049', 'BAT_1', '10-75', TIMESTAMP '2025-01-21 08:00:00', TIMESTAMP '2025-01-21 08:03:00', 80000.0),
('D1050', 'BAT_2', 'MED', TIMESTAMP '2025-01-21 08:30:00', TIMESTAMP '2025-01-21 08:31:00', 0.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Response Calculation
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentFireDB.Silver.ResponseTimes AS
SELECT
    d.DispatchID,
    d.BattalionID,
    t.Description,
    d.PropertyLossEst,
    d.DispatchTime,
    d.OnSceneTime,
    TIMESTAMPDIFF(SECOND, d.DispatchTime, d.OnSceneTime) AS ActualSeconds,
    t.TargetResponseSeconds,
    CASE 
        WHEN TIMESTAMPDIFF(SECOND, d.DispatchTime, d.OnSceneTime) <= t.TargetResponseSeconds THEN 1 
        ELSE 0 
    END AS MetTargetFlag
FROM GovernmentFireDB.Bronze.Dispatches d
JOIN GovernmentFireDB.Bronze.IncidentTypes t ON d.TypeCode = t.TypeCode;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Battalion Scorecard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentFireDB.Gold.BattalionPerformance AS
SELECT
    BattalionID,
    COUNT(*) AS TotalIncidents,
    AVG(ActualSeconds) AS AvgResponseSeconds,
    SUM(PropertyLossEst) AS TotalLoss,
    (CAST(SUM(MetTargetFlag) AS DOUBLE) / COUNT(*)) * 100 AS SLA_Compliance_Pct
FROM GovernmentFireDB.Silver.ResponseTimes
GROUP BY BattalionID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List total property loss by Battalion."
    2. "Show the average response time for 'Working Fire' incidents."
    3. "Which Battalion has the lowest SLA Compliance?"
*/
