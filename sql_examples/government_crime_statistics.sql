/*
    Dremio High-Volume SQL Pattern: Government Crime Statistics
    
    Business Scenario:
    Police Departments need to allocate patrols based on "Crime Hotspots".
    Analyzing crime types by Precinct and Time of Day helps optimize resource deployment.
    
    Data Story:
    We track Incident Reports and Precinct Locations.
    
    Medallion Architecture:
    - Bronze: IncidentReports, Precincts.
      *Volume*: 50+ records.
    - Silver: EnrichedIncidents (Time block classification: Day/Night).
    - Gold: HotspotAnalysis (High-frequency zones).
    
    Key Dremio Features:
    - EXTRACT(HOUR)
    - Aggregation by Zone
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentCrimeDB;
CREATE FOLDER IF NOT EXISTS GovernmentCrimeDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentCrimeDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentCrimeDB.Gold;
USE GovernmentCrimeDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentCrimeDB.Bronze.Precincts (
    PrecinctID STRING,
    Borough STRING,
    Commander STRING
);

INSERT INTO GovernmentCrimeDB.Bronze.Precincts VALUES
('PCT_01', 'Manhattan', 'Cmdr. Blue'),
('PCT_02', 'Brooklyn', 'Cmdr. Red'),
('PCT_03', 'Queens', 'Cmdr. Green'),
('PCT_04', 'Bronx', 'Cmdr. Yellow'),
('PCT_05', 'Staten Island', 'Cmdr. White');

CREATE OR REPLACE TABLE GovernmentCrimeDB.Bronze.IncidentReports (
    IncidentID STRING,
    PrecinctID STRING,
    CrimeType STRING, -- Burglary, Assault, Grand Larceny, Noise
    OccurrenceTime TIMESTAMP,
    Status STRING -- Open, Closed, Unfounded
);

-- Bulk Incidents (50 Records)
INSERT INTO GovernmentCrimeDB.Bronze.IncidentReports VALUES
('INC1001', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-01 14:00:00', 'Open'),
('INC1002', 'PCT_02', 'Assault', TIMESTAMP '2025-01-01 22:00:00', 'Open'),
('INC1003', 'PCT_03', 'Burglary', TIMESTAMP '2025-01-02 02:00:00', 'Closed'),
('INC1004', 'PCT_04', 'Noise', TIMESTAMP '2025-01-02 23:30:00', 'Closed'),
('INC1005', 'PCT_05', 'Grand Larceny', TIMESTAMP '2025-01-03 10:00:00', 'Open'),
('INC1006', 'PCT_01', 'Assault', TIMESTAMP '2025-01-03 18:00:00', 'Open'),
('INC1007', 'PCT_02', 'Burglary', TIMESTAMP '2025-01-04 03:00:00', 'Open'),
('INC1008', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-04 12:00:00', 'Closed'),
('INC1009', 'PCT_04', 'Assault', TIMESTAMP '2025-01-05 20:00:00', 'Open'),
('INC1010', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-05 15:00:00', 'Open'),
('INC1011', 'PCT_01', 'Noise', TIMESTAMP '2025-01-05 23:00:00', 'Closed'),
('INC1012', 'PCT_02', 'Assault', TIMESTAMP '2025-01-06 19:00:00', 'Open'),
('INC1013', 'PCT_02', 'Burglary', TIMESTAMP '2025-01-06 04:00:00', 'Open'),
('INC1014', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-07 14:00:00', 'Closed'),
('INC1015', 'PCT_04', 'Assault', TIMESTAMP '2025-01-07 21:00:00', 'Open'),
('INC1016', 'PCT_05', 'Noise', TIMESTAMP '2025-01-08 00:30:00', 'Closed'),
('INC1017', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-08 16:00:00', 'Open'),
('INC1018', 'PCT_02', 'Assault', TIMESTAMP '2025-01-09 20:00:00', 'Open'),
('INC1019', 'PCT_03', 'Burglary', TIMESTAMP '2025-01-09 02:00:00', 'Open'),
('INC1020', 'PCT_04', 'Grand Larceny', TIMESTAMP '2025-01-10 13:00:00', 'Closed'),
('INC1021', 'PCT_01', 'Assault', TIMESTAMP '2025-01-10 22:00:00', 'Open'),
('INC1022', 'PCT_02', 'Noise', TIMESTAMP '2025-01-11 01:00:00', 'Closed'),
('INC1023', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-11 15:00:00', 'Open'),
('INC1024', 'PCT_04', 'Assault', TIMESTAMP '2025-01-12 18:00:00', 'Open'),
('INC1025', 'PCT_05', 'Burglary', TIMESTAMP '2025-01-12 03:00:00', 'Open'),
('INC1026', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-13 11:00:00', 'Closed'),
('INC1027', 'PCT_02', 'Assault', TIMESTAMP '2025-01-13 19:30:00', 'Open'),
('INC1028', 'PCT_03', 'Noise', TIMESTAMP '2025-01-13 23:45:00', 'Closed'),
('INC1029', 'PCT_04', 'Grand Larceny', TIMESTAMP '2025-01-14 14:00:00', 'Open'),
('INC1030', 'PCT_01', 'Assault', TIMESTAMP '2025-01-14 21:00:00', 'Open'),
('INC1031', 'PCT_02', 'Burglary', TIMESTAMP '2025-01-15 02:00:00', 'Open'),
('INC1032', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-15 16:00:00', 'Closed'),
('INC1033', 'PCT_04', 'Assault', TIMESTAMP '2025-01-16 18:00:00', 'Open'),
('INC1034', 'PCT_05', 'Noise', TIMESTAMP '2025-01-16 00:00:00', 'Closed'),
('INC1035', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-17 12:00:00', 'Open'),
('INC1036', 'PCT_02', 'Assault', TIMESTAMP '2025-01-17 20:00:00', 'Open'),
('INC1037', 'PCT_03', 'Burglary', TIMESTAMP '2025-01-18 04:00:00', 'Open'),
('INC1038', 'PCT_04', 'Grand Larceny', TIMESTAMP '2025-01-18 13:00:00', 'Closed'),
('INC1039', 'PCT_01', 'Assault', TIMESTAMP '2025-01-19 22:00:00', 'Open'),
('INC1040', 'PCT_02', 'Noise', TIMESTAMP '2025-01-19 01:30:00', 'Closed'),
('INC1041', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-20 14:00:00', 'Open'),
('INC1042', 'PCT_04', 'Assault', TIMESTAMP '2025-01-20 17:00:00', 'Open'),
('INC1043', 'PCT_05', 'Burglary', TIMESTAMP '2025-01-20 03:00:00', 'Open'),
('INC1044', 'PCT_01', 'Grand Larceny', TIMESTAMP '2025-01-20 10:00:00', 'Closed'),
('INC1045', 'PCT_02', 'Assault', TIMESTAMP '2025-01-20 18:00:00', 'Open'),
('INC1046', 'PCT_03', 'Noise', TIMESTAMP '2025-01-20 23:00:00', 'Closed'),
('INC1047', 'PCT_04', 'Grand Larceny', TIMESTAMP '2025-01-20 15:00:00', 'Open'),
('INC1048', 'PCT_01', 'Assault', TIMESTAMP '2025-01-20 20:00:00', 'Open'),
('INC1049', 'PCT_02', 'Burglary', TIMESTAMP '2025-01-20 02:00:00', 'Open'),
('INC1050', 'PCT_03', 'Grand Larceny', TIMESTAMP '2025-01-20 11:00:00', 'Closed');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Temporal Enrichment
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCrimeDB.Silver.DailyCrimeLog AS
SELECT
    i.IncidentID,
    i.PrecinctID,
    p.Borough,
    i.CrimeType,
    i.OccurrenceTime,
    EXTRACT(HOUR FROM i.OccurrenceTime) AS HourOfDay,
    CASE 
        WHEN EXTRACT(HOUR FROM i.OccurrenceTime) BETWEEN 6 AND 18 THEN 'DAY_SHIFT'
        ELSE 'NIGHT_SHIFT'
    END AS ShiftBlock,
    i.Status
FROM GovernmentCrimeDB.Bronze.IncidentReports i
JOIN GovernmentCrimeDB.Bronze.Precincts p ON i.PrecinctID = p.PrecinctID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Hotspot Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCrimeDB.Gold.CrimeHeatmap AS
SELECT
    PrecinctID,
    Borough,
    ShiftBlock,
    CrimeType,
    COUNT(*) AS IncidentCount,
    -- Simple Risk Level based on Count
    CASE 
        WHEN COUNT(*) > 5 THEN 'HIGH_RISK_ZONE'
        WHEN COUNT(*) > 2 THEN 'MODERATE_RISK'
        ELSE 'LOW_RISK'
    END AS RiskLevel
FROM GovernmentCrimeDB.Silver.DailyCrimeLog
GROUP BY PrecinctID, Borough, ShiftBlock, CrimeType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show High Risk Zones for the Night Shift."
    2. "Count incidents by Crime Type in Manhattan."
    3. "Which Precinct had the most Assaults?"
*/
