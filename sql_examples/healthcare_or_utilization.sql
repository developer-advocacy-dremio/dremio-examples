/*
    Dremio High-Volume SQL Pattern: Healthcare OR (Operating Room) Utilization
    
    Business Scenario:
    OR time is the most expensive resource in a hospital. Maximizing "Block Utilization"
    (Actual Minutes / Blocked Minutes) is key to profitability.
    
    Data Story:
    We compare the Block Schedule (Reserved time) vs Actual Surgical Logs (Wheels In to Wheels Out).
    
    Medallion Architecture:
    - Bronze: SurgicalLogs, BlockSchedule.
      *Volume*: 50+ records.
    - Silver: UtilizationMetrics (Minutes Used vs Blocked).
    - Gold: EfficiencyDashboard (Under-utilized blocks).
    
    Key Dremio Features:
    - TIMESTAMPDIFF(MINUTE)
    - JOIN on Surgeon/Service
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareORDB;
CREATE FOLDER IF NOT EXISTS HealthcareORDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareORDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareORDB.Gold;
USE HealthcareORDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareORDB.Bronze.BlockSchedule (
    BlockID STRING,
    OR_Room STRING,
    Service STRING, -- Ortho, Cardio, Neuro
    SurgeonID STRING,
    BlockDate DATE,
    BlockStart TIMESTAMP,
    BlockEnd TIMESTAMP
);

INSERT INTO HealthcareORDB.Bronze.BlockSchedule VALUES
('B001', 'OR1', 'Ortho', 'DR_SMITH', DATE '2025-01-20', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 15:00:00'), -- 8 hours (480 mins)
('B002', 'OR2', 'Cardio', 'DR_JONES', DATE '2025-01-20', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 17:00:00'), -- 10 hours
('B003', 'OR1', 'Neuro', 'DR_LEE', DATE '2025-01-21', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 15:00:00');
-- Bulk Blocks
INSERT INTO HealthcareORDB.Bronze.BlockSchedule
SELECT 
  'B' || CAST(rn + 100 AS STRING),
  CASE WHEN rn % 2 = 0 THEN 'OR3' ELSE 'OR4' END,
  'General',
  'DR_GENERIC',
  DATE_ADD(DATE '2025-01-20', CAST((rn % 5) AS INT)),
  TIMESTAMP '2025-01-20 07:00:00',
  TIMESTAMP '2025-01-20 15:00:00'
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS t(rn); -- Simplified bulk

CREATE OR REPLACE TABLE HealthcareORDB.Bronze.SurgicalLogs (
    CaseID STRING,
    BlockID STRING, -- Link to block if planned
    PatientID STRING,
    WheelsIn TIMESTAMP,
    WheelsOut TIMESTAMP
);

INSERT INTO HealthcareORDB.Bronze.SurgicalLogs VALUES
('C001', 'B001', 'P001', TIMESTAMP '2025-01-20 07:15:00', TIMESTAMP '2025-01-20 09:15:00'), -- 2h
('C002', 'B001', 'P002', TIMESTAMP '2025-01-20 09:45:00', TIMESTAMP '2025-01-20 11:45:00'), -- 2h
('C003', 'B001', 'P003', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 14:00:00'), -- 1.5h. Total 5.5h used out of 8h.
('C004', 'B002', 'P004', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 12:00:00'); -- 5h used out of 10h.
-- Bulk Cases
INSERT INTO HealthcareORDB.Bronze.SurgicalLogs
SELECT 
  'C' || CAST(rn + 100 AS STRING),
  'B' || CAST((rn % 9) + 100 AS STRING), -- Link to bulk blocks
  'P' || CAST(rn AS STRING),
  TIMESTAMP '2025-01-20 08:00:00',
  TIMESTAMP '2025-01-20 11:00:00' -- 3 hours per case
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Utilization Calc
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareORDB.Silver.BlockPerf AS
SELECT
    b.BlockID,
    b.OR_Room,
    b.Service,
    b.SurgeonID,
    TIMESTAMPDIFF(MINUTE, b.BlockStart, b.BlockEnd) AS BlockedMinutes,
    SUM(TIMESTAMPDIFF(MINUTE, s.WheelsIn, s.WheelsOut)) AS UsedMinutes
FROM HealthcareORDB.Bronze.BlockSchedule b
LEFT JOIN HealthcareORDB.Bronze.SurgicalLogs s ON b.BlockID = s.BlockID
GROUP BY b.BlockID, b.OR_Room, b.Service, b.SurgeonID, b.BlockStart, b.BlockEnd;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Efficiency Dashboard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareORDB.Gold.RoomEfficiency AS
SELECT
    BlockID,
    OR_Room,
    Service,
    SurgeonID,
    BlockedMinutes,
    COALESCE(UsedMinutes, 0) AS UsedMinutes,
    (CAST(COALESCE(UsedMinutes, 0) AS DOUBLE) / NULLIF(BlockedMinutes, 0)) * 100 AS UtilizationPct,
    CASE
        WHEN (CAST(COALESCE(UsedMinutes, 0) AS DOUBLE) / NULLIF(BlockedMinutes, 0)) < 0.70 THEN 'UNDER_UTILIZED'
        WHEN (CAST(COALESCE(UsedMinutes, 0) AS DOUBLE) / NULLIF(BlockedMinutes, 0)) > 0.90 THEN 'OVER_UTILIZED'
        ELSE 'OPTIMAL'
    END AS Status
FROM HealthcareORDB.Silver.BlockPerf;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me all surgeons with under-utilized block time (< 70%)."
    2. "Calculate the average utilization percentage by Service."
    3. "Which OR Room is the most efficiently used?"
    4. "List blocks where utilization was 0% (Unused time)."
*/
