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
('B001', 'OR1', 'Ortho', 'DR_SMITH', DATE '2025-01-20', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 15:00:00'),
('B002', 'OR2', 'Cardio', 'DR_JONES', DATE '2025-01-20', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 17:00:00'),
('B003', 'OR1', 'Neuro', 'DR_LEE', DATE '2025-01-21', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 15:00:00'),
('B004', 'OR3', 'Ortho', 'DR_BROWN', DATE '2025-01-20', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 15:00:00'),
('B005', 'OR4', 'General', 'DR_WHITE', DATE '2025-01-20', TIMESTAMP '2025-01-20 08:00:00', TIMESTAMP '2025-01-20 16:00:00'),
('B006', 'OR2', 'Cardio', 'DR_JONES', DATE '2025-01-21', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 17:00:00'),
('B007', 'OR3', 'Ortho', 'DR_SMITH', DATE '2025-01-21', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 15:00:00'),
('B008', 'OR4', 'General', 'DR_GREEN', DATE '2025-01-21', TIMESTAMP '2025-01-21 08:00:00', TIMESTAMP '2025-01-21 16:00:00'),
('B009', 'OR1', 'Neuro', 'DR_LEE', DATE '2025-01-22', TIMESTAMP '2025-01-22 07:00:00', TIMESTAMP '2025-01-22 15:00:00'),
('B010', 'OR2', 'Cardio', 'DR_BLACK', DATE '2025-01-22', TIMESTAMP '2025-01-22 07:00:00', TIMESTAMP '2025-01-22 17:00:00');

CREATE OR REPLACE TABLE HealthcareORDB.Bronze.SurgicalLogs (
    CaseID STRING,
    BlockID STRING, -- Link to block if planned
    PatientID STRING,
    WheelsIn TIMESTAMP,
    WheelsOut TIMESTAMP
);

INSERT INTO HealthcareORDB.Bronze.SurgicalLogs VALUES
('C001', 'B001', 'P001', TIMESTAMP '2025-01-20 07:15:00', TIMESTAMP '2025-01-20 09:15:00'),
('C002', 'B001', 'P002', TIMESTAMP '2025-01-20 09:45:00', TIMESTAMP '2025-01-20 11:45:00'),
('C003', 'B001', 'P003', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 14:00:00'),
('C004', 'B002', 'P004', TIMESTAMP '2025-01-20 07:00:00', TIMESTAMP '2025-01-20 12:00:00'),
('C005', 'B002', 'P005', TIMESTAMP '2025-01-20 12:30:00', TIMESTAMP '2025-01-20 16:30:00'),
('C006', 'B003', 'P006', TIMESTAMP '2025-01-21 07:30:00', TIMESTAMP '2025-01-21 10:30:00'),
('C007', 'B003', 'P007', TIMESTAMP '2025-01-21 11:00:00', TIMESTAMP '2025-01-21 14:00:00'),
('C008', 'B004', 'P008', TIMESTAMP '2025-01-20 07:15:00', TIMESTAMP '2025-01-20 08:15:00'), -- Low Utilization
('C009', 'B004', 'P009', TIMESTAMP '2025-01-20 09:00:00', TIMESTAMP '2025-01-20 10:00:00'),
('C010', 'B005', 'P010', TIMESTAMP '2025-01-20 08:30:00', TIMESTAMP '2025-01-20 10:30:00'),
('C011', 'B005', 'P011', TIMESTAMP '2025-01-20 11:00:00', TIMESTAMP '2025-01-20 13:00:00'),
('C012', 'B005', 'P012', TIMESTAMP '2025-01-20 13:30:00', TIMESTAMP '2025-01-20 15:30:00'),
('C013', 'B006', 'P013', TIMESTAMP '2025-01-21 07:00:00', TIMESTAMP '2025-01-21 16:30:00'), -- High Utilization
('C014', 'B007', 'P014', TIMESTAMP '2025-01-21 07:15:00', TIMESTAMP '2025-01-21 09:15:00'),
('C015', 'B007', 'P015', TIMESTAMP '2025-01-21 10:00:00', TIMESTAMP '2025-01-21 12:00:00'),
('C016', 'B008', 'P016', TIMESTAMP '2025-01-21 08:15:00', TIMESTAMP '2025-01-21 09:15:00'),
('C017', 'B008', 'P017', TIMESTAMP '2025-01-21 10:00:00', TIMESTAMP '2025-01-21 11:00:00'),
('C018', 'B008', 'P018', TIMESTAMP '2025-01-21 12:00:00', TIMESTAMP '2025-01-21 13:00:00'),
('C019', 'B009', 'P019', TIMESTAMP '2025-01-22 07:30:00', TIMESTAMP '2025-01-22 14:30:00'),
('C020', 'B010', 'P020', TIMESTAMP '2025-01-22 07:30:00', TIMESTAMP '2025-01-22 12:30:00'),
('C021', 'B010', 'P021', TIMESTAMP '2025-01-22 13:00:00', TIMESTAMP '2025-01-22 16:00:00'),
('C022', 'B001', 'P022', TIMESTAMP '2025-01-20 14:15:00', TIMESTAMP '2025-01-20 14:45:00'),
('C023', 'B004', 'P023', TIMESTAMP '2025-01-20 13:00:00', TIMESTAMP '2025-01-20 14:00:00'),
('C024', 'B007', 'P024', TIMESTAMP '2025-01-21 13:00:00', TIMESTAMP '2025-01-21 14:30:00'),
('C025', 'B008', 'P025', TIMESTAMP '2025-01-21 14:00:00', TIMESTAMP '2025-01-21 15:30:00'),
('C026', 'B001', 'P026', TIMESTAMP '2025-01-20 14:50:00', TIMESTAMP '2025-01-20 15:00:00'),
('C027', 'B002', 'P027', TIMESTAMP '2025-01-20 16:35:00', TIMESTAMP '2025-01-20 16:55:00'),
('C028', 'B003', 'P028', TIMESTAMP '2025-01-21 14:10:00', TIMESTAMP '2025-01-21 14:50:00'),
('C029', 'B004', 'P029', TIMESTAMP '2025-01-20 14:10:00', TIMESTAMP '2025-01-20 14:45:00'),
('C030', 'B005', 'P030', TIMESTAMP '2025-01-20 15:35:00', TIMESTAMP '2025-01-20 15:55:00'),
('C031', 'B006', 'P031', TIMESTAMP '2025-01-21 16:35:00', TIMESTAMP '2025-01-21 16:55:00'),
('C032', 'B007', 'P032', TIMESTAMP '2025-01-21 14:35:00', TIMESTAMP '2025-01-21 14:55:00'),
('C033', 'B008', 'P033', TIMESTAMP '2025-01-21 15:35:00', TIMESTAMP '2025-01-21 15:55:00'),
('C034', 'B009', 'P034', TIMESTAMP '2025-01-22 14:35:00', TIMESTAMP '2025-01-22 14:55:00'),
('C035', 'B010', 'P035', TIMESTAMP '2025-01-22 16:05:00', TIMESTAMP '2025-01-22 16:55:00'),
('C036', 'B001', 'P036', TIMESTAMP '2025-01-20 07:05:00', TIMESTAMP '2025-01-20 07:15:00'),
('C037', 'B002', 'P037', TIMESTAMP '2025-01-20 06:55:00', TIMESTAMP '2025-01-20 07:00:00'),
('C038', 'B003', 'P038', TIMESTAMP '2025-01-21 07:05:00', TIMESTAMP '2025-01-21 07:30:00'),
('C039', 'B004', 'P039', TIMESTAMP '2025-01-20 07:05:00', TIMESTAMP '2025-01-20 07:15:00'),
('C040', 'B005', 'P040', TIMESTAMP '2025-01-20 08:05:00', TIMESTAMP '2025-01-20 08:30:00'),
('C041', 'B006', 'P041', TIMESTAMP '2025-01-21 07:15:00', TIMESTAMP '2025-01-21 07:30:00'), -- Overlap Error potential?
('C042', 'B007', 'P042', TIMESTAMP '2025-01-21 07:05:00', TIMESTAMP '2025-01-21 07:15:00'),
('C043', 'B008', 'P043', TIMESTAMP '2025-01-21 08:05:00', TIMESTAMP '2025-01-21 08:15:00'),
('C044', 'B009', 'P044', TIMESTAMP '2025-01-22 07:05:00', TIMESTAMP '2025-01-22 07:30:00'),
('C045', 'B010', 'P045', TIMESTAMP '2025-01-22 07:05:00', TIMESTAMP '2025-01-22 07:30:00'),
('C046', 'B001', 'P046', TIMESTAMP '2025-01-20 11:50:00', TIMESTAMP '2025-01-20 12:20:00'),
('C047', 'B002', 'P047', TIMESTAMP '2025-01-20 12:05:00', TIMESTAMP '2025-01-20 12:25:00'),
('C048', 'B003', 'P048', TIMESTAMP '2025-01-21 10:35:00', TIMESTAMP '2025-01-21 10:55:00'),
('C049', 'B004', 'P049', TIMESTAMP '2025-01-20 12:05:00', TIMESTAMP '2025-01-20 12:30:00'),
('C050', 'B005', 'P050', TIMESTAMP '2025-01-20 13:05:00', TIMESTAMP '2025-01-20 13:25:00');

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
