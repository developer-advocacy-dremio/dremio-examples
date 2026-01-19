/*
    Dremio High-Volume SQL Pattern: Government Social Services & Benefits
    
    Business Scenario:
    Tracking benefit application processing times and approval rates.
    Ensuring compliance with statutory processing windows (e.g., 30 days for SNAP).
    
    Data Story:
    We track Application Submissions and Decisions.
    
    Medallion Architecture:
    - Bronze: Applications.
      *Volume*: 50+ records.
    - Silver: ProcessingTimes (Duration).
    - Gold: ProgramEfficiency (Avg Time & Backlog).
    
    Key Dremio Features:
    - Date Diff
    - Coalesce
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentSocialDB;
CREATE FOLDER IF NOT EXISTS GovernmentSocialDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentSocialDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentSocialDB.Gold;
USE GovernmentSocialDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentSocialDB.Bronze.Applications (
    AppID STRING,
    Program STRING, -- SNAP, Unemployment, Housing
    ApplicantID STRING,
    SubmitDate DATE,
    DecisionDate DATE,
    Status STRING -- Approved, Denied, Pending
);

INSERT INTO GovernmentSocialDB.Bronze.Applications VALUES
('APP001', 'SNAP', 'U101', DATE '2025-01-01', DATE '2025-01-05', 'Approved'),
('APP002', 'SNAP', 'U102', DATE '2025-01-01', DATE '2025-01-10', 'Approved'),
('APP003', 'Unemployment', 'U103', DATE '2025-01-02', DATE '2025-01-03', 'Denied'),
('APP004', 'Housing', 'U104', DATE '2025-01-02', DATE '2025-01-20', 'Approved'), -- Slow
('APP005', 'SNAP', 'U105', DATE '2025-01-03', DATE '2025-01-06', 'Denied'),
('APP006', 'Unemployment', 'U106', DATE '2025-01-03', DATE '2025-01-15', 'Approved'),
('APP007', 'Housing', 'U107', DATE '2025-01-04', NULL, 'Pending'),
('APP008', 'SNAP', 'U108', DATE '2025-01-05', DATE '2025-01-08', 'Approved'),
('APP009', 'Unemployment', 'U109', DATE '2025-01-06', DATE '2025-01-09', 'Approved'),
('APP010', 'Housing', 'U110', DATE '2025-01-07', NULL, 'Pending'),
('APP011', 'SNAP', 'U111', DATE '2025-01-08', DATE '2025-01-12', 'Denied'),
('APP012', 'Unemployment', 'U112', DATE '2025-01-09', DATE '2025-01-10', 'Approved'),
('APP013', 'Housing', 'U113', DATE '2025-01-10', NULL, 'Pending'),
('APP014', 'SNAP', 'U114', DATE '2025-01-11', DATE '2025-01-15', 'Approved'),
('APP015', 'Unemployment', 'U115', DATE '2025-01-12', NULL, 'Pending'),
('APP016', 'Housing', 'U116', DATE '2025-01-13', DATE '2025-01-25', 'Approved'),
('APP017', 'SNAP', 'U117', DATE '2025-01-14', DATE '2025-01-18', 'Approved'),
('APP018', 'Unemployment', 'U118', DATE '2025-01-15', DATE '2025-01-16', 'Denied'),
('APP019', 'Housing', 'U119', DATE '2025-01-16', NULL, 'Pending'),
('APP020', 'SNAP', 'U120', DATE '2025-01-17', NULL, 'Pending'),
('APP021', 'Unemployment', 'U121', DATE '2025-01-18', DATE '2025-01-20', 'Approved'),
('APP022', 'Housing', 'U122', DATE '2025-01-19', NULL, 'Pending'),
('APP023', 'SNAP', 'U123', DATE '2025-01-20', NULL, 'Pending'),
('APP024', 'Unemployment', 'U124', DATE '2025-01-01', DATE '2025-01-05', 'Approved'),
('APP025', 'Housing', 'U125', DATE '2025-01-02', DATE '2025-01-18', 'Denied'),
('APP026', 'SNAP', 'U126', DATE '2025-01-03', DATE '2025-01-07', 'Approved'),
('APP027', 'Unemployment', 'U127', DATE '2025-01-04', DATE '2025-01-06', 'Approved'),
('APP028', 'Housing', 'U128', DATE '2025-01-05', NULL, 'Pending'),
('APP029', 'SNAP', 'U129', DATE '2025-01-06', DATE '2025-01-11', 'Denied'),
('APP030', 'Unemployment', 'U130', DATE '2025-01-07', DATE '2025-01-08', 'Approved'),
('APP031', 'Housing', 'U131', DATE '2025-01-08', DATE '2025-01-28', 'Approved'),
('APP032', 'SNAP', 'U132', DATE '2025-01-09', DATE '2025-01-13', 'Approved'),
('APP033', 'Unemployment', 'U133', DATE '2025-01-10', NULL, 'Pending'),
('APP034', 'Housing', 'U134', DATE '2025-01-11', NULL, 'Pending'),
('APP035', 'SNAP', 'U135', DATE '2025-01-12', DATE '2025-01-15', 'Approved'),
('APP036', 'Unemployment', 'U136', DATE '2025-01-13', DATE '2025-01-18', 'Denied'),
('APP037', 'Housing', 'U137', DATE '2025-01-14', NULL, 'Pending'),
('APP038', 'SNAP', 'U138', DATE '2025-01-15', NULL, 'Pending'),
('APP039', 'Unemployment', 'U139', DATE '2025-01-16', DATE '2025-01-19', 'Approved'),
('APP040', 'Housing', 'U140', DATE '2025-01-17', NULL, 'Pending'),
('APP041', 'SNAP', 'U141', DATE '2025-01-18', DATE '2025-01-22', 'Denied'),
('APP042', 'Unemployment', 'U142', DATE '2025-01-19', DATE '2025-01-20', 'Approved'),
('APP043', 'Housing', 'U143', DATE '2025-01-20', NULL, 'Pending'),
('APP044', 'SNAP', 'U144', DATE '2025-01-01', DATE '2025-01-09', 'Approved'),
('APP045', 'Unemployment', 'U145', DATE '2025-01-02', DATE '2025-01-04', 'Approved'),
('APP046', 'Housing', 'U146', DATE '2025-01-03', DATE '2025-01-25', 'Approved'),
('APP047', 'SNAP', 'U147', DATE '2025-01-04', DATE '2025-01-06', 'Denied'),
('APP048', 'Unemployment', 'U148', DATE '2025-01-05', NULL, 'Pending'),
('APP049', 'Housing', 'U149', DATE '2025-01-06', NULL, 'Pending'),
('APP050', 'SNAP', 'U150', DATE '2025-01-07', DATE '2025-01-11', 'Approved');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Processing Times
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSocialDB.Silver.ProcessingTimes AS
SELECT 
    AppID,
    Program,
    Status,
    COALESCE(DecisionDate, DATE '2025-01-20') AS CalcEndDate,
    TIMESTAMPDIFF(DAY, SubmitDate, COALESCE(DecisionDate, DATE '2025-01-20')) AS DaysInSystem
FROM GovernmentSocialDB.Bronze.Applications;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Program Efficiency
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentSocialDB.Gold.ProgramEfficiency AS
SELECT 
    Program,
    COUNT(*) AS TotalApps,
    AVG(DaysInSystem) AS AvgProcessingDays,
    SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) AS BacklogCount
FROM GovernmentSocialDB.Silver.ProcessingTimes
GROUP BY Program;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Program has the highest BacklogCount?"
    2. "Show average processing days by Program."
    3. "List all Pending Housing applications."
*/
