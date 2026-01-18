/*
    Dremio High-Volume SQL Pattern: Government Court Case Management
    
    Business Scenario:
    Analyzing court docket backlogs, case processing frequency, and case aging.
    Identifies bottlenecks in the judicial system.
    
    Data Story:
    We track Court Cases and Judicial Assignments.
    
    Medallion Architecture:
    - Bronze: Cases.
      *Volume*: 50+ records.
    - Silver: CaseAging (Days Open).
    - Gold: DocketBacklog (Backlog by Type).
    
    Key Dremio Features:
    - Date Diff
    - Conditional Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentCourtDB;
CREATE FOLDER IF NOT EXISTS GovernmentCourtDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentCourtDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentCourtDB.Gold;
USE GovernmentCourtDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentCourtDB.Bronze.Cases (
    CaseID STRING,
    Type STRING, -- Civil, Criminal, Traffic
    FileDate DATE,
    CloseDate DATE, -- NULL if Open
    JudgeID STRING
);

INSERT INTO GovernmentCourtDB.Bronze.Cases VALUES
('C101', 'Traffic', DATE '2024-11-01', DATE '2024-12-01', 'J1'),
('C102', 'Traffic', DATE '2024-11-15', DATE '2024-11-15', 'J2'),
('C103', 'Civil', DATE '2024-06-01', DATE '2025-01-10', 'J1'),
('C104', 'Criminal', DATE '2024-08-01', NULL, 'J3'),
('C105', 'Civil', DATE '2024-09-01', NULL, 'J2'),
('C106', 'Traffic', DATE '2025-01-01', DATE '2025-01-15', 'J1'),
('C107', 'Criminal', DATE '2025-01-05', NULL, 'J3'),
('C108', 'Traffic', DATE '2025-01-10', DATE '2025-01-10', 'J1'),
('C109', 'Civil', DATE '2025-01-12', NULL, 'J4'),
('C110', 'Criminal', DATE '2024-05-01', DATE '2025-01-01', 'J3'),
('C111', 'Traffic', DATE '2025-01-15', NULL, 'J2'),
('C112', 'Traffic', DATE '2024-12-01', DATE '2024-12-05', 'J1'),
('C113', 'Civil', DATE '2024-07-01', NULL, 'J4'),
('C114', 'Criminal', DATE '2024-09-15', NULL, 'J3'),
('C115', 'Traffic', DATE '2025-01-02', DATE '2025-01-02', 'J2'),
('C116', 'Civil', DATE '2024-10-01', DATE '2025-01-05', 'J1'),
('C117', 'Criminal', DATE '2024-11-01', NULL, 'J3'),
('C118', 'Traffic', DATE '2025-01-18', NULL, 'J2'),
('C119', 'Civil', DATE '2024-11-15', NULL, 'J4'),
('C120', 'Traffic', DATE '2025-01-20', DATE '2025-01-20', 'J1'),
('C121', 'Criminal', DATE '2024-06-01', DATE '2025-01-10', 'J3'),
('C122', 'Traffic', DATE '2024-12-10', DATE '2024-12-11', 'J2'),
('C123', 'Civil', DATE '2024-08-15', NULL, 'J1'),
('C124', 'Criminal', DATE '2024-10-10', NULL, 'J3'),
('C125', 'Traffic', DATE '2025-01-05', DATE '2025-01-06', 'J1'),
('C126', 'Civil', DATE '2024-12-01', NULL, 'J4'),
('C127', 'Traffic', DATE '2025-01-08', NULL, 'J2'),
('C128', 'Criminal', DATE '2024-11-20', NULL, 'J3'),
('C129', 'Civil', DATE '2024-05-15', DATE '2025-01-01', 'J1'),
('C130', 'Traffic', DATE '2025-01-12', DATE '2025-01-12', 'J2'),
('C131', 'Criminal', DATE '2024-12-05', NULL, 'J3'),
('C132', 'Traffic', DATE '2025-01-14', NULL, 'J1'),
('C133', 'Civil', DATE '2025-01-01', NULL, 'J4'),
('C134', 'Traffic', DATE '2024-12-20', DATE '2024-12-21', 'J2'),
('C135', 'Criminal', DATE '2024-08-01', DATE '2025-01-15', 'J3'),
('C136', 'Civil', DATE '2024-09-10', NULL, 'J1'),
('C137', 'Traffic', DATE '2025-01-16', NULL, 'J2'),
('C138', 'Criminal', DATE '2024-10-01', NULL, 'J3'),
('C139', 'Traffic', DATE '2025-01-01', DATE '2025-01-02', 'J1'),
('C140', 'Civil', DATE '2024-11-05', NULL, 'J4'),
('C141', 'Traffic', DATE '2025-01-03', NULL, 'J2'),
('C142', 'Criminal', DATE '2024-12-15', NULL, 'J3'),
('C143', 'Civil', DATE '2024-06-20', DATE '2025-01-05', 'J1'),
('C144', 'Traffic', DATE '2025-01-18', DATE '2025-01-18', 'J1'),
('C145', 'Criminal', DATE '2025-01-01', NULL, 'J3'),
('C146', 'Civil', DATE '2024-10-15', NULL, 'J4'),
('C147', 'Traffic', DATE '2025-01-19', NULL, 'J2'),
('C148', 'Criminal', DATE '2024-07-01', DATE '2025-01-01', 'J3'),
('C149', 'Traffic', DATE '2025-01-20', NULL, 'J1'),
('C150', 'Civil', DATE '2024-12-10', NULL, 'J4');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Case Aging
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCourtDB.Silver.CaseAging AS
SELECT 
    CaseID,
    Type,
    JudgeID,
    FileDate,
    CloseDate,
    CASE 
        WHEN CloseDate IS NULL THEN 'Open' 
        ELSE 'Closed' 
    END AS Status,
    TIMESTAMPDIFF(DAY, FileDate, COALESCE(CloseDate, DATE '2025-01-20')) AS AgeDays
FROM GovernmentCourtDB.Bronze.Cases;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Docket Backlog
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentCourtDB.Gold.DocketBacklog AS
SELECT 
    Type,
    COUNT(*) AS TotalCases,
    SUM(CASE WHEN Status = 'Open' THEN 1 ELSE 0 END) AS OpenCases,
    AVG(AgeDays) AS AvgCaseDuration
FROM GovernmentCourtDB.Silver.CaseAging
GROUP BY Type;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which case Type has the highest average duration?"
    2. "Count open cases by Type."
    3. "Show me the oldest open Civil cases."
*/
