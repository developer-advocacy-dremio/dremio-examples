/*
    Dremio High-Volume SQL Pattern: Government Permitting & Licensing
    
    Business Scenario:
    Tracking turnaround times for building permits and zoning requests.
    Identifying bottlenecks in "Approval Date" vs "Submit Date".
    
    Data Story:
    We track Permit Applications and Review Status.
    
    Medallion Architecture:
    - Bronze: Applications.
      *Volume*: 50+ records.
    - Silver: TurnaroundMetrics (Status & Duration).
    - Gold: ZonePerformance (Avg days to approve).
    
    Key Dremio Features:
    - Date Diff
    - Metrics Aggregation
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentPermitDB;
CREATE FOLDER IF NOT EXISTS GovernmentPermitDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentPermitDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentPermitDB.Gold;
USE GovernmentPermitDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentPermitDB.Bronze.Applications (
    AppID STRING,
    PermitType STRING, -- Residential, Commercial, Renovation
    SubmitDate DATE,
    ApprovalDate DATE, -- NULL if Pending
    ZoneCode STRING
);

INSERT INTO GovernmentPermitDB.Bronze.Applications VALUES
('A101', 'Residential', DATE '2025-01-01', DATE '2025-01-10', 'R1'),
('A102', 'Residential', DATE '2025-01-01', DATE '2025-01-05', 'R1'),
('A103', 'Commercial', DATE '2025-01-02', DATE '2025-01-25', 'C2'), -- Long
('A104', 'Renovation', DATE '2025-01-03', DATE '2025-01-04', 'R1'),
('A105', 'Commercial', DATE '2025-01-05', NULL, 'C1'), -- Pending
('A106', 'Residential', DATE '2025-01-06', DATE '2025-01-15', 'R2'),
('A107', 'Renovation', DATE '2025-01-07', DATE '2025-01-09', 'R2'),
('A108', 'Commercial', DATE '2025-01-10', DATE '2025-02-01', 'C2'),
('A109', 'Residential', DATE '2025-01-12', DATE '2025-01-18', 'R1'),
('A110', 'Renovation', DATE '2025-01-15', NULL, 'R3'), -- Pending
('A111', 'Commercial', DATE '2025-01-15', NULL, 'C1'),
('A112', 'Residential', DATE '2024-12-20', DATE '2024-12-25', 'R1'),
('A113', 'Commercial', DATE '2024-12-25', DATE '2025-01-05', 'C1'),
('A114', 'Renovation', DATE '2025-01-01', DATE '2025-01-02', 'R2'),
('A115', 'Residential', DATE '2025-01-02', NULL, 'R3'),
('A116', 'Commercial', DATE '2025-01-05', DATE '2025-01-20', 'C2'),
('A117', 'Residential', DATE '2025-01-08', DATE '2025-01-10', 'R1'),
('A118', 'Renovation', DATE '2025-01-10', DATE '2025-01-12', 'R1'),
('A119', 'Commercial', DATE '2025-01-12', NULL, 'C1'),
('A120', 'Residential', DATE '2025-01-14', DATE '2025-01-19', 'R2'),
('A121', 'Renovation', DATE '2025-01-01', DATE '2025-01-03', 'R3'),
('A122', 'Commercial', DATE '2025-01-03', DATE '2025-01-15', 'C2'),
('A123', 'Residential', DATE '2025-01-06', NULL, 'R1'),
('A124', 'Commercial', DATE '2025-01-08', DATE '2025-01-22', 'C1'),
('A125', 'Renovation', DATE '2025-01-10', DATE '2025-01-11', 'R2'),
('A126', 'Residential', DATE '2025-01-12', DATE '2025-01-16', 'R3'),
('A127', 'Commercial', DATE '2025-01-15', NULL, 'C2'),
('A128', 'Residential', DATE '2025-01-18', NULL, 'R1'),
('A129', 'Renovation', DATE '2025-01-19', DATE '2025-01-20', 'R1'),
('A130', 'Commercial', DATE '2024-12-15', DATE '2025-01-10', 'C1'), -- Long
('A131', 'Residential', DATE '2025-01-01', DATE '2025-01-08', 'R2'),
('A132', 'Commercial', DATE '2025-01-04', NULL, 'C2'),
('A133', 'Renovation', DATE '2025-01-06', DATE '2025-01-07', 'R3'),
('A134', 'Residential', DATE '2025-01-09', DATE '2025-01-14', 'R1'),
('A135', 'Commercial', DATE '2025-01-11', DATE '2025-01-25', 'C1'),
('A136', 'Renovation', DATE '2025-01-13', NULL, 'R2'),
('A137', 'Residential', DATE '2025-01-15', DATE '2025-01-18', 'R3'),
('A138', 'Commercial', DATE '2025-01-17', NULL, 'C2'),
('A139', 'Residential', DATE '2025-01-20', NULL, 'R1'),
('A140', 'Renovation', DATE '2024-12-30', DATE '2025-01-02', 'R1'),
('A141', 'Commercial', DATE '2024-12-31', DATE '2025-01-15', 'C1'),
('A142', 'Residential', DATE '2025-01-02', DATE '2025-01-06', 'R2'),
('A143', 'Commercial', DATE '2025-01-05', NULL, 'C2'),
('A144', 'Renovation', DATE '2025-01-07', DATE '2025-01-08', 'R3'),
('A145', 'Residential', DATE '2025-01-10', DATE '2025-01-12', 'R1'),
('A146', 'Commercial', DATE '2025-01-12', DATE '2025-01-28', 'C1'),
('A147', 'Renovation', DATE '2025-01-14', NULL, 'R2'),
('A148', 'Residential', DATE '2025-01-16', DATE '2025-01-19', 'R3'),
('A149', 'Commercial', DATE '2025-01-18', NULL, 'C2'),
('A150', 'Residential', DATE '2025-01-19', NULL, 'R1');


-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Turnaround Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentPermitDB.Silver.TurnaroundMetrics AS
SELECT 
    AppID,
    PermitType,
    ZoneCode,
    CASE 
        WHEN ApprovalDate IS NULL THEN 'Pending'
        ELSE 'Approved'
    END AS Status,
    TIMESTAMPDIFF(DAY, SubmitDate, COALESCE(ApprovalDate, DATE '2025-01-20')) AS DaysActive
FROM GovernmentPermitDB.Bronze.Applications;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Zone Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentPermitDB.Gold.ZonePerformance AS
SELECT 
    ZoneCode,
    COUNT(*) AS TotalApps,
    AVG(DaysActive) AS AvgApprovalDays,
    SUM(CASE WHEN Status = 'Pending' THEN 1 ELSE 0 END) AS PendingCount
FROM GovernmentPermitDB.Silver.TurnaroundMetrics
GROUP BY ZoneCode;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Rank zones by AvgApprovalDays ascending."
    2. "Count pending applications by Zone."
    3. "Which Permit Type takes the longest to approve?"
*/
