/*
    Dremio High-Volume SQL Pattern: Government Public Records (FOIA)
    
    Business Scenario:
    Agencies are legally mandated to respond to Freedom of Information Act (FOIA) requests 
    within a set time. Tracking "Backlog" and "Processing Time" ensures transparency compliance.
    
    Data Story:
    We track Requests and Workflow Events.
    
    Medallion Architecture:
    - Bronze: FOIARequests, Depts.
      *Volume*: 50+ records.
    - Silver: AgingReport (Days Open).
    - Gold: ComplianceDashboard (Statutory violations).
    
    Key Dremio Features:
    - TIMESTAMPDIFF for aging
    - Status filtering
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentFOIADB;
CREATE FOLDER IF NOT EXISTS GovernmentFOIADB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentFOIADB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentFOIADB.Gold;
USE GovernmentFOIADB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentFOIADB.Bronze.FOIARequests (
    RequestID STRING,
    RequesterType STRING, -- Media, Citizen, Legal
    SubmitDate DATE,
    Status STRING, -- New, In Progress, Closed
    Department STRING
);

-- Bulk Requests (50 Records)
INSERT INTO GovernmentFOIADB.Bronze.FOIARequests VALUES
('FOIA1001', 'Media', DATE '2024-11-01', 'Closed', 'Mayor Office'),
('FOIA1002', 'Citizen', DATE '2024-12-01', 'In Progress', 'Police'), -- Overdue
('FOIA1003', 'Legal', DATE '2024-12-15', 'Closed', 'Health'),
('FOIA1004', 'Media', DATE '2025-01-01', 'New', 'Police'),
('FOIA1005', 'Citizen', DATE '2025-01-05', 'In Progress', 'Mayor Office'),
('FOIA1006', 'Legal', DATE '2025-01-10', 'New', 'Transportation'),
('FOIA1007', 'Media', DATE '2024-11-15', 'In Progress', 'Police'), -- Very Overdue
('FOIA1008', 'Citizen', DATE '2025-01-12', 'Closed', 'Housing'),
('FOIA1009', 'Legal', DATE '2025-01-15', 'New', 'Health'),
('FOIA1010', 'Media', DATE '2024-10-01', 'In Progress', 'Mayor Office'), -- Extremely Overdue
('FOIA1011', 'Citizen', DATE '2025-01-18', 'New', 'Police'),
('FOIA1012', 'Legal', DATE '2025-01-18', 'New', 'Transportation'),
('FOIA1013', 'Media', DATE '2025-01-19', 'New', 'Housing'),
('FOIA1014', 'Citizen', DATE '2024-12-20', 'Closed', 'Police'),
('FOIA1015', 'Legal', DATE '2024-12-25', 'In Progress', 'Education'), -- Overdue
('FOIA1016', 'Media', DATE '2025-01-02', 'In Progress', 'Mayor Office'),
('FOIA1017', 'Citizen', DATE '2025-01-03', 'Closed', 'Education'),
('FOIA1018', 'Legal', DATE '2025-01-04', 'Closed', 'Health'),
('FOIA1019', 'Media', DATE '2025-01-06', 'New', 'Police'),
('FOIA1020', 'Citizen', DATE '2025-01-07', 'New', 'Education'),
('FOIA1021', 'Legal', DATE '2025-01-08', 'In Progress', 'Mayor Office'),
('FOIA1022', 'Media', DATE '2025-01-09', 'In Progress', 'Transportation'),
('FOIA1023', 'Citizen', DATE '2025-01-11', 'Closed', 'Police'),
('FOIA1024', 'Legal', DATE '2025-01-13', 'New', 'Housing'),
('FOIA1025', 'Media', DATE '2025-01-14', 'New', 'Health'),
('FOIA1026', 'Citizen', DATE '2024-11-20', 'In Progress', 'Mayor Office'), -- Overdue
('FOIA1027', 'Legal', DATE '2024-12-05', 'Closed', 'Police'),
('FOIA1028', 'Media', DATE '2024-12-10', 'Closed', 'Education'),
('FOIA1029', 'Citizen', DATE '2025-01-16', 'New', 'Police'),
('FOIA1030', 'Legal', DATE '2025-01-17', 'New', 'Mayor Office'),
('FOIA1031', 'Media', DATE '2025-01-18', 'In Progress', 'Housing'),
('FOIA1032', 'Citizen', DATE '2025-01-20', 'New', 'Health'),
('FOIA1033', 'Legal', DATE '2025-01-20', 'New', 'Transportation'),
('FOIA1034', 'Media', DATE '2024-12-12', 'In Progress', 'Police'), -- Overdue
('FOIA1035', 'Citizen', DATE '2025-01-01', 'Closed', 'Education'),
('FOIA1036', 'Legal', DATE '2025-01-05', 'Closed', 'Mayor Office'),
('FOIA1037', 'Media', DATE '2025-01-10', 'In Progress', 'Health'),
('FOIA1038', 'Citizen', DATE '2025-01-15', 'New', 'Housing'),
('FOIA1039', 'Legal', DATE '2025-01-19', 'New', 'Education'),
('FOIA1040', 'Media', DATE '2024-11-05', 'Closed', 'Police'),
('FOIA1041', 'Citizen', DATE '2024-11-25', 'In Progress', 'Mayor Office'), -- Overdue
('FOIA1042', 'Legal', DATE '2024-12-15', 'Closed', 'Transportation'),
('FOIA1043', 'Media', DATE '2025-01-08', 'New', 'Police'),
('FOIA1044', 'Citizen', DATE '2025-01-12', 'In Progress', 'Health'),
('FOIA1045', 'Legal', DATE '2025-01-18', 'In Progress', 'Mayor Office'),
('FOIA1046', 'Media', DATE '2025-01-20', 'New', 'Education'),
('FOIA1047', 'Citizen', DATE '2024-12-30', 'Closed', 'Police'),
('FOIA1048', 'Legal', DATE '2025-01-02', 'Closed', 'Housing'),
('FOIA1049', 'Media', DATE '2025-01-04', 'In Progress', 'Transportation'),
('FOIA1050', 'Citizen', DATE '2025-01-06', 'New', 'Mayor Office');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Aging
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentFOIADB.Silver.RequestAging AS
SELECT
    RequestID,
    RequesterType,
    Department,
    SubmitDate,
    Status,
    TIMESTAMPDIFF(DAY, SubmitDate, DATE '2025-01-20') AS DaysOpen
FROM GovernmentFOIADB.Bronze.FOIARequests
WHERE Status != 'Closed';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Backlog Metrics
-------------------------------------------------------------------------------
-- Statutory limit often 20 days
CREATE OR REPLACE VIEW GovernmentFOIADB.Gold.BacklogAnalysis AS
SELECT
    Department,
    COUNT(*) AS TotalOpenRequests,
    SUM(CASE WHEN DaysOpen > 20 THEN 1 ELSE 0 END) AS OverdueRequests,
    AVG(DaysOpen) AS AvgWaitTime
FROM GovernmentFOIADB.Silver.RequestAging
GROUP BY Department;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "How many requests are overdue for the Mayor Office?"
    2. "Shows average wait time by Department."
    3. "List all open Media requests."
*/
