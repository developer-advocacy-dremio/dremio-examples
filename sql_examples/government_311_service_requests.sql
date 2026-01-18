/*
    Dremio High-Volume SQL Pattern: Government 311 Service Requests
    
    Business Scenario:
    Cities use "311" systems to track non-emergency citizen requests (potholes, graffiti, noise).
    Tracking "SLA Compliance" (Service Level Agreement) is key to measuring agency performance.
    
    Data Story:
    We have specific Service Requests and Agency Connectors.
    
    Medallion Architecture:
    - Bronze: ServiceRequests, Agencies.
      *Volume*: 50+ records.
    - Silver: RequestAging (Time to close).
    - Gold: PerformanceDashboard (SLA Breach Rates).
    
    Key Dremio Features:
    - TIMESTAMPDIFF for aging
    - CASE logic for SLAs
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS Government311DB;
CREATE FOLDER IF NOT EXISTS Government311DB.Bronze;
CREATE FOLDER IF NOT EXISTS Government311DB.Silver;
CREATE FOLDER IF NOT EXISTS Government311DB.Gold;
USE Government311DB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE Government311DB.Bronze.Agencies (
    AgencyID STRING,
    Name STRING,
    Department_Head STRING
);

INSERT INTO Government311DB.Bronze.Agencies VALUES
('DOT', 'Department of Transportation', 'Commissioner X'),
('DEP', 'Department of Environmental Protection', 'Commissioner Y'),
('DSNY', 'Department of Sanitation', 'Commissioner Z'),
('DOH', 'Department of Health', 'Commissioner A');

CREATE OR REPLACE TABLE Government311DB.Bronze.ServiceRequests (
    RequestID STRING,
    AgencyID STRING,
    Type STRING, -- Pothole, Noise, Missed Collection
    CreatedDate TIMESTAMP,
    ClosedDate TIMESTAMP, -- NULL if open
    ZipCode STRING
);

-- Bulk Requests (50 Records)
INSERT INTO Government311DB.Bronze.ServiceRequests VALUES
('SR1001', 'DOT', 'Pothole', TIMESTAMP '2025-01-10 10:00:00', TIMESTAMP '2025-01-12 14:00:00', '10001'), -- Closed in 2 days (OK)
('SR1002', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-10 22:00:00', TIMESTAMP '2025-01-11 23:00:00', '10002'), -- Closed in 1 day (OK)
('SR1003', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-11 08:00:00', NULL, '10001'), -- Open > 2 days (Breach if current date > 1/13)
('SR1004', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-11 09:00:00', NULL, '10003'),
('SR1005', 'DOT', 'Pothole', TIMESTAMP '2025-01-12 15:00:00', NULL, '10001'), -- Open
('SR1006', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-13 01:00:00', TIMESTAMP '2025-01-16 02:00:00', '10002'), -- Closed in 3 days (Breach)
('SR1007', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-13 07:00:00', TIMESTAMP '2025-01-14 09:00:00', '10004'),
('SR1008', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-14 10:00:00', NULL, '10005'),
('SR1009', 'DOT', 'Pothole', TIMESTAMP '2025-01-14 12:00:00', TIMESTAMP '2025-01-19 12:00:00', '10001'), -- Closed in 5 days (Breach)
('SR1010', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-15 23:00:00', NULL, '10002'),
('SR1011', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-15 08:30:00', TIMESTAMP '2025-01-15 14:00:00', '10003'),
('SR1012', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-15 09:00:00', NULL, '10004'),
('SR1013', 'DOT', 'Pothole', TIMESTAMP '2025-01-16 10:00:00', NULL, '10005'),
('SR1014', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-16 21:00:00', TIMESTAMP '2025-01-17 08:00:00', '10006'),
('SR1015', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-16 07:15:00', NULL, '10001'),
('SR1016', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-17 11:00:00', TIMESTAMP '2025-01-20 09:00:00', '10002'),
('SR1017', 'DOT', 'Pothole', TIMESTAMP '2025-01-17 13:00:00', NULL, '10003'),
('SR1018', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-17 22:30:00', NULL, '10004'),
('SR1019', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-18 06:00:00', TIMESTAMP '2025-01-18 10:00:00', '10005'),
('SR1020', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-18 10:00:00', NULL, '10006'),
('SR1021', 'DOT', 'Pothole', TIMESTAMP '2025-01-18 14:00:00', NULL, '10001'),
('SR1022', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-19 02:00:00', TIMESTAMP '2025-01-19 03:00:00', '10002'),
('SR1023', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-19 07:00:00', NULL, '10003'),
('SR1024', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-19 09:00:00', NULL, '10004'),
('SR1025', 'DOT', 'Pothole', TIMESTAMP '2025-01-19 16:00:00', NULL, '10005'),
('SR1026', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-20 01:00:00', NULL, '10006'),
('SR1027', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-20 07:30:00', NULL, '10001'),
('SR1028', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-20 10:30:00', NULL, '10002'),
('SR1029', 'DOT', 'Pothole', TIMESTAMP '2025-01-10 10:00:00', TIMESTAMP '2025-01-15 10:00:00', '10003'), -- Closed (Breach)
('SR1030', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-11 23:00:00', NULL, '10004'),
('SR1031', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-12 08:00:00', TIMESTAMP '2025-01-13 08:00:00', '10005'),
('SR1032', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-12 09:00:00', NULL, '10006'),
('SR1033', 'DOT', 'Pothole', TIMESTAMP '2025-01-13 14:00:00', TIMESTAMP '2025-01-14 14:00:00', '10001'),
('SR1034', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-13 11:00:00', NULL, '10002'),
('SR1035', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-14 06:00:00', TIMESTAMP '2025-01-14 12:00:00', '10003'),
('SR1036', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-14 15:00:00', NULL, '10004'),
('SR1037', 'DOT', 'Pothole', TIMESTAMP '2025-01-15 10:00:00', NULL, '10005'),
('SR1038', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-15 22:00:00', TIMESTAMP '2025-01-16 08:00:00', '10006'),
('SR1039', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-16 08:00:00', NULL, '10001'),
('SR1040', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-16 11:00:00', NULL, '10002'),
('SR1041', 'DOT', 'Pothole', TIMESTAMP '2025-01-16 13:00:00', TIMESTAMP '2025-01-18 13:00:00', '10003'),
('SR1042', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-17 00:00:00', NULL, '10004'),
('SR1043', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-17 07:00:00', NULL, '10005'),
('SR1044', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-18 09:30:00', NULL, '10006'),
('SR1045', 'DOT', 'Pothole', TIMESTAMP '2025-01-18 15:00:00', NULL, '10001'),
('SR1046', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-19 23:00:00', NULL, '10002'),
('SR1047', 'DSNY', 'Missed Collection', TIMESTAMP '2025-01-19 08:00:00', NULL, '10003'),
('SR1048', 'DOH', 'Rodent Sighting', TIMESTAMP '2025-01-20 12:00:00', NULL, '10004'),
('SR1049', 'DOT', 'Pothole', TIMESTAMP '2025-01-20 09:00:00', NULL, '10005'),
('SR1050', 'DEP', 'Noise Complaint', TIMESTAMP '2025-01-20 02:00:00', NULL, '10006');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Aging & SLA Logic
-------------------------------------------------------------------------------
-- SLA Rules: Pothole (3 days), Noise (1 day), Trash (2 days), Rodent (7 days)
CREATE OR REPLACE VIEW Government311DB.Silver.RequestPerformance AS
SELECT
    r.RequestID,
    r.AgencyID,
    r.Type,
    r.CreatedDate,
    r.ClosedDate,
    TIMESTAMPDIFF(DAY, r.CreatedDate, COALESCE(r.ClosedDate, TIMESTAMP '2025-01-20 00:00:00')) AS DaysOpen,
    CASE 
        WHEN r.Type = 'Pothole' AND TIMESTAMPDIFF(DAY, r.CreatedDate, COALESCE(r.ClosedDate, TIMESTAMP '2025-01-20 00:00:00')) > 3 THEN 1
        WHEN r.Type = 'Noise Complaint' AND TIMESTAMPDIFF(DAY, r.CreatedDate, COALESCE(r.ClosedDate, TIMESTAMP '2025-01-20 00:00:00')) > 1 THEN 1
        WHEN r.Type = 'Missed Collection' AND TIMESTAMPDIFF(DAY, r.CreatedDate, COALESCE(r.ClosedDate, TIMESTAMP '2025-01-20 00:00:00')) > 2 THEN 1
        ELSE 0
    END AS SLA_Breach_Flag
FROM Government311DB.Bronze.ServiceRequests r;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Agency Performance Dashboard
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW Government311DB.Gold.AgencyScorecard AS
SELECT
    a.Name AS AgencyName,
    p.Type,
    COUNT(p.RequestID) AS TotalRequests,
    SUM(p.SLA_Breach_Flag) AS BreachedRequests,
    AVG(p.DaysOpen) AS AvgResolutionDays,
    (1.0 - (CAST(SUM(p.SLA_Breach_Flag) AS DOUBLE) / COUNT(p.RequestID))) * 100 AS SLA_Compliance_Pct
FROM Government311DB.Silver.RequestPerformance p
JOIN Government311DB.Bronze.Agencies a ON p.AgencyID = a.AgencyID
GROUP BY a.Name, p.Type;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which agency has the lowest SLA Compliance percentage?"
    2. "Show the average resolution time for Potholes."
    3. "List all open complaints that have breached their SLA."
*/
