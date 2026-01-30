/*
 * Cybersecurity: Phishing Simulation Results
 * 
 * Scenario:
 * Analyzing employee responses to phishing simulation campaigns to identify high-risk departments.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CyberSecDB;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Training;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Training.Bronze;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Training.Silver;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Training.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Campaign Data
-------------------------------------------------------------------------------

-- Campaigns Table
CREATE TABLE IF NOT EXISTS CyberSecDB.Training.Bronze.SimCampaigns (
    CampaignID VARCHAR,
    CampaignName VARCHAR,
    LaunchDate DATE,
    TemplateType VARCHAR -- Credential Harvest, Attachment, Link
);

INSERT INTO CyberSecDB.Training.Bronze.SimCampaigns VALUES
('CAMP-001', 'Q1-Payroll-Update', '2025-01-15', 'Link'),
('CAMP-002', 'Q1-Urgent-Invoice', '2025-02-10', 'Attachment'),
('CAMP-003', 'Q2-Password-Reset', '2025-04-05', 'Credential Harvest'),
('CAMP-004', 'Q2-Free-Gift-Card', '2025-05-20', 'Link'),
('CAMP-005', 'Q3-CEO-Request', '2025-07-12', 'Link');

-- UserActions Table
CREATE TABLE IF NOT EXISTS CyberSecDB.Training.Bronze.UserActions (
    ActionID INT,
    CampaignID VARCHAR,
    EmployeeEmail VARCHAR,
    Department VARCHAR,
    ActionType VARCHAR, -- Sent, Opened, Clicked, Reported, EnteredCreds
    Timestamp TIMESTAMP
);

INSERT INTO CyberSecDB.Training.Bronze.UserActions VALUES
(1, 'CAMP-001', 'alice@company.com', 'HR', 'Sent', '2025-01-15 09:00:00'),
(2, 'CAMP-001', 'alice@company.com', 'HR', 'Opened', '2025-01-15 09:05:00'),
(3, 'CAMP-001', 'alice@company.com', 'HR', 'Reported', '2025-01-15 09:06:00'), -- Good
(4, 'CAMP-001', 'bob@company.com', 'Sales', 'Sent', '2025-01-15 09:00:00'),
(5, 'CAMP-001', 'bob@company.com', 'Sales', 'Opened', '2025-01-15 09:10:00'),
(6, 'CAMP-001', 'bob@company.com', 'Sales', 'Clicked', '2025-01-15 09:11:00'), -- Fail
(7, 'CAMP-001', 'charlie@company.com', 'IT', 'Sent', '2025-01-15 09:00:00'),
(8, 'CAMP-001', 'charlie@company.com', 'IT', 'Reported', '2025-01-15 09:01:00'), -- Good (no open)
(9, 'CAMP-002', 'alice@company.com', 'HR', 'Sent', '2025-02-10 09:00:00'),
(10, 'CAMP-002', 'alice@company.com', 'HR', 'Opened', '2025-02-10 10:00:00'), -- No click
(11, 'CAMP-002', 'bob@company.com', 'Sales', 'Sent', '2025-02-10 09:00:00'),
(12, 'CAMP-002', 'bob@company.com', 'Sales', 'Clicked', '2025-02-10 09:15:00'), -- Fail again
(13, 'CAMP-002', 'dave@company.com', 'Finance', 'Sent', '2025-02-10 09:00:00'),
(14, 'CAMP-002', 'dave@company.com', 'Finance', 'Opened', '2025-02-10 09:30:00'),
(15, 'CAMP-002', 'dave@company.com', 'Finance', 'Clicked', '2025-02-10 09:31:00'), -- Fail
(16, 'CAMP-003', 'eve@company.com', 'Marketing', 'Sent', '2025-04-05 09:00:00'),
(17, 'CAMP-003', 'eve@company.com', 'Marketing', 'EnteredCreds', '2025-04-05 09:20:00'), -- Critical Fail
(18, 'CAMP-003', 'frank@company.com', 'Sales', 'Sent', '2025-04-05 09:00:00'),
(19, 'CAMP-003', 'frank@company.com', 'Sales', 'Reported', '2025-04-05 09:05:00'),
(20, 'CAMP-001', 'grace@company.com', 'Engineering', 'Sent', '2025-01-15 09:00:00'),
(21, 'CAMP-001', 'grace@company.com', 'Engineering', 'Reported', '2025-01-15 09:10:00'),
(22, 'CAMP-004', 'bob@company.com', 'Sales', 'Sent', '2025-05-20 09:00:00'),
(23, 'CAMP-004', 'bob@company.com', 'Sales', 'Reported', '2025-05-20 09:10:00'), -- Bob improved!
(24, 'CAMP-004', 'alice@company.com', 'HR', 'Sent', '2025-05-20 09:00:00'),
(25, 'CAMP-004', 'alice@company.com', 'HR', 'Clicked', '2025-05-20 09:30:00'), -- Alice slipped
(26, 'CAMP-005', 'harry@company.com', 'Legal', 'Sent', '2025-07-12 09:00:00'),
(27, 'CAMP-005', 'harry@company.com', 'Legal', 'Reported', '2025-07-12 09:05:00'),
(28, 'CAMP-005', 'ivan@company.com', 'Legal', 'Sent', '2025-07-12 09:00:00'),
(29, 'CAMP-005', 'ivan@company.com', 'Legal', 'Opened', '2025-07-12 09:10:00'),
(30, 'CAMP-001', 'jack@company.com', 'Sales', 'Sent', '2025-01-15 09:00:00'),
(31, 'CAMP-001', 'jack@company.com', 'Sales', 'Clicked', '2025-01-15 09:20:00'),
(32, 'CAMP-002', 'karen@company.com', 'Marketing', 'Sent', '2025-02-10 09:00:00'),
(33, 'CAMP-002', 'karen@company.com', 'Marketing', 'Clicked', '2025-02-10 09:15:00'),
(34, 'CAMP-003', 'larry@company.com', 'IT', 'Sent', '2025-04-05 09:00:00'),
(35, 'CAMP-003', 'larry@company.com', 'IT', 'Reported', '2025-04-05 09:02:00'),
(36, 'CAMP-004', 'mike@company.com', 'Finance', 'Sent', '2025-05-20 09:00:00'),
(37, 'CAMP-004', 'mike@company.com', 'Finance', 'Clicked', '2025-05-20 09:15:00'),
(38, 'CAMP-005', 'nancy@company.com', 'HR', 'Sent', '2025-07-12 09:00:00'),
(39, 'CAMP-005', 'nancy@company.com', 'HR', 'Reported', '2025-07-12 09:30:00'),
(40, 'CAMP-001', 'oscar@company.com', 'Engineering', 'Sent', '2025-01-15 09:00:00'),
(41, 'CAMP-001', 'oscar@company.com', 'Engineering', 'Opened', '2025-01-15 09:10:00'),
(42, 'CAMP-002', 'paul@company.com', 'Sales', 'Sent', '2025-02-10 09:00:00'),
(43, 'CAMP-002', 'paul@company.com', 'Sales', 'Clicked', '2025-02-10 09:20:00'),
(44, 'CAMP-003', 'quinn@company.com', 'Marketing', 'Sent', '2025-04-05 09:00:00'),
(45, 'CAMP-003', 'quinn@company.com', 'Marketing', 'EnteredCreds', '2025-04-05 10:00:00'), -- Fail
(46, 'CAMP-004', 'rachel@company.com', 'Legal', 'Sent', '2025-05-20 09:00:00'),
(47, 'CAMP-004', 'rachel@company.com', 'Legal', 'Reported', '2025-05-20 09:05:00'),
(48, 'CAMP-005', 'steve@company.com', 'Finance', 'Sent', '2025-07-12 09:00:00'),
(49, 'CAMP-005', 'steve@company.com', 'Finance', 'Clicked', '2025-07-12 09:15:00'),
(50, 'CAMP-001', 'tina@company.com', 'Sales', 'Sent', '2025-01-15 09:00:00'); 

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Campaign Performance
-------------------------------------------------------------------------------

-- Flag user actions
CREATE OR REPLACE VIEW CyberSecDB.Training.Silver.EnrichedActions AS
SELECT 
    ua.CampaignID,
    c.CampaignName,
    ua.EmployeeEmail,
    ua.Department,
    ua.ActionType,
    CASE 
        WHEN ua.ActionType IN ('Clicked', 'AttachmentOpened', 'EnteredCreds') THEN 1 
        ELSE 0 
    END AS IsFailure,
    CASE 
        WHEN ua.ActionType = 'Reported' THEN 1 
        ELSE 0 
    END AS IsSuccess
FROM CyberSecDB.Training.Bronze.UserActions ua
JOIN CyberSecDB.Training.Bronze.SimCampaigns c ON ua.CampaignID = c.CampaignID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Departmental Vulnerability
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CyberSecDB.Training.Gold.DepartmentRisk AS
SELECT 
    Department,
    COUNT(DISTINCT EmployeeEmail) AS TotalParticipants,
    SUM(IsFailure) AS TotalFailures,
    SUM(IsSuccess) AS TotalReports,
    (CAST(SUM(IsFailure) AS DOUBLE) / COUNT(DISTINCT EmployeeEmail)) * 100 AS FailureRatePct
FROM CyberSecDB.Training.Silver.EnrichedActions
GROUP BY Department;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Which department has the highest Failure Rate according to CyberSecDB.Training.Gold.DepartmentRisk?"

PROMPT 2:
"List all employees who 'EnteredCreds' during any phishing campaign from the Silver layer."

PROMPT 3:
"Show the click rate trend over time by comparing the different campaigns."
*/
