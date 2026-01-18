/*
    Dremio High-Volume SQL Pattern: Government Building Code Violations
    
    Business Scenario:
    Buildings Departments inspect construction sites for safety violations. Chronic offenders
    risk "Stop Work Orders". We track open violations and their severity.
    
    Data Story:
    We track Permits, Inspections, and Contractor Profiles.
    
    Medallion Architecture:
    - Bronze: Permits, ContractorViolations.
      *Volume*: 50+ records.
    - Silver: InspectionHistory (Dates and Outcomes).
    - Gold: HighRiskContractors (Multiple open serious violations).
    
    Key Dremio Features:
    - Group By
    - Aggregation Filter
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentBuildingsDB;
CREATE FOLDER IF NOT EXISTS GovernmentBuildingsDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentBuildingsDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentBuildingsDB.Gold;
USE GovernmentBuildingsDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentBuildingsDB.Bronze.Contractors (
    LicenseID STRING,
    CompanyName STRING
);

INSERT INTO GovernmentBuildingsDB.Bronze.Contractors VALUES
('LIC101', 'General Builder A'),
('LIC102', 'General Builder B'),
('LIC103', 'Top Tier Const'),
('LIC104', 'Safe Hands LLC'),
('LIC105', 'City Structs Inc'),
('LIC106', 'BuildRight'),
('LIC107', 'Cornerstone Dev'),
('LIC108', 'Metro Builders'),
('LIC109', 'Skyline Const'),
('LIC110', 'Urban Developers');

CREATE OR REPLACE TABLE GovernmentBuildingsDB.Bronze.Violations (
    ViolationID STRING,
    LicenseID STRING, -- Contractor
    Address STRING,
    ViolationType STRING, -- Structure, Electrical, Plumbing, Paperwork
    Severity STRING, -- High, Medium, Low
    IssueDate DATE,
    Status STRING -- Open, Resolved
);

-- Bulk Violations (50 Records)
INSERT INTO GovernmentBuildingsDB.Bronze.Violations VALUES
('VIO5001', 'LIC101', '100 Main St', 'Structure', 'High', DATE '2025-01-01', 'Open'),
('VIO5002', 'LIC101', '102 Main St', 'Electrical', 'Medium', DATE '2025-01-02', 'Resolved'),
('VIO5003', 'LIC101', '100 Main St', 'Paperwork', 'Low', DATE '2025-01-05', 'Open'), -- High Risk risk?
('VIO5004', 'LIC102', '200 Broadway', 'Plumbing', 'High', DATE '2025-01-01', 'Open'),
('VIO5005', 'LIC102', '200 Broadway', 'Structure', 'High', DATE '2025-01-15', 'Open'), -- Stop Work candidate
('VIO5006', 'LIC103', '50 5th Ave', 'Paperwork', 'Low', DATE '2025-01-10', 'Open'),
('VIO5007', 'LIC103', '50 5th Ave', 'Electrical', 'Low', DATE '2025-01-12', 'Resolved'),
('VIO5008', 'LIC104', '1 Park Pl', 'Structure', 'High', DATE '2024-12-01', 'Resolved'),
('VIO5009', 'LIC105', '99 Wall St', 'Electrical', 'High', DATE '2025-01-20', 'Open'),
('VIO5010', 'LIC105', '99 Wall St', 'Plumbing', 'Medium', DATE '2025-01-20', 'Open'),
('VIO5011', 'LIC106', '12 Elm St', 'Structure', 'High', DATE '2025-01-05', 'Open'),
('VIO5012', 'LIC106', '12 Elm St', 'Structure', 'High', DATE '2025-01-10', 'Open'), -- Stop Work
('VIO5013', 'LIC107', '88 Oak Ln', 'Paperwork', 'Low', DATE '2025-01-15', 'Resolved'),
('VIO5014', 'LIC108', '77 Pine Rd', 'Electrical', 'Medium', DATE '2025-01-18', 'Open'),
('VIO5015', 'LIC109', '66 Maple Dr', 'Plumbing', 'Low', DATE '2025-01-19', 'Open'),
('VIO5016', 'LIC110', '55 Cedar Blvd', 'Structure', 'Medium', DATE '2025-01-01', 'Resolved'),
('VIO5017', 'LIC101', '105 Main St', 'Structure', 'High', DATE '2025-01-18', 'Open'), -- Second High for LIC101
('VIO5018', 'LIC102', '205 Broadway', 'Paperwork', 'Low', DATE '2025-01-19', 'Resolved'),
('VIO5019', 'LIC103', '55 5th Ave', 'Electrical', 'Medium', DATE '2025-01-20', 'Open'),
('VIO5020', 'LIC104', '2 Park Pl', 'Plumbing', 'High', DATE '2025-01-05', 'Open'),
('VIO5021', 'LIC105', '100 Wall St', 'Structure', 'Low', DATE '2025-01-06', 'Resolved'),
('VIO5022', 'LIC106', '15 Elm St', 'Structure', 'Low', DATE '2025-01-07', 'Open'),
('VIO5023', 'LIC107', '90 Oak Ln', 'Electrical', 'High', DATE '2025-01-08', 'Open'),
('VIO5024', 'LIC108', '80 Pine Rd', 'Paperwork', 'Medium', DATE '2025-01-09', 'Open'),
('VIO5025', 'LIC109', '70 Maple Dr', 'Electrical', 'Medium', DATE '2025-01-10', 'Resolved'),
('VIO5026', 'LIC110', '60 Cedar Blvd', 'Plumbing', 'High', DATE '2025-01-11', 'Open'),
('VIO5027', 'LIC101', '110 Main St', 'Paperwork', 'Low', DATE '2025-01-12', 'Open'),
('VIO5028', 'LIC102', '210 Broadway', 'Structure', 'Medium', DATE '2025-01-13', 'Resolved'),
('VIO5029', 'LIC103', '60 5th Ave', 'Electrical', 'High', DATE '2025-01-14', 'Open'),
('VIO5030', 'LIC104', '3 Park Pl', 'Plumbing', 'Low', DATE '2025-01-15', 'Resolved'),
('VIO5031', 'LIC105', '105 Wall St', 'Structure', 'High', DATE '2025-01-16', 'Open'), -- Second High for LIC105 (Total 2)
('VIO5032', 'LIC106', '18 Elm St', 'Paperwork', 'Medium', DATE '2025-01-17', 'Open'),
('VIO5033', 'LIC107', '95 Oak Ln', 'Electrical', 'Low', DATE '2025-01-18', 'Resolved'),
('VIO5034', 'LIC108', '85 Pine Rd', 'Structure', 'High', DATE '2025-01-19', 'Open'),
('VIO5035', 'LIC109', '75 Maple Dr', 'Plumbing', 'Medium', DATE '2025-01-20', 'Open'),
('VIO5036', 'LIC110', '65 Cedar Blvd', 'Paperwork', 'Low', DATE '2025-01-01', 'Resolved'),
('VIO5037', 'LIC101', '115 Main St', 'Electrical', 'High', DATE '2025-01-02', 'Resolved'),
('VIO5038', 'LIC102', '215 Broadway', 'Structure', 'Low', DATE '2025-01-03', 'Open'),
('VIO5039', 'LIC103', '65 5th Ave', 'Plumbing', 'High', DATE '2025-01-04', 'Open'), -- Second High for LIC103
('VIO5040', 'LIC104', '4 Park Pl', 'Paperwork', 'Medium', DATE '2025-01-05', 'Resolved'),
('VIO5041', 'LIC105', '110 Wall St', 'Electrical', 'Low', DATE '2025-01-06', 'Open'),
('VIO5042', 'LIC106', '20 Elm St', 'Structure', 'Medium', DATE '2025-01-07', 'Open'),
('VIO5043', 'LIC107', '100 Oak Ln', 'Plumbing', 'High', DATE '2025-01-08', 'Open'), -- Second High for LIC107
('VIO5044', 'LIC108', '90 Pine Rd', 'Paperwork', 'Low', DATE '2025-01-09', 'Resolved'),
('VIO5045', 'LIC109', '80 Maple Dr', 'Electrical', 'High', DATE '2025-01-10', 'Open'),
('VIO5046', 'LIC110', '70 Cedar Blvd', 'Structure', 'Medium', DATE '2025-01-11', 'Open'),
('VIO5047', 'LIC101', '120 Main St', 'Plumbing', 'Low', DATE '2025-01-12', 'Resolved'),
('VIO5048', 'LIC102', '220 Broadway', 'Structure', 'High', DATE '2025-01-13', 'Open'), -- Third High for LIC102
('VIO5049', 'LIC103', '70 5th Ave', 'Electrical', 'Medium', DATE '2025-01-14', 'Resolved'),
('VIO5050', 'LIC104', '5 Park Pl', 'Paperwork', 'High', DATE '2025-01-15', 'Open'); -- Second High for LIC104

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Open Violation Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentBuildingsDB.Silver.ContractorRisk AS
SELECT
    v.LicenseID,
    c.CompanyName,
    v.ViolationType,
    v.Severity,
    v.Status,
    v.IssueDate
FROM GovernmentBuildingsDB.Bronze.Violations v
JOIN GovernmentBuildingsDB.Bronze.Contractors c ON v.LicenseID = c.LicenseID
WHERE v.Status = 'Open';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Enforcement Target List
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentBuildingsDB.Gold.HighRiskContractors AS
SELECT
    CompanyName,
    COUNT(*) AS TotalOpenViolations,
    SUM(CASE WHEN Severity = 'High' THEN 1 ELSE 0 END) AS HighSeverityCount,
    CASE 
        WHEN SUM(CASE WHEN Severity = 'High' THEN 1 ELSE 0 END) >= 2 THEN 'STOP_WORK_ORDER'
        ELSE 'WARNING'
    END AS EnforcementAction
FROM GovernmentBuildingsDB.Silver.ContractorRisk
GROUP BY CompanyName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List companies with a STOP WORK ORDER recommendation."
    2. "Count open violations by Type."
    3. "Who has the most High Severity violations?"
*/
