/*
 * KYC (Know Your Customer) Remediation Demo
 * 
 * Scenario:
 * A bank must ensure all customer identification documents (Passports, Driver's Licenses) are valid.
 * Expired documents trigger a "Remediation" workflow to request new ones.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Generate a daily worklist of customers needing outreach.
 * 
 * Note: Assumes a catalog named 'ComplianceDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ComplianceDB;
CREATE FOLDER IF NOT EXISTS ComplianceDB.Bronze;
CREATE FOLDER IF NOT EXISTS ComplianceDB.Silver;
CREATE FOLDER IF NOT EXISTS ComplianceDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Document Registry
-------------------------------------------------------------------------------
-- Description: System of record for customer ID documents.

CREATE TABLE IF NOT EXISTS ComplianceDB.Bronze.CustomerDocuments (
    DocID VARCHAR,
    CustomerID VARCHAR,
    DocType VARCHAR, -- 'Passport', 'DriversLicense', 'NationalID'
    IssueDate DATE,
    ExpiryDate DATE,
    IssuingCountry VARCHAR
);

-- 1.1 Populate CustomerDocuments (50+ Records)
-- Simulating a mix of valid, expiring soon, and already expired docs.
INSERT INTO ComplianceDB.Bronze.CustomerDocuments (DocID, CustomerID, DocType, IssueDate, ExpiryDate, IssuingCountry) VALUES
('D-001', 'C-1001', 'Passport', '2015-01-01', '2025-01-01', 'USA'), -- Expired
('D-002', 'C-1002', 'Passport', '2016-05-15', '2026-05-15', 'CAN'),
('D-003', 'C-1003', 'DriversLicense', '2020-01-01', '2025-02-01', 'USA'), -- Expiring Soon
('D-004', 'C-1004', 'NationalID', '2018-03-20', '2028-03-20', 'GBR'),
('D-005', 'C-1005', 'Passport', '2015-06-01', '2025-06-01', 'USA'),
('D-006', 'C-1006', 'DriversLicense', '2021-08-10', '2026-08-10', 'USA'),
('D-007', 'C-1007', 'Passport', '2014-12-12', '2024-12-12', 'FRA'), -- Expired
('D-008', 'C-1008', 'Passport', '2019-11-01', '2029-11-01', 'DEU'),
('D-009', 'C-1009', 'NationalID', '2020-02-28', '2025-02-28', 'ESP'), -- Expiring Soon
('D-010', 'C-1010', 'DriversLicense', '2019-07-04', '2024-07-04', 'USA'), -- Expired Long Ago
('D-011', 'C-1011', 'Passport', '2022-01-01', '2032-01-01', 'USA'),
('D-012', 'C-1012', 'DriversLicense', '2023-05-05', '2028-05-05', 'CAN'),
('D-013', 'C-1013', 'Passport', '2015-03-15', '2025-03-15', 'ITA'),
('D-014', 'C-1014', 'NationalID', '2017-09-09', '2027-09-09', 'JPN'),
('D-015', 'C-1015', 'Passport', '2014-10-10', '2024-10-10', 'USA'), -- Expired
('D-016', 'C-1016', 'DriversLicense', '2021-12-25', '2026-12-25', 'USA'),
('D-017', 'C-1017', 'Passport', '2020-04-20', '2030-04-20', 'AUS'),
('D-018', 'C-1018', 'NationalID', '2019-06-01', '2024-06-01', 'BRA'), -- Expired
('D-019', 'C-1019', 'Passport', '2016-08-08', '2026-08-08', 'IND'),
('D-020', 'C-1020', 'DriversLicense', '2022-02-02', '2027-02-02', 'USA'),
('D-021', 'C-1021', 'Passport', '2015-05-05', '2025-05-05', 'USA'),
('D-022', 'C-1022', 'Passport', '2013-01-01', '2023-01-01', 'CHN'), -- Expired
('D-023', 'C-1023', 'NationalID', '2021-11-11', '2031-11-11', 'KOR'),
('D-024', 'C-1024', 'DriversLicense', '2020-10-31', '2025-10-31', 'USA'),
('D-025', 'C-1025', 'Passport', '2017-07-07', '2027-07-07', 'MEX'),
('D-026', 'C-1026', 'Passport', '2014-04-01', '2024-04-01', 'USA'), -- Expired
('D-027', 'C-1027', 'DriversLicense', '2019-03-17', '2024-03-17', 'IRL'), -- Expired
('D-028', 'C-1028', 'Passport', '2022-09-09', '2032-09-09', 'NZL'),
('D-029', 'C-1029', 'NationalID', '2023-01-20', '2033-01-20', 'SGP'),
('D-030', 'C-1030', 'Passport', '2015-12-25', '2025-12-25', 'USA'),
('D-031', 'C-1031', 'DriversLicense', '2018-06-15', '2023-06-15', 'ZAF'), -- Expired
('D-032', 'C-1032', 'Passport', '2016-02-28', '2026-02-28', 'RUS'),
('D-033', 'C-1033', 'NationalID', '2020-05-01', '2030-05-01', 'SWE'),
('D-034', 'C-1034', 'Passport', '2014-08-30', '2024-08-30', 'NOR'), -- Expired
('D-035', 'C-1035', 'DriversLicense', '2021-04-04', '2026-04-04', 'USA'),
('D-036', 'C-1036', 'Passport', '2017-01-20', '2027-01-20', 'DNK'),
('D-037', 'C-1037', 'Passport', '2015-09-15', '2025-09-15', 'FIN'),
('D-038', 'C-1038', 'NationalID', '2019-12-01', '2029-12-01', 'POL'),
('D-039', 'C-1039', 'Passport', '2014-06-01', '2024-06-01', 'USA'), -- Expired
('D-040', 'C-1040', 'DriversLicense', '2022-11-22', '2027-11-22', 'USA'),
('D-041', 'C-1041', 'Passport', '2016-03-03', '2026-03-03', 'NLD'),
('D-042', 'C-1042', 'NationalID', '2021-07-07', '2031-07-07', 'BEL'),
('D-043', 'C-1043', 'Passport', '2015-02-14', '2025-02-14', 'CHE'),
('D-044', 'C-1044', 'DriversLicense', '2018-08-08', '2023-08-08', 'AUT'), -- Expired
('D-045', 'C-1045', 'Passport', '2023-01-01', '2033-01-01', 'USA'),
('D-046', 'C-1046', 'Passport', '2014-11-20', '2024-11-20', 'GRC'), -- Expired
('D-047', 'C-1047', 'NationalID', '2020-09-09', '2030-09-09', 'PRT'),
('D-048', 'C-1048', 'DriversLicense', '2022-04-15', '2027-04-15', 'USA'),
('D-049', 'C-1049', 'Passport', '2015-10-31', '2025-10-31', 'USA'),
('D-050', 'C-1050', 'Passport', '2017-05-01', '2027-05-01', 'ARG');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Compliance Logic
-------------------------------------------------------------------------------
-- Description: Calculating Days to Expiry relative to a fixed analysis date (e.g., 2025-01-15).
-- Transformation: Grouping into buckets (Expired, ExpiringSoon, Valid).

CREATE OR REPLACE VIEW ComplianceDB.Silver.DocumentComplianceStatus AS
SELECT
    CustomerID,
    DocID,
    DocType,
    ExpiryDate,
    TIMESTAMPDIFF(DAY, DATE '2025-01-15', ExpiryDate) AS DaysUntilExpiry,
    CASE 
        WHEN ExpiryDate < DATE '2025-01-15' THEN 'EXPIRED'
        WHEN TIMESTAMPDIFF(DAY, DATE '2025-01-15', ExpiryDate) <= 30 THEN 'EXPIRING_SOON'
        ELSE 'VALID'
    END AS Status
FROM ComplianceDB.Bronze.CustomerDocuments;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Remediation Management
-------------------------------------------------------------------------------
-- Description: Lists for operations teams to contact customers.
-- Insight: Prioritizing 'EXPIRED' status.

CREATE OR REPLACE VIEW ComplianceDB.Gold.RemediationWorklist AS
SELECT
    CustomerID,
    DocType,
    ExpiryDate,
    DaysUntilExpiry,
    Status
FROM ComplianceDB.Silver.DocumentComplianceStatus
WHERE Status IN ('EXPIRED', 'EXPIRING_SOON')
ORDER BY DaysUntilExpiry ASC; -- Most urgent first (negatives are oldest expired)

CREATE OR REPLACE VIEW ComplianceDB.Gold.ComplianceStats AS
SELECT
    Status,
    COUNT(*) AS Count
FROM ComplianceDB.Silver.DocumentComplianceStatus
GROUP BY Status;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show me the RemediationWorklist sorted by DaysUntilExpiry to see the most critical violations first."

PROMPT:
"What is the breakdown of document statuses in the ComplianceDB.Gold.ComplianceStats view?"
*/
