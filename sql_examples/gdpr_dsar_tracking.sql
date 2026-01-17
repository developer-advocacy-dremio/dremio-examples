/*
 * GDPR DSAR Compliance Tracking Demo
 * 
 * Scenario:
 * Under GDPR/CCPA, customers can submit Data Subject Access Requests (DSAR).
 * Organizations must fulfill these (e.g., provide data or delete it) within 30 days.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Track request status and identify SLA breaches.
 * 
 * Note: Assumes a catalog named 'PrivacyOpsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PrivacyOpsDB;
CREATE FOLDER IF NOT EXISTS PrivacyOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS PrivacyOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS PrivacyOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Request Logs
-------------------------------------------------------------------------------
-- Description: Log of incoming requests and system scan results.

CREATE TABLE IF NOT EXISTS PrivacyOpsDB.Bronze.AccessRequests (
    RequestID VARCHAR,
    CustomerID VARCHAR,
    RequestType VARCHAR, -- 'Access', 'Deletion', 'Correction'
    ReceivedDate DATE,
    Status VARCHAR -- 'New', 'In_Progress', 'Completed'
);

CREATE TABLE IF NOT EXISTS PrivacyOpsDB.Bronze.SystemEvaluations (
    ScanID INT,
    RequestID VARCHAR,
    SystemName VARCHAR, -- 'CRM', 'MarketingDB', 'DataLake'
    DataFound BOOLEAN,
    ScanDate TIMESTAMP
);

-- 1.1 Populate AccessRequests (50+ Records)
INSERT INTO PrivacyOpsDB.Bronze.AccessRequests (RequestID, CustomerID, RequestType, ReceivedDate, Status) VALUES
('R-001', 'C-1001', 'Access', '2025-01-01', 'Completed'),
('R-002', 'C-1002', 'Deletion', '2025-01-01', 'Completed'),
('R-003', 'C-1003', 'Access', '2025-01-02', 'In_Progress'), -- Potential Breach
('R-004', 'C-1004', 'Correction', '2025-01-03', 'Completed'),
('R-005', 'C-1005', 'Deletion', '2025-01-05', 'New'), -- Stuck
('R-006', 'C-1006', 'Access', '2025-01-05', 'Completed'),
('R-007', 'C-1007', 'Access', '2025-01-06', 'Completed'),
('R-008', 'C-1008', 'Deletion', '2025-01-06', 'Completed'),
('R-009', 'C-1009', 'Access', '2025-01-07', 'In_Progress'),
('R-010', 'C-1010', 'Correction', '2025-01-08', 'Completed'),
('R-011', 'C-1011', 'Access', '2025-01-08', 'Completed'),
('R-012', 'C-1012', 'Deletion', '2025-01-09', 'Completed'),
('R-013', 'C-1013', 'Access', '2025-01-10', 'Completed'),
('R-014', 'C-1014', 'Access', '2025-01-10', 'In_Progress'),
('R-015', 'C-1015', 'Deletion', '2025-01-11', 'Completed'),
('R-016', 'C-1016', 'Correction', '2025-01-12', 'New'),
('R-017', 'C-1017', 'Access', '2025-01-12', 'Completed'),
('R-018', 'C-1018', 'Deletion', '2025-01-13', 'Completed'),
('R-019', 'C-1019', 'Access', '2025-01-14', 'Completed'),
('R-020', 'C-1020', 'Access', '2025-01-15', 'In_Progress'),
('R-021', 'C-1021', 'Deletion', '2025-01-15', 'Completed'),
('R-022', 'C-1022', 'Correction', '2025-01-16', 'Completed'),
('R-023', 'C-1023', 'Access', '2025-01-17', 'Completed'),
('R-024', 'C-1024', 'Deletion', '2025-01-18', 'Completed'),
('R-025', 'C-1025', 'Access', '2025-01-18', 'In_Progress'),
('R-026', 'C-1026', 'Access', '2025-01-19', 'Completed'),
('R-027', 'C-1027', 'Deletion', '2025-01-20', 'Completed'),
('R-028', 'C-1028', 'Correction', '2025-01-21', 'New'),
('R-029', 'C-1029', 'Access', '2025-01-22', 'Completed'),
('R-030', 'C-1030', 'Deletion', '2025-01-23', 'Completed'),
('R-031', 'C-1031', 'Access', '2025-01-24', 'Completed'),
('R-032', 'C-1032', 'Access', '2025-01-25', 'In_Progress'),
('R-033', 'C-1033', 'Deletion', '2025-01-26', 'Completed'),
('R-034', 'C-1034', 'Correction', '2025-01-27', 'Completed'),
('R-035', 'C-1035', 'Access', '2025-01-28', 'Completed'),
('R-036', 'C-1036', 'Deletion', '2025-01-29', 'Completed'),
('R-037', 'C-1037', 'Access', '2025-01-30', 'In_Progress'),
('R-038', 'C-1038', 'Access', '2025-01-31', 'New'),
('R-039', 'C-1039', 'Deletion', '2025-02-01', 'Completed'),
('R-040', 'C-1040', 'Correction', '2025-02-02', 'Completed'),
('R-041', 'C-1041', 'Access', '2025-02-03', 'Completed'),
('R-042', 'C-1042', 'Deletion', '2025-02-04', 'Completed'),
('R-043', 'C-1043', 'Access', '2025-02-05', 'In_Progress'),
('R-044', 'C-1044', 'Access', '2025-02-06', 'Completed'),
('R-045', 'C-1045', 'Deletion', '2025-02-07', 'Completed'),
('R-046', 'C-1046', 'Correction', '2025-02-08', 'Completed'),
('R-047', 'C-1047', 'Access', '2025-02-09', 'Completed'),
('R-048', 'C-1048', 'Deletion', '2025-02-10', 'Completed'),
('R-049', 'C-1049', 'Access', '2025-02-11', 'In_Progress'),
('R-050', 'C-1050', 'Access', '2025-02-12', 'New');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Timeliness Analysis
-------------------------------------------------------------------------------
-- Description: Calculating age of open requests.
-- Transformation: Age = Today - ReceivedDate.

CREATE OR REPLACE VIEW PrivacyOpsDB.Silver.RequestLifecycle AS
SELECT
    RequestID,
    CustomerID,
    RequestType,
    Status,
    ReceivedDate,
    TIMESTAMPDIFF(DAY, ReceivedDate, DATE '2025-02-15') AS RequestAgeDays -- Simulating 'Today'
FROM PrivacyOpsDB.Bronze.AccessRequests;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Compliance Reporting
-------------------------------------------------------------------------------
-- Description: Monitoring SLAs. GDPR usually requires 30 days.
-- Insight: Flagging requests > 25 days as 'Warning' and > 30 as 'Breach'.

CREATE OR REPLACE VIEW PrivacyOpsDB.Gold.SLA_Breach_Report AS
SELECT
    RequestID,
    Status,
    RequestAgeDays,
    CASE 
        WHEN RequestAgeDays > 30 THEN 'BREACH'
        WHEN RequestAgeDays > 25 THEN 'WARNING'
        ELSE 'OK'
    END AS ComplianceStatus
FROM PrivacyOpsDB.Silver.RequestLifecycle
WHERE Status <> 'Completed';

CREATE OR REPLACE VIEW PrivacyOpsDB.Gold.WeeklyProgress AS
SELECT
    RequestType,
    Status,
    COUNT(*) AS Count
FROM PrivacyOpsDB.Bronze.AccessRequests
GROUP BY RequestType, Status;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show all critical items from PrivacyOpsDB.Gold.SLA_Breach_Report where ComplianceStatus is 'BREACH' or 'WARNING'."

PROMPT:
"Visualize the count of requests by Status for each RequestType using PrivacyOpsDB.Gold.WeeklyProgress."
*/
