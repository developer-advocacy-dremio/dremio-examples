/*
 * Cybersecurity: Zero Trust Access Logs
 * 
 * Scenario:
 * Correlating identity verification, device health, and resource access requests to enforce a Zero Trust architecture.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ZeroTrustDB;
CREATE FOLDER IF NOT EXISTS ZeroTrustDB.Security;
CREATE FOLDER IF NOT EXISTS ZeroTrustDB.Security.Bronze;
CREATE FOLDER IF NOT EXISTS ZeroTrustDB.Security.Silver;
CREATE FOLDER IF NOT EXISTS ZeroTrustDB.Security.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Access Requests
-------------------------------------------------------------------------------

-- AccessLogs Table
CREATE TABLE IF NOT EXISTS ZeroTrustDB.Security.Bronze.AccessLogs (
    RequestID VARCHAR,
    UserID VARCHAR,
    DeviceID VARCHAR,
    ResourceRequested VARCHAR, -- HR-DB, Prod-Server, Email
    DeviceHealthStatus VARCHAR, -- Healthy, Patched, Vulnerable
    MFA_Status VARCHAR, -- Verified, Failed, Skipped
    AccessTimestamp TIMESTAMP
);

INSERT INTO ZeroTrustDB.Security.Bronze.AccessLogs VALUES
('R-001', 'User-A', 'Dev-001', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 09:00:00'),
('R-002', 'User-B', 'Dev-002', 'HR-DB', 'Vulnerable', 'Verified', '2026-06-01 09:05:00'), -- Block
('R-003', 'User-C', 'Dev-003', 'Email', 'Healthy', 'Skipped', '2026-06-01 09:10:00'), -- Investigate
('R-004', 'User-A', 'Dev-001', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 09:15:00'),
('R-005', 'User-D', 'Dev-004', 'Prod-Server', 'Patched', 'Failed', '2026-06-01 09:20:00'), -- Fail
('R-006', 'User-E', 'Dev-005', 'Docs', 'Healthy', 'Verified', '2026-06-01 09:25:00'),
('R-007', 'User-F', 'Dev-006', 'Email', 'Healthy', 'Verified', '2026-06-01 09:30:00'),
('R-008', 'User-G', 'Dev-007', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 09:35:00'),
('R-009', 'User-H', 'Dev-008', 'Prod-Server', 'Vulnerable', 'Verified', '2026-06-01 09:40:00'), -- Block
('R-010', 'User-I', 'Dev-009', 'Email', 'Healthy', 'Verified', '2026-06-01 09:45:00'),
('R-011', 'User-J', 'Dev-010', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 09:50:00'),
('R-012', 'User-K', 'Dev-011', 'Docs', 'Patched', 'Verified', '2026-06-01 09:55:00'),
('R-013', 'User-L', 'Dev-012', 'Prod-Server', 'Healthy', 'Skipped', '2026-06-01 10:00:00'), -- Suspicious
('R-014', 'User-M', 'Dev-013', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 10:05:00'),
('R-015', 'User-N', 'Dev-014', 'Email', 'Vulnerable', 'Failed', '2026-06-01 10:10:00'),
('R-016', 'User-O', 'Dev-015', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 10:15:00'),
('R-017', 'User-P', 'Dev-016', 'Docs', 'Healthy', 'Verified', '2026-06-01 10:20:00'),
('R-018', 'User-Q', 'Dev-017', 'Prod-Server', 'Patched', 'Verified', '2026-06-01 10:25:00'),
('R-019', 'User-R', 'Dev-018', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 10:30:00'),
('R-020', 'User-S', 'Dev-019', 'Email', 'Healthy', 'Verified', '2026-06-01 10:35:00'),
('R-021', 'User-T', 'Dev-020', 'Finance-DB', 'Vulnerable', 'Verified', '2026-06-01 10:40:00'), -- Block
('R-022', 'User-U', 'Dev-021', 'Docs', 'Healthy', 'Verified', '2026-06-01 10:45:00'),
('R-023', 'User-V', 'Dev-022', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 10:50:00'),
('R-024', 'User-W', 'Dev-023', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 10:55:00'),
('R-025', 'User-X', 'Dev-024', 'Email', 'Patched', 'Verified', '2026-06-01 11:00:00'),
('R-026', 'User-Y', 'Dev-025', 'Finance-DB', 'Healthy', 'Failed', '2026-06-01 11:05:00'),
('R-027', 'User-Z', 'Dev-026', 'Docs', 'Healthy', 'Verified', '2026-06-01 11:10:00'),
('R-028', 'User-A', 'Dev-001', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 11:15:00'),
('R-029', 'User-B', 'Dev-002', 'Email', 'Vulnerable', 'Verified', '2026-06-01 11:20:00'),
('R-030', 'User-C', 'Dev-003', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 11:25:00'),
('R-031', 'User-D', 'Dev-004', 'Docs', 'Patched', 'Verified', '2026-06-01 11:30:00'),
('R-032', 'User-E', 'Dev-005', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 11:35:00'),
('R-033', 'User-F', 'Dev-006', 'HR-DB', 'Healthy', 'Failed', '2026-06-01 11:40:00'),
('R-034', 'User-G', 'Dev-007', 'Email', 'Healthy', 'Verified', '2026-06-01 11:45:00'),
('R-035', 'User-H', 'Dev-008', 'Finance-DB', 'Vulnerable', 'Verified', '2026-06-01 11:50:00'),
('R-036', 'User-I', 'Dev-009', 'Docs', 'Healthy', 'Verified', '2026-06-01 11:55:00'),
('R-037', 'User-J', 'Dev-010', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 12:00:00'),
('R-038', 'User-K', 'Dev-011', 'HR-DB', 'Patched', 'Verified', '2026-06-01 12:05:00'),
('R-039', 'User-L', 'Dev-012', 'Email', 'Healthy', 'Verified', '2026-06-01 12:10:00'),
('R-040', 'User-M', 'Dev-013', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 12:15:00'),
('R-041', 'User-N', 'Dev-014', 'Docs', 'Vulnerable', 'Skipped', '2026-06-01 12:20:00'), -- Very bad
('R-042', 'User-O', 'Dev-015', 'Prod-Server', 'Healthy', 'Verified', '2026-06-01 12:25:00'),
('R-043', 'User-P', 'Dev-016', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 12:30:00'),
('R-044', 'User-Q', 'Dev-017', 'Email', 'Patched', 'Verified', '2026-06-01 12:35:00'),
('R-045', 'User-R', 'Dev-018', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 12:40:00'),
('R-046', 'User-S', 'Dev-019', 'Docs', 'Healthy', 'Verified', '2026-06-01 12:45:00'),
('R-047', 'User-T', 'Dev-020', 'Prod-Server', 'Vulnerable', 'Failed', '2026-06-01 12:50:00'),
('R-048', 'User-U', 'Dev-021', 'HR-DB', 'Healthy', 'Verified', '2026-06-01 12:55:00'),
('R-049', 'User-V', 'Dev-022', 'Email', 'Healthy', 'Verified', '2026-06-01 13:00:00'),
('R-050', 'User-W', 'Dev-023', 'Finance-DB', 'Healthy', 'Verified', '2026-06-01 13:05:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Policy Enforcement
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ZeroTrustDB.Security.Silver.AccessDecisions AS
SELECT 
    RequestID,
    UserID,
    ResourceRequested,
    DeviceHealthStatus,
    MFA_Status,
    AccessTimestamp,
    -- Zero Trust Policy Logic
    CASE 
        WHEN DeviceHealthStatus = 'Vulnerable' THEN 'DENY - Device Risk'
        WHEN MFA_Status IN ('Failed', 'Skipped') THEN 'DENY - Auth Failed'
        ELSE 'ALLOW'
    END AS EnforcementAction
FROM ZeroTrustDB.Security.Bronze.AccessLogs;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Threat Audit
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ZeroTrustDB.Security.Gold.AccessViolations AS
SELECT 
    UserID,
    ResourceRequested,
    COUNT(*) AS DeniedAttempts,
    -- Concat reasons for audit
    LISTAGG(DISTINCT EnforcementAction, ', ') AS ViolationReasons,
    MAX(AccessTimestamp) AS LastAttempt
FROM ZeroTrustDB.Security.Silver.AccessDecisions
WHERE EnforcementAction LIKE 'DENY%'
GROUP BY UserID, ResourceRequested;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List all Users who had access denied due to 'Device Risk' in the Gold audit view."

PROMPT 2:
"Count the number of 'Skipped' MFA attempts in the Bronze logs."

PROMPT 3:
"Show the percentage of 'ALLOW' vs 'DENY' decisions in the Silver layer."
*/
