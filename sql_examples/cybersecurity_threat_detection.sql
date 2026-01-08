/*
 * Cybersecurity Threat Detection Demo
 * 
 * Scenario:
 * A security operations center (SOC) monitors login attempts for malicious activity.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Detect brute force attacks and unauthorized access.
 * 
 * Note: Assumes a catalog named 'CyberSecDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CyberSecDB;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Bronze;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Silver;
CREATE FOLDER IF NOT EXISTS CyberSecDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Access Logs
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CyberSecDB.Bronze.Users (
    UserID INT,
    Username VARCHAR,
    "Role" VARCHAR, -- 'Admin', 'User'
    Region VARCHAR
);

CREATE TABLE IF NOT EXISTS CyberSecDB.Bronze.AccessControlLists (
    ACL_ID INT,
    Resource VARCHAR,
    AllowedRole VARCHAR
);

CREATE TABLE IF NOT EXISTS CyberSecDB.Bronze.LoginLogs (
    LogID INT,
    Username VARCHAR,
    AttemptTime TIMESTAMP,
    IPAddress VARCHAR,
    Status VARCHAR -- 'Success', 'Failure'
);

-- 1.2 Populate Bronze Tables
INSERT INTO CyberSecDB.Bronze.Users (UserID, Username, "Role", Region) VALUES
(1, 'admin_dave', 'Admin', 'US'),
(2, 'user_sarah', 'User', 'EU'),
(3, 'user_tom', 'User', 'US');

INSERT INTO CyberSecDB.Bronze.AccessControlLists (ACL_ID, Resource, AllowedRole) VALUES
(1, 'Server-01', 'Admin'),
(2, 'App-HR', 'User');

INSERT INTO CyberSecDB.Bronze.LoginLogs (LogID, Username, AttemptTime, IPAddress, Status) VALUES
(1, 'admin_dave', '2025-09-01 08:00:00', '192.168.1.5', 'Success'),
(2, 'user_sarah', '2025-09-01 09:00:00', '10.0.0.50', 'Success'),
(3, 'admin_dave', '2025-09-02 02:00:00', '203.0.113.5', 'Failure'), -- Suspicious
(4, 'admin_dave', '2025-09-02 02:00:05', '203.0.113.5', 'Failure'),
(5, 'admin_dave', '2025-09-02 02:00:10', '203.0.113.5', 'Failure'), -- Brute force pattern
(6, 'unknown_user', '2025-09-02 03:00:00', '198.51.100.2', 'Failure');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Threat Enrichment
-------------------------------------------------------------------------------

-- 2.1 View: Suspicious_Activity
-- Highlights failed logins from recognized users.
CREATE OR REPLACE VIEW CyberSecDB.Silver.Suspicious_Activity AS
SELECT
    l.Username,
    l.AttemptTime,
    l.IPAddress,
    l.Status,
    u."Role"
FROM CyberSecDB.Bronze.LoginLogs l
LEFT JOIN CyberSecDB.Bronze.Users u ON l.Username = u.Username
WHERE l.Status = 'Failure';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Incident Reporting
-------------------------------------------------------------------------------

-- 3.1 View: Brute_Force_Detection
-- Flags IPs with > 2 failures within window.
CREATE OR REPLACE VIEW CyberSecDB.Gold.Brute_Force_Detection AS
SELECT
    IPAddress,
    Username,
    COUNT(LogID) AS FailureCount,
    MIN(AttemptTime) AS FirstAttempt,
    MAX(AttemptTime) AS LastAttempt
FROM CyberSecDB.Silver.Suspicious_Activity
GROUP BY IPAddress, Username
HAVING COUNT(LogID) > 2;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Active Threats):
"Show me all IPs flagged in CyberSecDB.Gold.Brute_Force_Detection."

PROMPT 2 (Admin Risks):
"List suspicious activity for users with 'Admin' role from CyberSecDB.Silver.Suspicious_Activity."
*/
