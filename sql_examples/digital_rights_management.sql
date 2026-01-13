/*
 * Digital Rights Management (DRM) Demo
 * 
 * Scenario:
 * Tracking content licensing, usage regions, and expiration dates.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Ensure compliance and renew high-value content licenses.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS MediaRightsDB;
CREATE FOLDER IF NOT EXISTS MediaRightsDB.Bronze;
CREATE FOLDER IF NOT EXISTS MediaRightsDB.Silver;
CREATE FOLDER IF NOT EXISTS MediaRightsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MediaRightsDB.Bronze.Licenses (
    LicenseID INT,
    Title VARCHAR,
    Distributor VARCHAR,
    AllowedRegions VARCHAR, -- 'US,CA,UK'
    StartDate DATE,
    EndDate DATE,
    Cost DOUBLE
);

CREATE TABLE IF NOT EXISTS MediaRightsDB.Bronze.StreamingLogs (
    StreamID INT,
    LicenseID INT,
    UserRegion VARCHAR,
    StreamDate DATE
);

INSERT INTO MediaRightsDB.Bronze.Licenses VALUES
(1, 'Blockbuster Hits Vol 1', 'StudioX', 'US,CA', '2024-01-01', '2025-12-31', 1000000.0),
(2, 'Indie Gems', 'IndieDist', 'Global', '2024-06-01', '2026-06-01', 50000.0);

INSERT INTO MediaRightsDB.Bronze.StreamingLogs VALUES
(1, 1, 'US', '2025-01-10'),
(2, 1, 'CA', '2025-01-11'),
(3, 1, 'UK', '2025-01-12'), -- Geo-blocking failure?
(4, 2, 'JP', '2025-02-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MediaRightsDB.Silver.UsageCompliance AS
SELECT 
    s.StreamID,
    l.Title,
    s.UserRegion,
    l.AllowedRegions,
    CASE 
        WHEN l.AllowedRegions = 'Global' THEN 'Valid'
        WHEN POSITION(s.UserRegion IN l.AllowedRegions) > 0 THEN 'Valid'
        ELSE 'Violation' 
    END AS ComplianceStatus
FROM MediaRightsDB.Bronze.StreamingLogs s
JOIN MediaRightsDB.Bronze.Licenses l ON s.LicenseID = l.LicenseID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW MediaRightsDB.Gold.LicenseExpirationReport AS
SELECT 
    Title,
    EndDate,
    TIMESTAMPDIFF(DAY, CURRENT_DATE, EndDate) AS DaysRemaining,
    Cost
FROM MediaRightsDB.Bronze.Licenses
WHERE TIMESTAMPDIFF(DAY, CURRENT_DATE, EndDate) < 365;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Find any streams in MediaRightsDB.Silver.UsageCompliance where ComplianceStatus is 'Violation'."
*/
