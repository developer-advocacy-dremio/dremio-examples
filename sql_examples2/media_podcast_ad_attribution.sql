/*
 * Media: Podcast Ad Attribution
 * 
 * Scenario:
 * Verifying dynamic ad insertion impressions against download IP geolocations and conversion event logs.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PodcastAdDB;
CREATE FOLDER IF NOT EXISTS PodcastAdDB.Analytics;
CREATE FOLDER IF NOT EXISTS PodcastAdDB.Analytics.Bronze;
CREATE FOLDER IF NOT EXISTS PodcastAdDB.Analytics.Silver;
CREATE FOLDER IF NOT EXISTS PodcastAdDB.Analytics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Download & Ad Logs
-------------------------------------------------------------------------------

-- DownloadLogs Table
CREATE TABLE IF NOT EXISTS PodcastAdDB.Analytics.Bronze.DownloadLogs (
    DownloadID VARCHAR,
    EpisodeID VARCHAR,
    IPAddress VARCHAR,
    UserAgent VARCHAR,
    GeoRegion VARCHAR, 
    AdInsertedID VARCHAR, -- DAI Tag
    DownloadTimestamp TIMESTAMP
);

INSERT INTO PodcastAdDB.Analytics.Bronze.DownloadLogs VALUES
('DL-001', 'EP-100', '192.168.1.1', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-01 10:00:00'),
('DL-002', 'EP-100', '192.168.1.2', 'Spotify', 'CA', 'AD-500', '2026-07-01 10:05:00'),
('DL-003', 'EP-100', '192.168.1.3', 'Overcast', 'TX', 'AD-501', '2026-07-01 10:10:00'),
('DL-004', 'EP-101', '192.168.1.4', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-02 10:00:00'),
('DL-005', 'EP-101', '192.168.1.5', 'Spotify', 'CA', 'AD-502', '2026-07-02 10:10:00'),
('DL-006', 'EP-100', '10.0.0.1', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-01 11:00:00'),
('DL-007', 'EP-100', '10.0.0.2', 'Spotify', 'NY', 'AD-500', '2026-07-01 11:05:00'),
('DL-008', 'EP-100', '10.0.0.3', 'Overcast', 'CA', 'AD-502', '2026-07-01 11:10:00'),
('DL-009', 'EP-100', '10.0.0.4', 'ApplePodcasts', 'TX', 'AD-501', '2026-07-01 11:15:00'),
('DL-010', 'EP-100', '10.0.0.5', 'Spotify', 'TX', 'AD-501', '2026-07-01 11:20:00'),
('DL-011', 'EP-101', '10.0.0.6', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-02 11:00:00'),
('DL-012', 'EP-101', '10.0.0.7', 'Spotify', 'NY', 'AD-500', '2026-07-02 11:05:00'),
('DL-013', 'EP-101', '10.0.0.8', 'Overcast', 'CA', 'AD-502', '2026-07-02 11:10:00'),
('DL-014', 'EP-101', '10.0.0.9', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-02 11:15:00'),
('DL-015', 'EP-101', '10.0.0.10', 'Spotify', 'FL', 'AD-503', '2026-07-02 11:20:00'),
('DL-016', 'EP-102', '10.0.0.11', 'ApplePodcasts', 'FL', 'AD-503', '2026-07-03 12:00:00'),
('DL-017', 'EP-102', '10.0.0.12', 'Spotify', 'FL', 'AD-503', '2026-07-03 12:05:00'),
('DL-018', 'EP-102', '10.0.0.13', 'Overcast', 'NY', 'AD-500', '2026-07-03 12:10:00'),
('DL-019', 'EP-102', '10.0.0.14', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-03 12:15:00'),
('DL-020', 'EP-102', '10.0.0.15', 'Spotify', 'TX', 'AD-501', '2026-07-03 12:20:00'),
('DL-021', 'EP-100', '11.0.0.1', 'ApplePodcasts', 'WA', 'AD-504', '2026-07-01 12:00:00'),
('DL-022', 'EP-100', '11.0.0.2', 'Spotify', 'WA', 'AD-504', '2026-07-01 12:05:00'),
('DL-023', 'EP-100', '11.0.0.3', 'Overcast', 'OR', 'AD-504', '2026-07-01 12:10:00'),
('DL-024', 'EP-100', '11.0.0.4', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-01 12:15:00'),
('DL-025', 'EP-100', '11.0.0.5', 'Spotify', 'CA', 'AD-502', '2026-07-01 12:20:00'),
('DL-026', 'EP-101', '12.0.0.1', 'ApplePodcasts', 'IL', 'AD-505', '2026-07-02 13:00:00'),
('DL-027', 'EP-101', '12.0.0.2', 'Spotify', 'IL', 'AD-505', '2026-07-02 13:05:00'),
('DL-028', 'EP-101', '12.0.0.3', 'Overcast', 'IL', 'AD-505', '2026-07-02 13:10:00'),
('DL-029', 'EP-101', '12.0.0.4', 'ApplePodcasts', 'NY', 'AD-500', '2026-07-02 13:15:00'),
('DL-030', 'EP-101', '12.0.0.5', 'Spotify', 'CA', 'AD-502', '2026-07-02 13:20:00'),
('DL-031', 'EP-102', '13.0.0.1', 'ApplePodcasts', 'MA', 'AD-506', '2026-07-03 14:00:00'),
('DL-032', 'EP-102', '13.0.0.2', 'Spotify', 'MA', 'AD-506', '2026-07-03 14:05:00'),
('DL-033', 'EP-102', '13.0.0.3', 'Overcast', 'NY', 'AD-500', '2026-07-03 14:10:00'),
('DL-034', 'EP-102', '13.0.0.4', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-03 14:15:00'),
('DL-035', 'EP-102', '13.0.0.5', 'Spotify', 'FL', 'AD-503', '2026-07-03 14:20:00'),
('DL-036', 'EP-100', '14.0.0.1', 'ApplePodcasts', 'AZ', 'AD-507', '2026-07-01 15:00:00'),
('DL-037', 'EP-100', '14.0.0.2', 'Spotify', 'AZ', 'AD-507', '2026-07-01 15:05:00'),
('DL-038', 'EP-100', '14.0.0.3', 'Overcast', 'NY', 'AD-500', '2026-07-01 15:10:00'),
('DL-039', 'EP-100', '14.0.0.4', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-01 15:15:00'),
('DL-040', 'EP-100', '14.0.0.5', 'Spotify', 'TX', 'AD-501', '2026-07-01 15:20:00'),
('DL-041', 'EP-101', '15.0.0.1', 'ApplePodcasts', 'CO', 'AD-508', '2026-07-02 16:00:00'),
('DL-042', 'EP-101', '15.0.0.2', 'Spotify', 'CO', 'AD-508', '2026-07-02 16:05:00'),
('DL-043', 'EP-101', '15.0.0.3', 'Overcast', 'NY', 'AD-500', '2026-07-02 16:10:00'),
('DL-044', 'EP-101', '15.0.0.4', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-02 16:15:00'),
('DL-045', 'EP-101', '15.0.0.5', 'Spotify', 'FL', 'AD-503', '2026-07-02 16:20:00'),
('DL-046', 'EP-102', '16.0.0.1', 'ApplePodcasts', 'GA', 'AD-509', '2026-07-03 17:00:00'),
('DL-047', 'EP-102', '16.0.0.2', 'Spotify', 'GA', 'AD-509', '2026-07-03 17:05:00'),
('DL-048', 'EP-102', '16.0.0.3', 'Overcast', 'NY', 'AD-500', '2026-07-03 17:10:00'),
('DL-049', 'EP-102', '16.0.0.4', 'ApplePodcasts', 'CA', 'AD-502', '2026-07-03 17:15:00'),
('DL-050', 'EP-102', '16.0.0.5', 'Spotify', 'TX', 'AD-501', '2026-07-03 17:20:00');

-- WebConversions Table
CREATE TABLE IF NOT EXISTS PodcastAdDB.Analytics.Bronze.WebConversions (
    ConversionID VARCHAR,
    IPAddress VARCHAR,
    PromoCode VARCHAR,
    PurchaseAmount DOUBLE,
    ConversionTimestamp TIMESTAMP
);

INSERT INTO PodcastAdDB.Analytics.Bronze.WebConversions VALUES
('C-001', '192.168.1.1', 'PODCAST20', 49.99, '2026-07-01 12:00:00'),
('C-002', '192.168.1.5', 'PODCAST20', 99.99, '2026-07-02 14:00:00'),
('C-003', '10.0.0.1', 'PODCAST20', 19.99, '2026-07-01 15:00:00'),
('C-004', '1.1.1.1', 'PODCAST20', 50.00, '2026-07-05 09:00:00'), -- Unattributed
('C-005', '11.0.0.1', 'PODCAST20', 29.99, '2026-07-01 18:00:00'),
('C-006', '12.0.0.1', 'PODCAST20', 39.99, '2026-07-02 19:00:00'),
('C-007', '13.0.0.1', 'PODCAST20', 59.99, '2026-07-03 20:00:00'),
('C-008', '14.0.0.1', 'PODCAST20', 69.99, '2026-07-01 21:00:00'),
('C-009', '15.0.0.1', 'PODCAST20', 79.99, '2026-07-02 22:00:00'),
('C-010', '16.0.0.1', 'PODCAST20', 89.99, '2026-07-03 23:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Attribution Logic
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PodcastAdDB.Analytics.Silver.AttributedConversions AS
SELECT 
    d.DownloadID,
    d.EpisodeID,
    d.AdInsertedID,
    c.ConversionID,
    c.PurchaseAmount,
    d.GeoRegion,
    -- Attribution Window Check (e.g. 7 days)
    CASE 
        WHEN c.ConversionID IS NOT NULL 
             AND (EXTRACT(EPOCH FROM c.ConversionTimestamp) - EXTRACT(EPOCH FROM d.DownloadTimestamp)) < 604800 -- 7 days in seconds
        THEN 'Attributed'
        ELSE 'Unattributed'
    END AS AttributionStatus
FROM PodcastAdDB.Analytics.Bronze.DownloadLogs d
LEFT JOIN PodcastAdDB.Analytics.Bronze.WebConversions c ON d.IPAddress = c.IPAddress;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Campaign ROAS
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW PodcastAdDB.Analytics.Gold.AdPerformance AS
SELECT 
    AdInsertedID,
    COUNT(DownloadID) AS Impressions,
    COUNT(ConversionID) AS Conversions,
    SUM(PurchaseAmount) AS TotalRevenue,
    CAST(COUNT(ConversionID) AS DOUBLE) / COUNT(DownloadID) * 100 AS ConversionRatePct
FROM PodcastAdDB.Analytics.Silver.AttributedConversions
WHERE AttributionStatus = 'Attributed'
GROUP BY AdInsertedID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Calculate the total revenue generated by AdInsertedID 'AD-500' in the Gold performance view."

PROMPT 2:
"List the top 3 GeoRegions with the most downloads for 'EP-100'."

PROMPT 3:
"Find all conversions that occurred within 24 hours of a download in the Silver layer."
*/
