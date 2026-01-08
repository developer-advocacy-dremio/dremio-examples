/*
 * AdTech Campaign Performance Demo
 * 
 * Scenario:
 * A digital marketing agency tracks ad impressions and clicks.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize Ad Spend (ROAS) and Click-Through Rate (CTR).
 * 
 * Note: Assumes a catalog named 'AdTechDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS AdTechDB;
CREATE FOLDER IF NOT EXISTS AdTechDB.Bronze;
CREATE FOLDER IF NOT EXISTS AdTechDB.Silver;
CREATE FOLDER IF NOT EXISTS AdTechDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Tracking Pixels
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AdTechDB.Bronze.Campaigns (
    CampaignID INT,
    Name VARCHAR,
    Platform VARCHAR, -- 'Social', 'Search', 'Display'
    Budget DOUBLE
);

CREATE TABLE IF NOT EXISTS AdTechDB.Bronze.AdImpressions (
    ImpressionID VARCHAR,
    CampaignID INT,
    "Timestamp" TIMESTAMP,
    UserSegment VARCHAR,
    Cost DOUBLE -- CPM cost
);

CREATE TABLE IF NOT EXISTS AdTechDB.Bronze.Clicks (
    ClickID VARCHAR,
    ImpressionID VARCHAR,
    "Timestamp" TIMESTAMP,
    Revenue DOUBLE -- Conversion Value
);

-- 1.2 Populate Bronze Tables
INSERT INTO AdTechDB.Bronze.Campaigns (CampaignID, Name, Platform, Budget) VALUES
(101, 'Summer Sale', 'Social', 5000.00),
(102, 'Back to School', 'Search', 8000.00);

INSERT INTO AdTechDB.Bronze.AdImpressions (ImpressionID, CampaignID, "Timestamp", UserSegment, Cost) VALUES
('IMP-1', 101, '2025-06-01 10:00:00', 'Youth', 0.05),
('IMP-2', 101, '2025-06-01 10:05:00', 'Youth', 0.05),
('IMP-3', 102, '2025-06-01 11:00:00', 'Parent', 0.10),
('IMP-4', 102, '2025-06-01 11:15:00', 'Parent', 0.10),
('IMP-5', 101, '2025-06-01 12:00:00', 'Adult', 0.05);

INSERT INTO AdTechDB.Bronze.Clicks (ClickID, ImpressionID, "Timestamp", Revenue) VALUES
('CLK-1', 'IMP-1', '2025-06-01 10:01:00', 50.00),
('CLK-2', 'IMP-3', '2025-06-01 11:05:00', 120.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Funnel Data
-------------------------------------------------------------------------------

-- 2.1 View: Impression_Click_Join
-- Joins impressions to clicks to confirm attribution.
CREATE OR REPLACE VIEW AdTechDB.Silver.Impression_Click_Join AS
SELECT
    i.CampaignID,
    i.ImpressionID,
    i.UserSegment,
    i.Cost,
    c.ClickID,
    c.Revenue,
    CASE WHEN c.ClickID IS NOT NULL THEN 1 ELSE 0 END AS IsClicked
FROM AdTechDB.Bronze.AdImpressions i
LEFT JOIN AdTechDB.Bronze.Clicks c ON i.ImpressionID = c.ImpressionID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Campaign ROI
-------------------------------------------------------------------------------

-- 3.1 View: Campaign_Stats
-- Calculates CTR and ROAS.
CREATE OR REPLACE VIEW AdTechDB.Gold.Campaign_Stats AS
SELECT
    c.Name,
    c.Platform,
    COUNT(j.ImpressionID) AS Impressions,
    SUM(j.IsClicked) AS Clicks,
    (CAST(SUM(j.IsClicked) AS DOUBLE) / COUNT(j.ImpressionID)) * 100 AS CTR_Percent,
    SUM(j.Cost) AS TotalSpend,
    SUM(COALESCE(j.Revenue, 0)) AS TotalRevenue,
    (SUM(COALESCE(j.Revenue, 0)) / NULLIF(SUM(j.Cost), 0)) AS ROAS
FROM AdTechDB.Silver.Impression_Click_Join j
JOIN AdTechDB.Bronze.Campaigns c ON j.CampaignID = c.CampaignID
GROUP BY c.Name, c.Platform;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Best ROI):
"Which campaign has the highest ROAS in AdTechDB.Gold.Campaign_Stats?"

PROMPT 2 (Engagement):
"What is the average CTR_Percent per Platform?"
*/
