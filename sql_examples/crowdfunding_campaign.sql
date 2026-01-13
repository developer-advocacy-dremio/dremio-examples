/*
 * Crowdfunding Campaign Demo
 * 
 * Scenario:
 * Tracking backers, daily pledges, and campaign goal progress.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Predict campaign success and identify top backer demographics.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS CrowdFundDB;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Bronze;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Silver;
CREATE FOLDER IF NOT EXISTS CrowdFundDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CrowdFundDB.Bronze.Campaigns (
    CampaignID INT,
    Title VARCHAR,
    Category VARCHAR,
    GoalAmount DOUBLE,
    StartDate DATE,
    EndDate DATE
);

CREATE TABLE IF NOT EXISTS CrowdFundDB.Bronze.Pledges (
    PledgeID INT,
    CampaignID INT,
    BackerID INT,
    Amount DOUBLE,
    PledgeDate TIMESTAMP,
    Status VARCHAR -- 'Collected', 'Failed', 'Refunded'
);

INSERT INTO CrowdFundDB.Bronze.Campaigns VALUES
(1, 'Smart Watch X', 'Technology', 50000.0, '2025-01-01', '2025-01-31'),
(2, 'Indie Movie Z', 'Film', 20000.0, '2025-02-01', '2025-03-01');

INSERT INTO CrowdFundDB.Bronze.Pledges VALUES
(1, 1, 101, 200.0, '2025-01-02 10:00:00', 'Collected'),
(2, 1, 102, 50.0, '2025-01-03 14:00:00', 'Collected'),
(3, 1, 103, 5000.0, '2025-01-05 09:00:00', 'Failed'), -- Card error
(4, 2, 101, 100.0, '2025-02-02 12:00:00', 'Collected');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CrowdFundDB.Silver.DailyPledges AS
SELECT 
    CampaignID,
    CAST(PledgeDate AS DATE) AS PledgedDay,
    COUNT(PledgeID) AS BackerCount,
    SUM(Amount) AS DailyTotal,
    SUM(CASE WHEN Status = 'Collected' THEN Amount ELSE 0 END) AS ValidPledgedAmount
FROM CrowdFundDB.Bronze.Pledges
GROUP BY CampaignID, CAST(PledgeDate AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CrowdFundDB.Gold.CampaignStatus AS
SELECT 
    c.Title,
    c.GoalAmount,
    SUM(dp.ValidPledgedAmount) AS TotalRaised,
    (SUM(dp.ValidPledgedAmount) / c.GoalAmount) * 100 AS PercentFunded
FROM CrowdFundDB.Bronze.Campaigns c
JOIN CrowdFundDB.Silver.DailyPledges dp ON c.CampaignID = dp.CampaignID
GROUP BY c.Title, c.GoalAmount;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which campaigns in CrowdFundDB.Gold.CampaignStatus have reached more than 80% of their goal?"
*/
