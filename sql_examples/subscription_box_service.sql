/*
 * Subscription Box Service Demo
 * 
 * Scenario:
 * Tracking active subscriptions, churn, and box customization preferences.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Increase retention and upsell value.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SubBoxDB;
CREATE FOLDER IF NOT EXISTS SubBoxDB.Bronze;
CREATE FOLDER IF NOT EXISTS SubBoxDB.Silver;
CREATE FOLDER IF NOT EXISTS SubBoxDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SubBoxDB.Bronze.Subscribers (
    SubID INT,
    PlanType VARCHAR, -- 'Basic', 'Premium'
    StartDate DATE,
    Status VARCHAR -- 'Active', 'Cancelled'
);

CREATE TABLE IF NOT EXISTS SubBoxDB.Bronze.MonthlyBoxes (
    BoxID INT,
    SubID INT,
    MonthStr VARCHAR, -- '2025-01'
    Rating INT, -- 1-5
    ReturnedItems INT
);

INSERT INTO SubBoxDB.Bronze.Subscribers VALUES
(1, 'Premium', '2024-01-01', 'Active'),
(2, 'Basic', '2024-03-01', 'Cancelled');

INSERT INTO SubBoxDB.Bronze.MonthlyBoxes VALUES
(101, 1, '2025-01', 5, 0),
(102, 2, '2025-01', 2, 2); -- Bad experience -> Churn?

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SubBoxDB.Silver.CustomerHealth AS
SELECT 
    s.SubID,
    s.PlanType,
    s.Status,
    COUNT(b.BoxID) AS BoxesReceived,
    AVG(b.Rating) AS AvgRating,
    SUM(b.ReturnedItems) AS TotalReturns
FROM SubBoxDB.Bronze.Subscribers s
LEFT JOIN SubBoxDB.Bronze.MonthlyBoxes b ON s.SubID = b.SubID
GROUP BY s.SubID, s.PlanType, s.Status;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SubBoxDB.Gold.ChurnAnalysis AS
SELECT 
    PlanType,
    COUNT(SubID) AS TotalSubs,
    SUM(CASE WHEN Status = 'Cancelled' THEN 1 ELSE 0 END) AS ChurnedSubs,
    AVG(AvgRating) AS AvgRatingForPlan
FROM SubBoxDB.Silver.CustomerHealth
GROUP BY PlanType;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which PlanType has the highest churn rate in SubBoxDB.Gold.ChurnAnalysis?"
*/
