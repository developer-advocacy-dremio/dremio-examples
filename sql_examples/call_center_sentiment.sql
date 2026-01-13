/*
 * Call Center Sentiment Demo
 * 
 * Scenario:
 * Analyzing call transcripts for customer emotions and agent effectiveness.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve agent training and reduce customer churn.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS CallCenterDB;
CREATE FOLDER IF NOT EXISTS CallCenterDB.Bronze;
CREATE FOLDER IF NOT EXISTS CallCenterDB.Silver;
CREATE FOLDER IF NOT EXISTS CallCenterDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS CallCenterDB.Bronze.Calls (
    CallID VARCHAR,
    AgentID INT,
    CustomerID INT,
    StartTime TIMESTAMP,
    DurationSeconds INT,
    TranscriptText VARCHAR,
    SentimentScore DOUBLE -- -1.0 (Negative) to 1.0 (Positive)
);

INSERT INTO CallCenterDB.Bronze.Calls VALUES
('C-001', 101, 500, '2025-03-01 09:00:00', 300, 'Thank you for your help...', 0.8),
('C-002', 102, 501, '2025-03-01 09:15:00', 600, 'I am very frustrated...', -0.9),
('C-003', 101, 502, '2025-03-01 10:00:00', 180, 'Issue resolved quickly.', 0.5);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CallCenterDB.Silver.AgentPerformance AS
SELECT 
    AgentID,
    CallID,
    DurationSeconds,
    SentimentScore,
    CASE 
        WHEN SentimentScore < -0.5 THEN 'Negative'
        WHEN SentimentScore > 0.5 THEN 'Positive'
        ELSE 'Neutral' 
    END AS SentimentCategory
FROM CallCenterDB.Bronze.Calls;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW CallCenterDB.Gold.AgentRanking AS
SELECT 
    AgentID,
    COUNT(CallID) AS TotalCalls,
    AVG(SentimentScore) AS AvgSentiment,
    AVG(DurationSeconds) AS AvgHandleTime
FROM CallCenterDB.Silver.AgentPerformance
GROUP BY AgentID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which AgentID has the highest AvgSentiment in CallCenterDB.Gold.AgentRanking?"
*/
