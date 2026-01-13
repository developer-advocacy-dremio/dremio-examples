/*
 * Content Moderation Queue Demo
 * 
 * Scenario:
 * Managing a queue of user-generated content flagged for review (AI + Human loop).
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Prioritize high-risk content and measure moderator throughput.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SocialMediaDB;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Bronze;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Silver;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SocialMediaDB.Bronze.Content (
    ContentID INT,
    UserID INT,
    Type VARCHAR, -- 'Post', 'Comment', 'Image'
    TextBody VARCHAR,
    PostTime TIMESTAMP,
    AIScore DOUBLE -- 0.0 to 1.0 (1.0 = High prob of violation)
);

CREATE TABLE IF NOT EXISTS SocialMediaDB.Bronze.ModerationLog (
    ActionID INT,
    ContentID INT,
    ModeratorID INT,
    ActionTime TIMESTAMP,
    Decision VARCHAR -- 'Approve', 'Remove', 'Escalate'
);

INSERT INTO SocialMediaDB.Bronze.Content VALUES
(1, 101, 'Post', 'Hello world!', '2025-01-01 10:00:00', 0.01),
(2, 102, 'Comment', 'Spam link: click here', '2025-01-01 10:05:00', 0.95),
(3, 103, 'Image', 'Binary data...', '2025-01-01 10:10:00', 0.88),
(4, 104, 'Post', 'Controversial opinion', '2025-01-01 10:15:00', 0.45);

INSERT INTO SocialMediaDB.Bronze.ModerationLog VALUES
(1, 2, 501, '2025-01-01 10:10:00', 'Remove'),
(2, 3, 502, '2025-01-01 10:12:00', 'Escalate');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SocialMediaDB.Silver.ModerationQueue AS
SELECT 
    c.ContentID,
    c.Type,
    c.AIScore,
    c.PostTime,
    CASE 
        WHEN m.Decision IS NULL THEN 'Pending'
        ELSE 'Reviewed' 
    END AS Status,
    m.Decision,
    m.ModeratorID
FROM SocialMediaDB.Bronze.Content c
LEFT JOIN SocialMediaDB.Bronze.ModerationLog m ON c.ContentID = m.ContentID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SocialMediaDB.Gold.ComplianceMetrics AS
SELECT 
    CAST(PostTime AS DATE) AS PostDate,
    COUNT(*) AS TotalContent,
    -- Auto-flagged by AI (>0.8)
    SUM(CASE WHEN AIScore > 0.8 THEN 1 ELSE 0 END) AS HighRiskItems,
    -- Moderator Action Rate
    SUM(CASE WHEN Status = 'Reviewed' THEN 1 ELSE 0 END) AS ReviewedItems,
    AVG(CASE WHEN Status = 'Reviewed' THEN AIScore ELSE NULL END) AS AvgRiskOfReviewed
FROM SocialMediaDB.Silver.ModerationQueue
GROUP BY CAST(PostTime AS DATE);

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show me pending items in SocialMediaDB.Silver.ModerationQueue sorted by AIScore descending."
*/
