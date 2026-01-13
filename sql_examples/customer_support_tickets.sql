/*
 * Customer Support Ticket Analysis Demo
 * 
 * Scenario:
 * Analyzing ticket volume, resolution time, and CSAT scores.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Improve support team efficiency and customer satisfaction.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SupportDB;
CREATE FOLDER IF NOT EXISTS SupportDB.Bronze;
CREATE FOLDER IF NOT EXISTS SupportDB.Silver;
CREATE FOLDER IF NOT EXISTS SupportDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS SupportDB.Bronze.Tickets (
    TicketID INT,
    CustomerID INT,
    Category VARCHAR,
    Priority VARCHAR, -- 'Low', 'Medium', 'High', 'Critical'
    CreatedTime TIMESTAMP,
    ResolvedTime TIMESTAMP,
    Status VARCHAR -- 'Open', 'Closed', 'Escalated'
);

CREATE TABLE IF NOT EXISTS SupportDB.Bronze.Surveys (
    SurveyID INT,
    TicketID INT,
    CSAT_Score INT, -- 1-5
    Comments VARCHAR
);

INSERT INTO SupportDB.Bronze.Tickets VALUES
(1, 101, 'Billing', 'Medium', '2025-01-01 09:00:00', '2025-01-01 10:00:00', 'Closed'),
(2, 102, 'Technical', 'High', '2025-01-01 09:30:00', '2025-01-02 09:30:00', 'Closed'), -- 24h sla
(3, 103, 'Login', 'Low', '2025-01-02 08:00:00', NULL, 'Open'),
(4, 104, 'Billing', 'Critical', '2025-01-02 10:00:00', '2025-01-02 10:30:00', 'Closed');

INSERT INTO SupportDB.Bronze.Surveys VALUES
(1, 1, 5, 'Fast service'),
(2, 2, 3, 'Took too long'),
(3, 4, 5, 'Great help');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SupportDB.Silver.TicketMetrics AS
SELECT 
    t.TicketID,
    t.Category,
    t.Priority,
    t.CreatedTime,
    t.ResolvedTime,
    TIMESTAMPDIFF(MINUTE, t.CreatedTime, t.ResolvedTime) AS ResolutionTimeMinutes,
    s.CSAT_Score
FROM SupportDB.Bronze.Tickets t
LEFT JOIN SupportDB.Bronze.Surveys s ON t.TicketID = s.TicketID
WHERE t.Status = 'Closed';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SupportDB.Gold.TeamPerformance AS
SELECT 
    Category,
    COUNT(TicketID) AS TotalTickets,
    AVG(ResolutionTimeMinutes) AS AvgResolutionTime,
    AVG(CSAT_Score) AS AvgCSAT
FROM SupportDB.Silver.TicketMetrics
GROUP BY Category;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Category has the lowest AvgCSAT in SupportDB.Gold.TeamPerformance?"
*/
