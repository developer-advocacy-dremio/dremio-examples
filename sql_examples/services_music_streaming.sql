/*
    Dremio High-Volume SQL Pattern: Services Music Streaming Analysis
    
    Business Scenario:
    A music streaming service is testing two recommendation algorithms (Algo-A vs Algo-B).
    We track "Skip Rates" and "Completion Rates" to determine the winner.
    
    Data Story:
    - Bronze: StreamLogs (Song plays), AB_Assignments (User buckets).
    - Silver: SessionMetrics (Skip vs Complete per song).
    - Gold: AlgorithmPerformance (Aggregate win/loss).
    
    Medallion Architecture:
    - Bronze: StreamLogs, AB_Assignments.
      *Volume*: 50+ records.
    - Silver: SessionMetrics.
    - Gold: AlgorithmPerformance.
    
    Key Dremio Features:
    - Conditional Aggregation
    - Group By Algorithm
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.AB_Assignments (
    UserID STRING,
    AlgorithmGroup STRING -- A (Control), B (New)
);

INSERT INTO ServicesDB.Bronze.AB_Assignments VALUES
('U101', 'A'), ('U102', 'B'), ('U103', 'A'), ('U104', 'B'),
('U105', 'A'), ('U106', 'B'), ('U107', 'A'), ('U108', 'B'),
('U109', 'A'), ('U110', 'B');

CREATE OR REPLACE TABLE ServicesDB.Bronze.StreamLogs (
    LogID INT,
    UserID STRING,
    SongID STRING,
    DurationSeconds INT,
    PlayedSeconds INT,
    Result STRING -- Completed, Skipped
);

INSERT INTO ServicesDB.Bronze.StreamLogs VALUES
-- Algo A (Control) - Mixed Performance
(1, 'U101', 'S001', 200, 200, 'Completed'),
(2, 'U101', 'S002', 180, 10, 'Skipped'),
(3, 'U101', 'S003', 210, 210, 'Completed'),
(4, 'U103', 'S001', 200, 200, 'Completed'),
(5, 'U103', 'S004', 240, 20, 'Skipped'),
(6, 'U105', 'S005', 180, 180, 'Completed'),
(7, 'U105', 'S002', 180, 5, 'Skipped'),
(8, 'U107', 'S001', 200, 200, 'Completed'),
(9, 'U107', 'S006', 300, 30, 'Skipped'),
(10, 'U109', 'S003', 210, 210, 'Completed'),
(11, 'U101', 'S007', 200, 15, 'Skipped'),
(12, 'U103', 'S008', 180, 180, 'Completed'),
(13, 'U105', 'S009', 240, 240, 'Completed'),
(14, 'U107', 'S010', 220, 10, 'Skipped'),
(15, 'U109', 'S001', 200, 190, 'Completed'), -- Near complete
(16, 'U101', 'S005', 180, 180, 'Completed'),
(17, 'U103', 'S002', 180, 5, 'Skipped'),
(18, 'U105', 'S006', 300, 300, 'Completed'),
(19, 'U107', 'S003', 210, 210, 'Completed'),
(20, 'U109', 'S004', 240, 10, 'Skipped'),

-- Algo B (New) - Better Performance (Less Skips)
(21, 'U102', 'S001', 200, 200, 'Completed'),
(22, 'U102', 'S002', 180, 180, 'Completed'), -- Improved
(23, 'U102', 'S003', 210, 210, 'Completed'),
(24, 'U104', 'S001', 200, 200, 'Completed'),
(25, 'U104', 'S004', 240, 200, 'Completed'), -- Improved
(26, 'U106', 'S005', 180, 180, 'Completed'),
(27, 'U106', 'S002', 180, 170, 'Completed'),
(28, 'U108', 'S001', 200, 200, 'Completed'),
(29, 'U108', 'S006', 300, 290, 'Completed'),
(30, 'U110', 'S003', 210, 210, 'Completed'),
(31, 'U102', 'S007', 200, 200, 'Completed'),
(32, 'U104', 'S008', 180, 180, 'Completed'),
(33, 'U106', 'S009', 240, 240, 'Completed'),
(34, 'U108', 'S010', 220, 220, 'Completed'),
(35, 'U110', 'S001', 200, 200, 'Completed'),
(36, 'U102', 'S005', 180, 180, 'Completed'),
(37, 'U104', 'S002', 180, 180, 'Completed'),
(38, 'U106', 'S006', 300, 300, 'Completed'),
(39, 'U108', 'S003', 210, 210, 'Completed'),
(40, 'U110', 'S004', 240, 240, 'Completed'),

-- Mixed filler
(41, 'U101', 'S011', 200, 10, 'Skipped'),
(42, 'U102', 'S011', 200, 200, 'Completed'),
(43, 'U103', 'S012', 180, 180, 'Completed'),
(44, 'U104', 'S012', 180, 180, 'Completed'),
(45, 'U105', 'S013', 240, 10, 'Skipped'),
(46, 'U106', 'S013', 240, 240, 'Completed'),
(47, 'U107', 'S014', 300, 10, 'Skipped'),
(48, 'U108', 'S014', 300, 300, 'Completed'),
(49, 'U109', 'S015', 210, 10, 'Skipped'),
(50, 'U110', 'S015', 210, 210, 'Completed');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Session Metrics
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.SessionMetrics AS
SELECT
    s.UserID,
    a.AlgorithmGroup,
    s.SongID,
    s.Result,
    s.PlayedSeconds,
    s.DurationSeconds,
    CAST(s.PlayedSeconds AS DOUBLE) / s.DurationSeconds AS PercentPlayed
FROM ServicesDB.Bronze.StreamLogs s
JOIN ServicesDB.Bronze.AB_Assignments a ON s.UserID = a.UserID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Algorithm Performance
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.AlgorithmPerformance AS
SELECT
    AlgorithmGroup,
    COUNT(*) AS TotalStreams,
    SUM(CASE WHEN Result = 'Completed' THEN 1 ELSE 0 END) AS CompletedCount,
    SUM(CASE WHEN Result = 'Skipped' THEN 1 ELSE 0 END) AS SkippedCount,
    AVG(PercentPlayed) * 100.0 AS AvgCompletionPct,
    (CAST(SUM(CASE WHEN Result = 'Skipped' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)) * 100.0 AS SkipRatePct
FROM ServicesDB.Silver.SessionMetrics
GROUP BY AlgorithmGroup;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which algorithm has the lower Skip Rate?"
    2. "Show average play percentage by Algorithm."
    3. "Count total streams per user group."
*/
