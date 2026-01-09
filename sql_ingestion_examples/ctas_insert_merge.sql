/*
 * Advanced Ingestion & DML Patterns
 * 
 * This script demonstrates standard SQL capabilities for moving and updating data in Dremio.
 * 
 * Key Concepts:
 * 1. CTAS (Create Table As Select): Creating physical tables from query results.
 * 2. INSERT SELECT: Appending data from one table to another.
 * 3. Incremental Loading: Inserting only new records that don't already exist.
 * 4. MERGE INTO (Upsert): Atomically updating existing records and inserting new ones.
 * 
 * Prerequisites:
 * - A designated space for creating tables (e.g., 'RetailDB').
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Source and Target Tables
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RetailDB.Ingestion_Lab;

-- 0.1 Source Table (Bronze/Raw)
CREATE TABLE IF NOT EXISTS RetailDB.Ingestion_Lab.Raw_Clicks (
    ClickID INT,
    UserID INT,
    URL VARCHAR,
    Timestamp TIMESTAMP
);

-- Populate Source with initial batch
INSERT INTO RetailDB.Ingestion_Lab.Raw_Clicks VALUES
(1, 101, '/home', '2025-01-01 10:00:00'),
(2, 102, '/products', '2025-01-01 10:05:00'),
(3, 101, '/cart', '2025-01-01 10:15:00');

-------------------------------------------------------------------------------
-- 1. CTAS (Create Table As Select)
-- Use Case: Materializing a complex query or view into a physical table for performance.
-------------------------------------------------------------------------------

-- 1.1 Simple CTAS
CREATE TABLE RetailDB.Ingestion_Lab.Daily_Summary AS
SELECT 
    UserID, 
    COUNT(*) AS TotalClicks, 
    MAX(Timestamp) AS LastActive
FROM RetailDB.Ingestion_Lab.Raw_Clicks
GROUP BY UserID;

-- 1.2 CTAS with Partitioning (Best Practice for Performance)
-- Creating a partitioned table based on the 'Product' category helps with pruning during queries.
CREATE TABLE RetailDB.Ingestion_Lab.Clicks_Partitioned 
    PARTITION BY (UserID) 
AS SELECT * FROM RetailDB.Ingestion_Lab.Raw_Clicks;

-------------------------------------------------------------------------------
-- 2. INSERT SELECT (Batch Append)
-- Use Case: Appending a new batch of raw data into a main history table.
-------------------------------------------------------------------------------

-- 2.1 Create a History Table
CREATE TABLE IF NOT EXISTS RetailDB.Ingestion_Lab.Click_History (
    ClickID INT,
    UserID INT,
    URL VARCHAR,
    Timestamp TIMESTAMP,
    IngestionDate DATE
);

-- 2.2 Insert all records from Raw_Clicks into History
INSERT INTO RetailDB.Ingestion_Lab.Click_History
SELECT 
    *, 
    CURRENT_DATE AS IngestionDate 
FROM RetailDB.Ingestion_Lab.Raw_Clicks;

-------------------------------------------------------------------------------
-- 3. INCREMENTAL LOADING (Insert New Only)
-- Use Case: Running a nightly job that should only insert records that aren't already in the target.
-- Two common patterns: LEFT JOIN / WHERE NULL or NOT EXISTS.
-------------------------------------------------------------------------------

-- Simulate new data in Raw_Clicks
INSERT INTO RetailDB.Ingestion_Lab.Raw_Clicks VALUES
(4, 103, '/checkout', '2025-01-02 08:00:00'); -- New Record
-- Record 1, 2, 3 overlap with what we already inserted.

-- 3.1 Pattern: LEFT JOIN
INSERT INTO RetailDB.Ingestion_Lab.Click_History
SELECT 
    src.ClickID,
    src.UserID,
    src.URL,
    src.Timestamp,
    CURRENT_DATE
FROM RetailDB.Ingestion_Lab.Raw_Clicks src
LEFT JOIN RetailDB.Ingestion_Lab.Click_History tgt 
    ON src.ClickID = tgt.ClickID
WHERE tgt.ClickID IS NULL; 
-- Only ClickID 4 will be inserted.

-------------------------------------------------------------------------------
-- 4. MERGE INTO (Upsert)
-- Use Case: "Update if exists, Insert if new". Perfect for Dimension tables or synchronizing state.
-- Note: Requires Iceberg tables.
-------------------------------------------------------------------------------

-- 4.1 Setup: User Profiles Table
CREATE TABLE IF NOT EXISTS RetailDB.Ingestion_Lab.User_Profiles (
    UserID INT,
    Status VARCHAR,
    LastLogin TIMESTAMP
);

INSERT INTO RetailDB.Ingestion_Lab.User_Profiles VALUES
(101, 'Active', '2025-01-01 10:00:00'),
(102, 'Inactive', '2024-12-01 09:00:00');

-- 4.2 Source of Updates (e.g., from a recent log)
CREATE TABLE IF NOT EXISTS RetailDB.Ingestion_Lab.User_Updates (
    UserID INT,
    Status VARCHAR,
    CurrentTime TIMESTAMP
);

INSERT INTO RetailDB.Ingestion_Lab.User_Updates VALUES
(101, 'Super Active', '2025-01-02 12:00:00'), -- Update existing
(103, 'Active', '2025-01-02 12:00:00');       -- Insert new

-- 4.3 Execute MERGE
MERGE INTO RetailDB.Ingestion_Lab.User_Profiles AS target
USING RetailDB.Ingestion_Lab.User_Updates AS source
ON (target.UserID = source.UserID)
WHEN MATCHED THEN
    UPDATE SET 
        Status = source.Status, 
        LastLogin = source.CurrentTime
WHEN NOT MATCHED THEN
    INSERT (UserID, Status, LastLogin)
    VALUES (source.UserID, source.Status, source.CurrentTime);

-- Result:
-- User 101: Updated to 'Super Active'
-- User 102: Unchanged
-- User 103: Inserted as 'Active'
