-------------------------------------------------------------------------------
-- Dremio SQL Examples: Continuous Ingestion - PIPES
-- 
-- Pipes automate the loading of data from object storage as soon as files land.
-- This provides a "near real-time" low-latency ingestion path without manual jobs.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Target Table
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS IngestionDB;

CREATE TABLE IF NOT EXISTS IngestionDB.LiveEvents (
    EventID VARCHAR,
    EventType VARCHAR,
    EventTime TIMESTAMP,
    Payload VARCHAR
);

-------------------------------------------------------------------------------
-- 2. CREATE A PIPE
-- The pipe wraps a COPY INTO command. Dremio will monitor the source (or use notification)
-- to trigger this command on new files.
-------------------------------------------------------------------------------

CREATE PIPE IF NOT EXISTS IngestionDB.EventPipe
AS COPY INTO IngestionDB.LiveEvents
FROM '@S3_Source/streaming/events/'
FILE_FORMAT 'json';

-------------------------------------------------------------------------------
-- 3. MONITORING PIPE STATUS
-- Check if the pipe is running and how many files it has processed.
-------------------------------------------------------------------------------

-- 3.1 Check Definition
DESCRIBE PIPE IngestionDB.EventPipe;

-- 3.2 View History (In some versions dependent on sys tables)
-- SELECT * FROM sys.generated_pipe_history WHERE pipe_name = 'IngestionDB.EventPipe';

-------------------------------------------------------------------------------
-- 4. MANAGING PIPES
-------------------------------------------------------------------------------

-- Pause the pipe (stop ingesting new files)
ALTER PIPE IngestionDB.EventPipe SET PIPE_EXECUTION_PAUSED = true;

-- Resume the pipe
ALTER PIPE IngestionDB.EventPipe SET PIPE_EXECUTION_PAUSED = false;

-- Drop the pipe (does not drop the target table)
DROP PIPE IF EXISTS IngestionDB.EventPipe;
