-------------------------------------------------------------------------------
-- Dremio SQL Examples: System Tables - Job Analytics
-- 
-- This script demonstrates how to query the jobs history to analyze workload
-- performance, errors, and user activity.
-- Note: 'sys.jobs' (Software) vs 'sys.project.jobs' (Cloud). 
-- This script uses 'sys.jobs' as the default reference.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. IDENTIFYING SLOW QUERIES
-- Find queries that took longer than 10 seconds.
-------------------------------------------------------------------------------

SELECT 
    job_id,
    user_name,
    status,
    start_time,
    end_time,
    duration_ms,
    -- Calculate generic duration in seconds for readability
    (end_time - start_time) / 1000.0 AS Duration_Seconds,
    query_type,
    query_text
FROM sys.jobs
WHERE duration_ms > 10000 -- 10 seconds
  AND start_time > DATE_SUB(CURRENT_DATE, 1) -- Last 24 hours
ORDER BY duration_ms DESC
LIMIT 20;

-------------------------------------------------------------------------------
-- 2. ANALYZING RESOURCE USAGE
-- Identify queries with high memory consumption or scanned data.
-- Note: Columns like 'input_records' or 'memory_allocated' may vary by version.
-------------------------------------------------------------------------------

SELECT 
    job_id,
    user_name,
    input_records,
    input_bytes,
    -- If memory stats are available in your version:
    -- memory_allocated_bytes,
    query_text
FROM sys.jobs
WHERE input_bytes > 1073741824 -- > 1 GB Scanned
ORDER BY input_bytes DESC;

-------------------------------------------------------------------------------
-- 3. ERROR ANALYSIS
-- Find the most common error messages to debug system issues.
-------------------------------------------------------------------------------

SELECT 
    error_msg,
    COUNT(*) AS Failure_Count,
    MIN(start_time) AS First_Seen,
    MAX(start_time) AS Last_Seen
FROM sys.jobs
WHERE status = 'FAILED'
GROUP BY error_msg
ORDER BY Failure_Count DESC;

-------------------------------------------------------------------------------
-- 4. USER ACTIVITY AUDIT
-- Who is running the most queries?
-------------------------------------------------------------------------------

SELECT 
    user_name,
    COUNT(*) AS Total_Jobs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS Failed_Jobs,
    AVG(duration_ms) AS Avg_Duration_Ms
FROM sys.jobs
GROUP BY user_name
ORDER BY Total_Jobs DESC;
