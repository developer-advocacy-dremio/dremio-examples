-------------------------------------------------------------------------------
-- Dremio SQL Examples: System Tables - Reflection Status
-- 
-- This script demonstrates monitoring Data Reflections via system tables.
-- Critical for ensuring acceleration is active and storage is managed.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. REFLECTION HEALTH CHECK
-- Identify reflections that are failing to refresh or invalid.
-------------------------------------------------------------------------------

SELECT 
    reflection_name,
    dataset_name,
    type, -- RAW or AGGREGATE
    status, -- OK, FAILED, INVALID, REFRESHING
    num_failures,
    last_refresh_duration_millis,
    last_refresh_from_pds -- Timestamp of the data consistency
FROM sys.reflections
WHERE status != 'OK' OR num_failures > 0
ORDER BY num_failures DESC;

-------------------------------------------------------------------------------
-- 2. STORAGE FOOTPRINT
-- Find the largest reflections consuming disk space.
-------------------------------------------------------------------------------

SELECT 
    reflection_name,
    dataset_name,
    total_size_bytes,
    (total_size_bytes / 1024.0 / 1024.0 / 1024.0) AS Size_GB,
    record_count
FROM sys.reflections
ORDER BY total_size_bytes DESC
LIMIT 10;

-------------------------------------------------------------------------------
-- 3. MATERIALIZATION HISTORY
-- Analyze the history of reflection refresh jobs (sys.materializations).
-- Useful for spotting trend in refresh times.
-------------------------------------------------------------------------------

SELECT 
    reflection_id,
    reflection_name,
    job_id AS Refresh_Job_ID,
    state, -- DONE, FAILED, CANCELED
    join_analysis, -- Optimization hints
    refresh_start,
    refresh_end
FROM sys.materializations
WHERE refresh_start > DATE_SUB(CURRENT_DATE, 7) -- Last 7 days
ORDER BY refresh_start DESC;
