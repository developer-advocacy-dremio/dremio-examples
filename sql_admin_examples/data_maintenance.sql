-------------------------------------------------------------------------------
-- Dremio SQL Admin Examples: Data Maintenance (Iceberg)
-- 
-- This script demonstrates table maintenance tasks, specifically for 
-- Apache Iceberg tables in Dremio.
-- Scenarios include:
-- 1. Optimization (Compaction)
-- 2. Vacuuming (Cleanup)
-- 3. Time Travel & Rollback
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 0. SETUP: Create Mock Data
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure a space named 'ManufactureDB' exists.
CREATE FOLDER IF NOT EXISTS "ManufactureDB"."Maintenance_Demo";

CREATE TABLE IF NOT EXISTS "ManufactureDB"."Maintenance_Demo"."IoT_Sensors" (
    SensorID INT, 
    MachineID INT, 
    "Date" DATE, 
    Value DOUBLE
);

-- Insert dummy data to enable Optimization
INSERT INTO "ManufactureDB"."Maintenance_Demo"."IoT_Sensors" VALUES
(1, 101, CURRENT_DATE, 98.6),
(2, 102, CURRENT_DATE, 102.4),
(1, 101, DATE_SUB(CURRENT_DATE, 1), 99.1);

-------------------------------------------------------------------------------
-- 1. OPTIMIZATION
-- Merges small files into larger ones to improve read performance.
-------------------------------------------------------------------------------


-- 1.1 Basic Compaction
-- Rewrites data files for the entire table.
OPTIMIZE TABLE "ManufactureDB"."Maintenance_Demo"."IoT_Sensors";

-- 1.2 Targeted Compaction (Partition Filtering)
-- Only optimize recent data (e.g., today's partition).
OPTIMIZE TABLE "ManufactureDB"."Maintenance_Demo"."IoT_Sensors"
REWRITE DATA (MIN_INPUT_FILES=5) -- Only compact if at least 5 small files exist
WHERE "Date" = CURRENT_DATE;

-- 1.3 Rewriting Manifests
-- Optimizes metadata processing by merging manifest files.
OPTIMIZE TABLE "ManufactureDB"."Maintenance_Demo"."IoT_Sensors" REWRITE MANIFESTS;

-------------------------------------------------------------------------------
-- 2. VACUUM (Cleanup)
-- Removes unused data files and expires old snapshots to free space.
-------------------------------------------------------------------------------

-- 2.1 Expire Snapshots
-- Removes snapshots older than 7 days.
VACUUM TABLE "ManufactureDB"."Maintenance_Demo"."IoT_Sensors"
EXPIRE SNAPSHOTS OLDER_THAN '2025-01-01 00:00:00'; -- Fixed Timestamp syntax

-- 2.2 Remove Orphan Files
-- Deletes files not referenced by any valid snapshot (Garbage Collection).
VACUUM TABLE "ManufactureDB"."Maintenance_Demo"."IoT_Sensors" REMOVE ORPHAN FILES;

-------------------------------------------------------------------------------
-- 3. ROLLBACK (Time Travel Recovery)
-- Reverts the table state to a previous version effectively.
-------------------------------------------------------------------------------

-- 3.1 Rollback to specific Snapshot ID
-- Useful if a bad INSERT/UPDATE occurred.
ROLLBACK TABLE "ManufactureDB"."IoT_Sensors" TO SNAPSHOT '1234567890123456789';

-- 3.2 Rollback to Timestamp
-- Reverts to the state at a specific time.
ROLLBACK TABLE "ManufactureDB"."IoT_Sensors" TO TIMESTAMP '2025-01-07 12:00:00';

-------------------------------------------------------------------------------
-- 4. ANALYZE (Statistics)
-- Computes statistics to help the Cost-Based Optimizer (CBO).
-------------------------------------------------------------------------------

-- 4.1 Compute Statistics for all columns
ANALYZE TABLE "ManufactureDB"."IoT_Sensors" COMPUTE STATISTICS;

-- 4.2 Compute Statistics for specific join keys
ANALYZE TABLE "ManufactureDB"."IoT_Sensors" 
COMPUTE STATISTICS FOR COLUMNS (SensorID, MachineID);
