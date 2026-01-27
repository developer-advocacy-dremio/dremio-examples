-------------------------------------------------------------------------------
-- Dremio SQL Examples: Iceberg Metadata - History & Snapshots
-- 
-- This script demonstrates how to inspect the history of an Iceberg table
-- in Dremio. This is critical for auditing changes, debugging data pipelines,
-- and identifying points for time-travel.
--
-- Note: These metadata tables act like system views specific to a table.
-- Syntax: SELECT * FROM TABLE( table_history( 'path.to.table' ) )
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data (Iceberg Table)
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure a space named 'IcebergMetadataDB' exists, or run within your Home Space.
CREATE FOLDER IF NOT EXISTS IcebergMetadataDB;

-- Create an Iceberg table
CREATE TABLE IF NOT EXISTS IcebergMetadataDB.AuditDemo (
    TxID INT,
    Value DOUBLE,
    UpdatedBy VARCHAR
);

-- Insert Initial Data (Snapshot 1)
INSERT INTO IcebergMetadataDB.AuditDemo VALUES (101, 50.0, 'System');

-- Update Data (Snapshot 2 - Copy-on-Write / Merge-on-Read ops)
UPDATE IcebergMetadataDB.AuditDemo SET Value = 55.0 WHERE TxID = 101;

-- Delete Data (Snapshot 3)
DELETE FROM IcebergMetadataDB.AuditDemo WHERE TxID = 101;

-------------------------------------------------------------------------------
-- 2. INSPECTING HISTORY
-- table_history() shows the sequence of snapshots committed to the table.
-------------------------------------------------------------------------------

-- 2.1 View all commits (Snapshots)
-- Returns: made_current_at, snapshot_id, parent_id, is_current_ancestor
SELECT * 
FROM TABLE( table_history( 'IcebergMetadataDB.AuditDemo' ) )
ORDER BY made_current_at DESC;

-------------------------------------------------------------------------------
-- 3. INSPECTING SNAPSHOT DETAILS
-- table_snapshot() provides deeper details about what each operation did.
-------------------------------------------------------------------------------

-- 3.1 View Snapshot Operation Types
-- Returns: operation (append, replace, overwrite, delete), summary map, manifest_list
SELECT 
    committed_at,
    snapshot_id,
    operation,
    summary['added-records'] AS Records_Added,
    summary['deleted-records'] AS Records_Deleted,
    summary['total-records'] AS Total_Records_After
FROM TABLE( table_snapshot( 'IcebergMetadataDB.AuditDemo' ) )
ORDER BY committed_at DESC;

-------------------------------------------------------------------------------
-- 4. PRACTICAL USE: TIME TRAVEL
-- Using the Snapshot ID from history to inspect state.
-------------------------------------------------------------------------------

-- 4.1 Query data as of the first insert (using dynamic snapshot lookup)
-- Note: In a real scenario, you'd paste the specific ID found above.
SELECT * 
FROM IcebergMetadataDB.AuditDemo 
AT SNAPSHOT '123456789'; -- Replace with actual ID from step 2.1

-- 4.2 Diffing Snapshots (Conceptual)
-- Join specific snapshots to see what changed between versions
/*
SELECT 
    Current.TxID,
    Current.Value AS CurrentValue,
    Prev.Value AS PreviousValue
FROM IcebergMetadataDB.AuditDemo AT SNAPSHOT 'CurrentID' Current
FULL OUTER JOIN IcebergMetadataDB.AuditDemo AT SNAPSHOT 'PreviousID' Prev
  ON Current.TxID = Prev.TxID
WHERE Current.Value != Prev.Value OR Current.Value IS NULL OR Prev.Value IS NULL;
*/
