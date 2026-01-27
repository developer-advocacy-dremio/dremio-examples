-------------------------------------------------------------------------------
-- Dremio SQL Examples: Iceberg Metadata - Files & Partitions
-- 
-- This script demonstrates scanning the physical file layout of an Iceberg table.
-- Useful for identifying "Small File Problems", partition skew, and storage costs.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data (Partitioned Iceberg Table)
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure a space named 'IcebergMetadataDB' exists, or run within your Home Space.
CREATE FOLDER IF NOT EXISTS IcebergMetadataDB;

-- Create a partitioned Iceberg table
CREATE TABLE IF NOT EXISTS IcebergMetadataDB.SensorLogs (
    LogID INT,
    Region VARCHAR,
    LogDate DATE,
    Message VARCHAR
) PARTITION BY (Region, LogDate);

-- Insert Data across multiple partitions
INSERT INTO IcebergMetadataDB.SensorLogs VALUES 
(1, 'US-East', '2025-01-01', 'Startup'),
(2, 'US-East', '2025-01-01', 'Running'),
(3, 'US-West', '2025-01-01', 'Startup'),
(4, 'US-East', '2025-01-02', 'Error');

-------------------------------------------------------------------------------
-- 2. INSPECTING DATA FILES
-- table_files() lists every parquet/avro file referenced by the current snapshot.
-------------------------------------------------------------------------------

-- 2.1 Analyze File Sizes (Small File Detection)
-- A high count of small files (<10MB) hurts performance.
SELECT 
    file_path,
    file_format,
    record_count,
    file_size_in_bytes,
    (file_size_in_bytes / 1024.0 / 1024.0) AS Size_MB
FROM TABLE( table_files( 'IcebergMetadataDB.SensorLogs' ) )
ORDER BY file_size_in_bytes ASC;

-- 2.2 Aggregate File Stats
SELECT 
    COUNT(*) AS Total_Files,
    SUM(record_count) AS Total_Records,
    AVG(file_size_in_bytes) / 1024 / 1024 AS Avg_File_Size_MB
FROM TABLE( table_files( 'IcebergMetadataDB.SensorLogs' ) );

-------------------------------------------------------------------------------
-- 3. INSPECTING PARTITIONS
-- table_partitions() shows summary stats per partition.
-------------------------------------------------------------------------------

-- 3.1 Check Partition Skew
-- Look for partitions with significantly more records or files than others.
SELECT 
    "partition" AS Partition_Value, -- Returns a struct of partition keys
    record_count,
    file_count
FROM TABLE( table_partitions( 'IcebergMetadataDB.SensorLogs' ) )
ORDER BY record_count DESC;

-------------------------------------------------------------------------------
-- 4. INSPECTING MANIFESTS
-- table_manifests() shows the manifest files that track the data files.
-------------------------------------------------------------------------------

-- 4.1 Manifest Analysis
-- Too many manifests can slow down planning time.
SELECT 
    path,
    length AS Manifest_Size_Bytes,
    added_data_files_count,
    deleted_data_files_count
FROM TABLE( table_manifests( 'IcebergMetadataDB.SensorLogs' ) );
