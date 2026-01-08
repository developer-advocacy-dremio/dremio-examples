-------------------------------------------------------------------------------
-- Dremio SQL Examples: Data Ingestion - COPY INTO
-- 
-- The COPY INTO command is the primary method for high-performance batch loading
-- of data from object storage (S3, ADLS, GCS) into Iceberg tables.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Target Table
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS IngestionDB;

CREATE TABLE IF NOT EXISTS IngestionDB.IncomingOrders (
    OrderID INT,
    CustomerName VARCHAR,
    OrderDate DATE,
    Amount DOUBLE
);

-------------------------------------------------------------------------------
-- 2. LOADING FROM CSV
-- Loading standard CSV files with headers
-------------------------------------------------------------------------------

-- Syntax: COPY INTO <target> FROM <source_path> FILE_FORMAT 'csv' ...
COPY INTO IngestionDB.IncomingOrders
FROM '@S3_Source/raw/orders/2025/01/' -- Path to folder or file(s)
FILE_FORMAT 'csv'
(
    extract_header 'true',
    field_delimiter ',',
    record_delimiter '\n'
)
ON_ERROR 'continue'; -- Skip bad records instead of failing the job

-------------------------------------------------------------------------------
-- 3. LOADING FROM JSON
-- Mapping JSON fields to table columns automatically
-------------------------------------------------------------------------------

COPY INTO IngestionDB.IncomingOrders
FROM '@S3_Source/raw/orders_json/'
FILE_FORMAT 'json'
REGEX '^order.*\.json'; -- Only load files matching this pattern

-------------------------------------------------------------------------------
-- 4. LOADING FROM PARQUET
-- High-speed loading from existing Parquet files (Schema Evolution handled by Iceberg)
-------------------------------------------------------------------------------

COPY INTO IngestionDB.IncomingOrders
FROM '@S3_Source/raw/orders_parquet/'
FILE_FORMAT 'parquet';

-------------------------------------------------------------------------------
-- 5. INSPECTING LOAD ERRORS
-- If ON_ERROR 'continue' was used, check the errors using system function.
-------------------------------------------------------------------------------

SELECT * 
FROM TABLE(
    copy_errors(
        'IngestionDB.IncomingOrders', 
        '123456-job-id-from-history'
    )
);
