-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Conversion & Type Functions
-- 
-- This script demonstrates explicit data type conversions.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.RawInputs (
    ID INT,
    StringNum VARCHAR,
    StringDate VARCHAR,
    BinaryData VARBINARY
);

INSERT INTO FunctionExamplesDB.RawInputs (ID, StringNum, StringDate, BinaryData) VALUES
(1, '12345', '2023-11-01', CAST('Dremio' AS VARBINARY)),
(2, '99.99', '2025-05-20', CAST('Rocks' AS VARBINARY));

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Cast and Convert
-------------------------------------------------------------------------------

-- 2.1 CAST (Standard SQL)
-- Casts a value to a specific type.
SELECT 
    StringNum,
    CAST(StringNum AS INT) AS As_Integer,
    CAST(StringNum AS DOUBLE) AS As_Double,
    CAST(StringNum AS DECIMAL(10,2)) AS As_Decimal
FROM FunctionExamplesDB.RawInputs;

-- 2.2 TYPEOF (If supported for debugging types)
-- Shows the data type of the expression.
SELECT 
    ID,
    TYPEOF(ID) AS ID_Type,
    TYPEOF(StringNum) AS String_Type
FROM FunctionExamplesDB.RawInputs;

-- 2.3 Hex conversions (TO_HEX, FROM_HEX)
SELECT 
    'Dremio' AS Original_String,
    TO_HEX(CAST('Dremio' AS VARBINARY)) AS Hex_Value,
    CAST(FROM_HEX(TO_HEX(CAST('Dremio' AS VARBINARY))) AS VARCHAR) AS Restored_String
FROM (VALUES(1));

-- 2.4 JSON Type Conversion (CONVERT_FROM, CONVERT_TO)
-- Used often for parsing JSON strings into Structs/Maps or serializing them back.
SELECT 
    '{"key": "value"}' AS Json_String,
    CONVERT_FROM('{"key": "value"}', 'json') AS Parsed_Struct,
    CONVERT_TO(CONVERT_FROM('{"key": "value"}', 'json'), 'json') AS Back_To_String
FROM (VALUES(1));
