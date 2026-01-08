-------------------------------------------------------------------------------
-- Dremio SQL Examples: Functions - Semi-Structured Data
-- 
-- Dremio excels at handling complex data types like JSON Arrays and Maps natively.
-- This script demonstrates flattening arrays and extracting nested fields.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Table with Complex Types
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.JsonOrders (
    OrderID INT,
    Customer STRUCT<Name: VARCHAR, Address: VARCHAR>,
    Items LIST<STRUCT<ItemName: VARCHAR, Quantity: INT, Price: DOUBLE>>, 
    Tags LIST<VARCHAR>
);

-- Note: Inserting complex types often uses string conversion or specific drivers.
-- For demo purposes, we usually query existing JSON or Parquet.
-- Conceptual Insert:
-- INSERT INTO FunctionExamplesDB.JsonOrders VALUES 
-- (1, {'Name': 'Alice', 'Address': '123 Main'}, [{'ItemName': 'Laptop', 'Quantity': 1}, {'ItemName': 'Mouse', 'Quantity': 2}], ['Tech', 'Urgent']);

-------------------------------------------------------------------------------
-- 2. FLATTEN: Exploding Arrays
-- FLATTEN takes a LIST and creates a new row for each element.
-------------------------------------------------------------------------------

-- 2.1 Basic Flatten
-- If Order 1 has 2 items, this returns 2 rows.
SELECT 
    OrderID,
    FLATTEN(Items) AS Item
FROM FunctionExamplesDB.JsonOrders;

-- 2.2 Flatten with Alias for Field Extraction
-- Extract fields from the exploded struct.
SELECT 
    OrderID,
    flat_items.ItemName,
    flat_items.Quantity,
    flat_items.Price
FROM FunctionExamplesDB.JsonOrders,
     LATERAL FLATTEN(Items) AS flat_items;

-------------------------------------------------------------------------------
-- 3. ARRAY FUNCTIONS
-- Working with lists without flattening.
-------------------------------------------------------------------------------

-- 3.1 Check if array contains value
-- Returns TRUE if 'Tech' is in the Tags list
-- SELECT OrderID, ARRAY_CONTAINS(Tags, 'Tech') FROM FunctionExamplesDB.JsonOrders;

-- 3.2 Get Array Length
-- SELECT OrderID, ARRAY_LENGTH(Items) AS ItemCount FROM FunctionExamplesDB.JsonOrders;

-- 3.3 Access by Index (0-based)
-- SELECT OrderID, Tags[0] AS FirstTag FROM FunctionExamplesDB.JsonOrders;

-------------------------------------------------------------------------------
-- 4. CONVERT_FROM: String to JSON
-- Useful when importing raw text/json files.
-------------------------------------------------------------------------------

SELECT 
    CONVERT_FROM('{"name": "Alice", "age": 30}', 'JSON') AS Parsed_Json
FROM (VALUES(1));
