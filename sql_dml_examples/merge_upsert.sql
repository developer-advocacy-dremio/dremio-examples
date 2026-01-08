-------------------------------------------------------------------------------
-- Dremio SQL Examples: Advanced DML - MERGE (Upsert)
-- 
-- The MERGE command allows you to perform simultaneous Updates, Inserts, and Deletes
-- based on a join condition. This is essential for:
-- 1. SCD (Slowly Changing Dimensions) Type 1 updates
-- 2. De-duplicating data from a staging table into a final table
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Target (Silver) and Source (Bronze/Staging) Tables
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS DmlDemo;

-- Target: Validated Customer List
CREATE TABLE IF NOT EXISTS DmlDemo.Customers_Silver (
    CustomerID INT,
    Name VARCHAR,
    Email VARCHAR,
    LastUpdated TIMESTAMP
);

-- Source: New Batch of Updates
CREATE TABLE IF NOT EXISTS DmlDemo.Customers_Bronze_Updates (
    CustomerID INT,
    Name VARCHAR,
    Email VARCHAR,
    OpType VARCHAR -- 'I' for Insert, 'U' for Update, 'D' for Delete
);

-- Mock Data
INSERT INTO DmlDemo.Customers_Silver VALUES 
(1, 'Alice', 'alice@old.com', CAST('2025-01-01 10:00:00' AS TIMESTAMP));

INSERT INTO DmlDemo.Customers_Bronze_Updates VALUES 
(1, 'Alice Smith', 'alice@new.com', 'U'), -- Update Alice
(2, 'Bob', 'bob@dremio.com', 'I');        -- Insert Bob

-------------------------------------------------------------------------------
-- 2. MERGE COMMAND (The "Upsert")
-- Update existing records, Insert new ones.
-------------------------------------------------------------------------------

MERGE INTO DmlDemo.Customers_Silver AS Target
USING DmlDemo.Customers_Bronze_Updates AS Source
ON Target.CustomerID = Source.CustomerID

-- Case 1: Record Exists in Target (Update)
WHEN MATCHED THEN
  UPDATE SET 
    Name = Source.Name,
    Email = Source.Email,
    LastUpdated = CURRENT_TIMESTAMP

-- Case 2: Record Does Not Exist in Target (Insert)
WHEN NOT MATCHED THEN
  INSERT (CustomerID, Name, Email, LastUpdated)
  VALUES (Source.CustomerID, Source.Name, Source.Email, CURRENT_TIMESTAMP);

-------------------------------------------------------------------------------
-- 3. MERGE WITH DELETE
-- You can also delete rows based on logic in the source.
-------------------------------------------------------------------------------

/*
MERGE INTO DmlDemo.Customers_Silver AS Target
USING DmlDemo.Customers_Bronze_Updates AS Source
ON Target.CustomerID = Source.CustomerID

WHEN MATCHED AND Source.OpType = 'D' THEN
  DELETE

WHEN MATCHED THEN
  UPDATE SET ...
*/
