-------------------------------------------------------------------------------
-- Dremio SQL Admin Examples: Reflection Management
-- 
-- This script demonstrates creation and management of Data Reflections to 
-- accelerate queries.
-- Scenarios include:
-- 1. Raw Reflections (Row-level acceleration)
-- 2. Aggregate Reflections (BI dashboard acceleration)
-- 3. Partitioning and Sorting strategies
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 0. SETUP: Create Mock Data
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure 'SupplyChainDB' and 'RetailDB' spaces exist.
CREATE FOLDER IF NOT EXISTS "SupplyChainDB"."Bronze";
CREATE FOLDER IF NOT EXISTS "SupplyChainDB"."Silver";
CREATE FOLDER IF NOT EXISTS "RetailDB"."Gold";

-- Create Mock Tables
CREATE TABLE IF NOT EXISTS "SupplyChainDB"."Bronze"."Shipments" (
    ShipmentID INT, ProductID INT, ShipmentDate DATE, Status VARCHAR, Quantity INT
);

CREATE TABLE IF NOT EXISTS "SupplyChainDB"."Silver"."Supplier_Performance" (
    SupplierName VARCHAR, Region VARCHAR, TotalReliability DOUBLE, ShipmentCount INT
);

CREATE TABLE IF NOT EXISTS "RetailDB"."Gold"."Sales_Summary" (
    SaleRegion VARCHAR, SaleQuarter VARCHAR, Revenue DOUBLE, Profit DOUBLE, UnitsSold INT
);

-------------------------------------------------------------------------------
-- 1. RAW REFLECTIONS
-- Use Case: Accelerating expensive row-level scans or filtering on large datasets.
-------------------------------------------------------------------------------

-- 1.1 Basic Raw Reflection
-- Accelerates 'SELECT *' or subset of columns.
ALTER TABLE "SupplyChainDB"."Bronze"."Shipments"
CREATE RAW REFLECTION "Raw_Shipments_Basic"
USING DISPLAY (ShipmentID, ProductID, ShipmentDate, Status);

-- 1.2 Advanced Raw Reflection (Partition & Sort)
-- PARTITION BY: Optimize for specific high-cardinality filters (e.g., Date).
-- LOCALSORT BY: Optimize for range queries or sorts.
ALTER TABLE "SupplyChainDB"."Bronze"."Shipments"
CREATE RAW REFLECTION "Raw_Shipments_Optimized"
USING DISPLAY (ShipmentID, ProductID, ShipmentDate, Status, Quantity)
PARTITION BY (ShipmentDate)
LOCALSORT BY (ProductID);

-------------------------------------------------------------------------------
-- 2. AGGREGATE REFLECTIONS
-- Use Case: Accelerating GROUP BY queries (Dashboards/BI).
-------------------------------------------------------------------------------

-- 2.1 Basic Aggregation
-- Pre-computes SUM/COUNT by Supplier.
ALTER TABLE "SupplyChainDB"."Silver"."Supplier_Performance"
CREATE AGGREGATE REFLECTION "Agg_By_Supplier"
USING 
    DIMENSIONS (SupplierName, Region)
    MEASURES (TotalReliability (SUM), ShipmentCount (COUNT));

-- 2.2 Aggregation with Time Dimensions
-- Common pattern for time-series charts.
ALTER TABLE "RetailDB"."Gold"."Sales_Summary"
CREATE AGGREGATE REFLECTION "Agg_Sales_Quarterly"
USING
    DIMENSIONS (SaleRegion, SaleQuarter)
    MEASURES (Revenue (SUM), Profit (MAX), UnitsSold (SUM))
PARTITION BY (SaleRegion)
LOCALSORT BY (SaleQuarter);

-------------------------------------------------------------------------------
-- 3. MAINTENANCE & TUNING
-------------------------------------------------------------------------------

-- 3.1 Dropping a Reflection
-- Free up resources if a reflection is unused (check sys.reflection_usage).
ALTER TABLE "SupplyChainDB"."Bronze"."Shipments"
DROP REFLECTION "Raw_Shipments_Basic";

-- 3.2 Triggering a Refresh (Manual)
-- Usually automatic, but useful for forcing updates after ETL loads.
ALTER TABLE "SupplyChainDB"."Bronze"."Shipments" REFRESH REFLECTION "Raw_Shipments_Optimized";

-- 3.3 External Reflections (Advanced)
-- Map an existing derived table/view to serve as a reflection for another table.
-- CREATE EXTERNAL REFLECTION "Ext_Ref_Marketing" USING "MarketingDB"."Derived"."Campaigns_Summary";

-------------------------------------------------------------------------------
-- 4. MONITORING (System Tables)
-- Query to find large reflections or those failing to refresh.
-------------------------------------------------------------------------------
/*
SELECT 
    reflection_name,
    dataset_name,
    status,
    num_failures,
    total_size_bytes
FROM sys.reflections
WHERE status != 'OK' OR num_failures > 0;
*/
