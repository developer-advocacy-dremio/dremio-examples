-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Aggregate Functions
-- 
-- This script demonstrates the usage of aggregate functions for summarizing data.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.Sales (
    Region VARCHAR,
    Product VARCHAR,
    Quantity INT,
    Amount DOUBLE
);

INSERT INTO FunctionExamplesDB.Sales (Region, Product, Quantity, Amount) VALUES
('North', 'Widget A', 10, 100.00),
('North', 'Widget B', 5, 50.00),
('North', 'Widget A', 2, 20.00),
('South', 'Widget A', 20, 200.00),
('South', 'Widget C', 8, 80.00),
('East', 'Widget B', 15, 150.00),
('East', 'Widget B', 5, 50.00);

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Aggregate Functions
-------------------------------------------------------------------------------

-- 2.1 Basic Aggregates (COUNT, SUM, AVG, MIN, MAX)
SELECT 
    Region,
    COUNT(*) AS Total_Transactions,
    SUM(Quantity) AS Total_Items_Sold,
    SUM(Amount) AS Total_Revenue,
    AVG(Amount) AS Avg_Transaction_Value,
    MIN(Amount) AS Min_Sale,
    MAX(Amount) AS Max_Sale
FROM FunctionExamplesDB.Sales
GROUP BY Region;

-- 2.2 Statistical Aggregates (STDDEV, VARIANCE)
-- STDDEV / STDDEV_SAMP: Sample standard deviation.
-- STDDEV_POP: Population standard deviation.
-- VARIANCE / VAR_SAMP: Sample variance.
SELECT 
    Product,
    AVG(Amount) AS Avg_Amount,
    STDDEV(Amount) AS StdDev_Amount,
    VARIANCE(Amount) AS Variance_Amount
FROM FunctionExamplesDB.Sales
GROUP BY Product
HAVING COUNT(*) > 1;

-- 2.3 String Aggregation (LISTAGG / ARRAY_AGG)
-- LISTAGG: Concatenates values into a single string with a delimiter.
-- ARRAY_AGG: Aggregates values into an array/list.
SELECT 
    Region,
    LISTAGG(Product, ', ') WITHIN GROUP (ORDER BY Product) AS Product_List_String,
    ARRAY_AGG(Product) AS Product_List_Array
FROM FunctionExamplesDB.Sales
GROUP BY Region;

-- 2.4 Approximate Aggregates (APPROX_COUNT_DISTINCT)
-- faster than COUNT(DISTINCT column) for large datasets.
SELECT 
    APPROX_COUNT_DISTINCT(Product) AS Approx_Unique_Products,
    COUNT(DISTINCT Product) AS Exact_Unique_Products
FROM FunctionExamplesDB.Sales;
