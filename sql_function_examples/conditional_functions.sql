-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Conditional & Null Handling Functions
-- 
-- This script demonstrates how to handle logic and NULLs in queries.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.Products (
    ProdID INT,
    Name VARCHAR,
    Price DOUBLE,
    Discount DOUBLE, -- Can be NULL
    Category VARCHAR -- Can be NULL
);

INSERT INTO FunctionExamplesDB.Products (ProdID, Name, Price, Discount, Category) VALUES
(1, 'Laptop', 1000.00, 0.10, 'Electronics'),
(2, 'Mouse', 20.00, NULL, 'Electronics'),
(3, 'Old Book', 5.00, NULL, NULL),
(4, 'Gift Card', 50.00, 0.00, 'Misc');

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Conditional Logic
-------------------------------------------------------------------------------

-- 2.1 COALESCE / NVL
-- Returns the first non-null value. 
-- Useful for providing defaults.
SELECT 
    Name,
    Discount AS Raw_Discount,
    COALESCE(Discount, 0.0) AS Discount_Coalesced, -- Standard SQL
    NVL(Discount, 0.0) AS Discount_NVL -- Oracle/Hive style alias
FROM FunctionExamplesDB.Products;

-- 2.2 NULLIF
-- Returns NULL if the two arguments are equal. 
-- Useful for avoiding division by zero (e.g., NULLIF(denominator, 0)).
SELECT 
    Name,
    Discount,
    NULLIF(Discount, 0.0) AS Null_If_Zero_Discount
FROM FunctionExamplesDB.Products;

-- 2.3 CASE WHEN
-- Implements If-Then-Else logic.
SELECT 
    Name,
    Price,
    CASE 
        WHEN Price >= 1000 THEN 'Premium'
        WHEN Price >= 100 THEN 'Standard'
        ELSE 'Budget'
    END AS Price_Tier,
    CASE Category
        WHEN 'Electronics' THEN 'Tech Dept'
        WHEN 'Misc' THEN 'General Dept'
        ELSE 'Unassigned Dept'
    END AS Department_Logic
FROM FunctionExamplesDB.Products;

-- 2.4 Boolean Predicates (IS NULL, IS NOT NULL, IS TRUE)
-- Demonstrating usage in projection (some dialects allow boolean as value).
SELECT 
    Name,
    (Discount IS NULL) AS Is_Discount_Null,
    (Category IS NOT NULL) AS Has_Category
FROM FunctionExamplesDB.Products;
