/*
 * Real Estate Market Trends Demo
 * 
 * Scenario:
 * A real estate agency tracks property values, sales velocity, and regional trends.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify hot markets and optimal pricing strategies.
 * 
 * Note: Assumes a catalog named 'RealEstateDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RealEstateDB;
CREATE FOLDER IF NOT EXISTS RealEstateDB.Bronze;
CREATE FOLDER IF NOT EXISTS RealEstateDB.Silver;
CREATE FOLDER IF NOT EXISTS RealEstateDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Listings Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RealEstateDB.Bronze.Properties (
    PropertyID INT,
    Address VARCHAR,
    City VARCHAR,
    ZipCode VARCHAR,
    SqFt INT,
    Bedrooms INT,
    Bathrooms INT,
    YearBuilt INT
);

CREATE TABLE IF NOT EXISTS RealEstateDB.Bronze.Listings (
    ListingID INT,
    PropertyID INT,
    ListDate DATE,
    ListPrice DOUBLE,
    Status VARCHAR -- 'Active', 'Sold', 'Pending'
);

CREATE TABLE IF NOT EXISTS RealEstateDB.Bronze.SalesTransactions (
    SaleID INT,
    ListingID INT,
    SaleDate DATE,
    SalePrice DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO RealEstateDB.Bronze.Properties (PropertyID, Address, City, ZipCode, SqFt, Bedrooms, Bathrooms, YearBuilt) VALUES
(1, '123 Maple Dr', 'Springfield', '62704', 2000, 3, 2, 1990),
(2, '456 Oak Ave', 'Springfield', '62704', 1500, 2, 1, 1955),
(3, '789 Pine Ln', 'Shelbyville', '62565', 3000, 4, 3, 2010),
(4, '321 Elm St', 'Springfield', '62702', 1800, 3, 2, 2005),
(5, '654 Cedar Rd', 'Shelbyville', '62565', 2400, 4, 2, 1998);

INSERT INTO RealEstateDB.Bronze.Listings (ListingID, PropertyID, ListDate, ListPrice, Status) VALUES
(101, 1, '2024-01-01', 350000.00, 'Sold'),
(102, 2, '2024-02-15', 200000.00, 'Active'),
(103, 3, '2024-01-10', 500000.00, 'Sold'),
(104, 4, '2024-03-01', 320000.00, 'Pending'),
(105, 5, '2024-03-15', 420000.00, 'Active');

INSERT INTO RealEstateDB.Bronze.SalesTransactions (SaleID, ListingID, SaleDate, SalePrice) VALUES
(1, 101, '2024-02-01', 345000.00), -- Sold slightly under ask
(2, 103, '2024-02-10', 510000.00); -- Sold over ask

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Market Activity
-------------------------------------------------------------------------------

-- 2.1 View: Property_Valuation
-- Calculates Price per SqFt and Days on Market (DOM).
CREATE OR REPLACE VIEW RealEstateDB.Silver.Property_Valuation AS
SELECT
    p.PropertyID,
    p.City,
    p.ZipCode,
    p.SqFt,
    l.ListPrice,
    s.SalePrice,
    l.ListDate,
    s.SaleDate,
    DATEDIFF(DAY, l.ListDate, COALESCE(s.SaleDate, CAST('2025-01-01' AS DATE))) AS DaysOnMarket,
    COALESCE(s.SalePrice, l.ListPrice) / p.SqFt AS PricePerSqFt
FROM RealEstateDB.Bronze.Properties p
JOIN RealEstateDB.Bronze.Listings l ON p.PropertyID = l.PropertyID
LEFT JOIN RealEstateDB.Bronze.SalesTransactions s ON l.ListingID = s.ListingID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Regional Trends
-------------------------------------------------------------------------------

-- 3.1 View: ZipCode_Performance
-- Aggregates metrics by ZipCode.
CREATE OR REPLACE VIEW RealEstateDB.Gold.ZipCode_Performance AS
SELECT
    ZipCode,
    City,
    COUNT(PropertyID) AS TotalListings,
    AVG(PricePerSqFt) AS AvgPPSqFt,
    AVG(CASE WHEN SalePrice IS NOT NULL THEN DaysOnMarket ELSE NULL END) AS AvgDOM_Sold,
    AVG(CASE WHEN SalePrice IS NOT NULL THEN (SalePrice / ListPrice) ELSE NULL END) * 100 AS SaleToListRatio
FROM RealEstateDB.Silver.Property_Valuation
GROUP BY ZipCode, City;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Market Hotspots):
"Which ZipCode has the highest AvgPPSqFt in RealEstateDB.Gold.ZipCode_Performance?"

PROMPT 2 (Pricing Strategy):
"List properties from RealEstateDB.Silver.Property_Valuation where SalePrice > ListPrice."
*/
