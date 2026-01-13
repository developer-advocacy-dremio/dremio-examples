/*
 * Weather Impact on Retail Demo
 * 
 * Scenario:
 * Correlating daily sales with local weather conditions.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize inventory for weather-driven demand (e.g., umbrellas, sunscreen).
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS WeatherRetailDB;
CREATE FOLDER IF NOT EXISTS WeatherRetailDB.Bronze;
CREATE FOLDER IF NOT EXISTS WeatherRetailDB.Silver;
CREATE FOLDER IF NOT EXISTS WeatherRetailDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS WeatherRetailDB.Bronze.WeatherLog (
    City VARCHAR,
    "Date" DATE,
    Condition VARCHAR, -- 'Sunny', 'Rain', 'Snow'
    AvgTempC DOUBLE
);

CREATE TABLE IF NOT EXISTS WeatherRetailDB.Bronze.StoreSales (
    StoreID INT,
    City VARCHAR,
    "Date" DATE,
    Category VARCHAR,
    Revenue DOUBLE
);

INSERT INTO WeatherRetailDB.Bronze.WeatherLog VALUES
('New York', '2025-04-01', 'Rain', 10.0),
('Los Angeles', '2025-04-01', 'Sunny', 25.0);

INSERT INTO WeatherRetailDB.Bronze.StoreSales VALUES
(1, 'New York', '2025-04-01', 'Umbrellas', 5000.0),
(1, 'New York', '2025-04-01', 'Ice Cream', 200.0),
(2, 'Los Angeles', '2025-04-01', 'Umbrellas', 0.0),
(2, 'Los Angeles', '2025-04-01', 'Ice Cream', 4000.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WeatherRetailDB.Silver.SalesVsWeather AS
SELECT 
    s."Date",
    s.City,
    w.Condition,
    w.AvgTempC,
    s.Category,
    s.Revenue
FROM WeatherRetailDB.Bronze.StoreSales s
JOIN WeatherRetailDB.Bronze.WeatherLog w 
  ON s.City = w.City AND s."Date" = w."Date";

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW WeatherRetailDB.Gold.CategorySensitivity AS
SELECT 
    Category,
    Condition,
    AVG(Revenue) AS AvgRevenue
FROM WeatherRetailDB.Silver.SalesVsWeather
GROUP BY Category, Condition;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which weather Condition drives the highest AvgRevenue for 'Umbrellas' in WeatherRetailDB.Gold.CategorySensitivity?"
*/
