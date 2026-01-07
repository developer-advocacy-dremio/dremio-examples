/*
 * Energy Smart Grid Consumption Demo
 * 
 * Scenario:
 * A utility company analyzes smart meter data to manage peak load and pricing.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify high-usage regions and revenue leakage.
 * 
 * Note: Assumes a catalog named 'EnergyDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EnergyDB;
CREATE FOLDER IF NOT EXISTS EnergyDB.Bronze;
CREATE FOLDER IF NOT EXISTS EnergyDB.Silver;
CREATE FOLDER IF NOT EXISTS EnergyDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EnergyDB.Bronze.Meters (
    MeterID INT,
    CustomerID INT,
    Region VARCHAR,
    InstallDate DATE,
    Type VARCHAR -- 'Residential', 'Commercial'
);

CREATE TABLE IF NOT EXISTS EnergyDB.Bronze.UsageReadings (
    ReadingID INT,
    MeterID INT,
    "Timestamp" TIMESTAMP,
    KWhConsumed DOUBLE
);

CREATE TABLE IF NOT EXISTS EnergyDB.Bronze.Tariffs (
    TariffID INT,
    Type VARCHAR,
    RatePerKWh DOUBLE,
    PeakMultiplier DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO EnergyDB.Bronze.Meters (MeterID, CustomerID, Region, InstallDate, Type) VALUES
(101, 1001, 'North', '2022-01-10', 'Residential'),
(102, 1002, 'North', '2021-05-15', 'Commercial'),
(103, 1003, 'South', '2023-02-20', 'Residential'),
(104, 1004, 'East', '2020-11-01', 'Commercial'),
(105, 1005, 'West', '2024-06-12', 'Residential');

INSERT INTO EnergyDB.Bronze.Tariffs (TariffID, Type, RatePerKWh, PeakMultiplier) VALUES
(1, 'Residential', 0.15, 1.5),
(2, 'Commercial', 0.12, 2.0);

INSERT INTO EnergyDB.Bronze.UsageReadings (ReadingID, MeterID, "Timestamp", KWhConsumed) VALUES
(1, 101, '2025-06-01 10:00:00', 1.2),
(2, 101, '2025-06-01 18:00:00', 3.5), -- Peak time
(3, 102, '2025-06-01 10:00:00', 15.0),
(4, 102, '2025-06-01 14:00:00', 25.0),
(5, 103, '2025-06-01 19:00:00', 2.1),
(6, 104, '2025-06-01 08:00:00', 12.0),
(7, 105, '2025-06-01 12:00:00', 0.8),
(8, 101, '2025-06-02 10:00:00', 1.1),
(9, 102, '2025-06-02 10:00:00', 14.5),
(10, 103, '2025-06-02 09:00:00', 0.9);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cost Calculation
-------------------------------------------------------------------------------

-- 2.1 View: Billable_Usage
-- Calculates cost based on time-of-use (Peak hours: 17:00 - 21:00)
CREATE OR REPLACE VIEW EnergyDB.Silver.Billable_Usage AS
SELECT
    u.ReadingID,
    u.MeterID,
    m.Region,
    m.Type,
    u."Timestamp",
    u.KWhConsumed,
    EXTRACT(HOUR FROM u."Timestamp") AS ReadingHour,
    CASE 
        WHEN EXTRACT(HOUR FROM u."Timestamp") BETWEEN 17 AND 21 THEN t.RatePerKWh * t.PeakMultiplier
        ELSE t.RatePerKWh
    END AS AppliedRate,
    u.KWhConsumed * (
        CASE 
            WHEN EXTRACT(HOUR FROM u."Timestamp") BETWEEN 17 AND 21 THEN t.RatePerKWh * t.PeakMultiplier
            ELSE t.RatePerKWh
        END
    ) AS EstimatedCost
FROM EnergyDB.Bronze.UsageReadings u
JOIN EnergyDB.Bronze.Meters m ON u.MeterID = m.MeterID
JOIN EnergyDB.Bronze.Tariffs t ON m.Type = t.Type;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Aggregate Consumption
-------------------------------------------------------------------------------

-- 3.1 View: Consumption_By_Region
-- Aggregates total load and revenue by region.
CREATE OR REPLACE VIEW EnergyDB.Gold.Consumption_By_Region AS
SELECT
    Region,
    SUM(KWhConsumed) AS TotalLoadKWh,
    SUM(EstimatedCost) AS TotalRevenue,
    AVG(CASE WHEN ReadingHour BETWEEN 17 AND 21 THEN KWhConsumed ELSE NULL END) AS AvgPeakLoad
FROM EnergyDB.Silver.Billable_Usage
GROUP BY Region;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Peak Load):
"Using EnergyDB.Gold.Consumption_By_Region, which region has the highest TotalLoadKWh?"

PROMPT 2 (Cost Audit):
"List all readings from EnergyDB.Silver.Billable_Usage where ReadingHour is between 17 and 21 and KWhConsumed > 2.0."
*/
