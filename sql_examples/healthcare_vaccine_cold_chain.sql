/*
    Dremio High-Volume SQL Pattern: Healthcare Vaccine Cold Chain
    
    Business Scenario:
    Vaccines must be stored within strict temperature ranges (2-8 C). "Excursions" must be flagged immediately to prevent spoilage.
    
    Data Story:
    We ingest IoT sensor streams from refrigerators.
    
    Medallion Architecture:
    - Bronze: TempSensors, InventoryLots.
      *Volume*: 50+ records.
    - Silver: ExcursionEvents (Temp > 8 or < 2).
    - Gold: SpoilageRisk (Lots in affected fridges).
    
    Key Dremio Features:
    - Range Checks
    - High-volume sensor ingest pattern
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS HealthcareColdChainDB;
CREATE FOLDER IF NOT EXISTS HealthcareColdChainDB.Bronze;
CREATE FOLDER IF NOT EXISTS HealthcareColdChainDB.Silver;
CREATE FOLDER IF NOT EXISTS HealthcareColdChainDB.Gold;
USE HealthcareColdChainDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE HealthcareColdChainDB.Bronze.TempSensors (
    SensorID STRING,
    ReadingTime TIMESTAMP,
    TempCelsius DOUBLE,
    FridgeID STRING
);

-- Bulk Readings (Simulating intermittent failures)
INSERT INTO HealthcareColdChainDB.Bronze.TempSensors
SELECT 
  'S' || CAST((rn % 5) + 1 AS STRING), -- 5 sensors
  DATE_ADD(TIMESTAMP '2025-01-01 00:00:00', CAST(rn * 30 AS INT) * 1000 * 60), -- Every 30 mins
  CASE 
    WHEN rn % 20 = 0 THEN 9.5 -- Excursion High
    WHEN rn % 25 = 0 THEN 1.5 -- Excursion Low
    ELSE 4.0 + (rn % 3) -- Normal (4-7 C)
  END,
  'F' || CAST((rn % 5) + 1 AS STRING)
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE HealthcareColdChainDB.Bronze.InventoryLots (
    LotID STRING,
    VaccineName STRING,
    FridgeID STRING,
    Quantity INT
);

INSERT INTO HealthcareColdChainDB.Bronze.InventoryLots VALUES
('L001', 'Flu', 'F1', 500),
('L002', 'COVID', 'F1', 200),
('L003', 'Flu', 'F2', 300),
('L004', 'Shingles', 'F3', 100);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Excursion Detection
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareColdChainDB.Silver.Excursions AS
SELECT
    SensorID,
    FridgeID,
    ReadingTime,
    TempCelsius,
    CASE
        WHEN TempCelsius > 8.0 THEN 'HIGH_TEMP'
        WHEN TempCelsius < 2.0 THEN 'LOW_TEMP'
        ELSE 'NORMAL'
    END AS Status
FROM HealthcareColdChainDB.Bronze.TempSensors;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Spoilage Risk
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW HealthcareColdChainDB.Gold.SpoilageRisk AS
SELECT
    e.FridgeID,
    i.VaccineName,
    i.LotID,
    COUNT(e.ReadingTime) AS ExcursionCount,
    MAX(e.TempCelsius) AS MaxTempReached,
    'Review Required' AS Action
FROM HealthcareColdChainDB.Silver.Excursions e
JOIN HealthcareColdChainDB.Bronze.InventoryLots i ON e.FridgeID = i.FridgeID
WHERE e.Status != 'NORMAL'
GROUP BY e.FridgeID, i.VaccineName, i.LotID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List all Lots at risk of spoilage."
    2. "Show the temperature trend for Fridge F1."
    3. "Count excursions by FridgeID."
*/
