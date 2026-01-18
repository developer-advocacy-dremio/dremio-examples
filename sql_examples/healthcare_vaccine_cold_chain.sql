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
INSERT INTO HealthcareColdChainDB.Bronze.TempSensors VALUES
('S1', TIMESTAMP '2025-01-01 00:30:00', 9.5, 'F1'), -- Excursion High
('S2', TIMESTAMP '2025-01-01 01:00:00', 4.5, 'F2'),
('S3', TIMESTAMP '2025-01-01 01:30:00', 5.0, 'F3'),
('S4', TIMESTAMP '2025-01-01 02:00:00', 4.8, 'F4'),
('S5', TIMESTAMP '2025-01-01 02:30:00', 1.5, 'F5'), -- Excursion Low
('S1', TIMESTAMP '2025-01-01 03:00:00', 5.2, 'F1'),
('S2', TIMESTAMP '2025-01-01 03:30:00', 5.1, 'F2'),
('S3', TIMESTAMP '2025-01-01 04:00:00', 4.9, 'F3'),
('S4', TIMESTAMP '2025-01-01 04:30:00', 5.0, 'F4'),
('S5', TIMESTAMP '2025-01-01 05:00:00', 5.3, 'F5'),
('S1', TIMESTAMP '2025-01-01 05:30:00', 5.5, 'F1'),
('S2', TIMESTAMP '2025-01-01 06:00:00', 5.4, 'F2'),
('S3', TIMESTAMP '2025-01-01 06:30:00', 5.6, 'F3'),
('S4', TIMESTAMP '2025-01-01 07:00:00', 5.2, 'F4'),
('S5', TIMESTAMP '2025-01-01 07:30:00', 5.1, 'F5'),
('S1', TIMESTAMP '2025-01-01 08:00:00', 5.0, 'F1'),
('S2', TIMESTAMP '2025-01-01 08:30:00', 4.8, 'F2'),
('S3', TIMESTAMP '2025-01-01 09:00:00', 4.9, 'F3'),
('S4', TIMESTAMP '2025-01-01 09:30:00', 5.1, 'F4'),
('S5', TIMESTAMP '2025-01-01 10:00:00', 9.5, 'F5'), -- High
('S1', TIMESTAMP '2025-01-01 10:30:00', 5.2, 'F1'),
('S2', TIMESTAMP '2025-01-01 11:00:00', 5.3, 'F2'),
('S3', TIMESTAMP '2025-01-01 11:30:00', 5.4, 'F3'),
('S4', TIMESTAMP '2025-01-01 12:00:00', 5.0, 'F4'),
('S5', TIMESTAMP '2025-01-01 12:30:00', 1.5, 'F5'), -- Low
('S1', TIMESTAMP '2025-01-01 13:00:00', 5.1, 'F1'),
('S2', TIMESTAMP '2025-01-01 13:30:00', 5.2, 'F2'),
('S3', TIMESTAMP '2025-01-01 14:00:00', 5.3, 'F3'),
('S4', TIMESTAMP '2025-01-01 14:30:00', 5.4, 'F4'),
('S5', TIMESTAMP '2025-01-01 15:00:00', 5.5, 'F5'),
('S1', TIMESTAMP '2025-01-01 15:30:00', 5.0, 'F1'),
('S2', TIMESTAMP '2025-01-01 16:00:00', 4.9, 'F2'),
('S3', TIMESTAMP '2025-01-01 16:30:00', 4.8, 'F3'),
('S4', TIMESTAMP '2025-01-01 17:00:00', 5.0, 'F4'),
('S5', TIMESTAMP '2025-01-01 17:30:00', 5.1, 'F5'),
('S1', TIMESTAMP '2025-01-01 18:00:00', 5.2, 'F1'),
('S2', TIMESTAMP '2025-01-01 18:30:00', 5.3, 'F2'),
('S3', TIMESTAMP '2025-01-01 19:00:00', 5.4, 'F3'),
('S4', TIMESTAMP '2025-01-01 19:30:00', 5.5, 'F4'),
('S5', TIMESTAMP '2025-01-01 20:00:00', 9.5, 'F5'), -- High
('S1', TIMESTAMP '2025-01-01 20:30:00', 5.1, 'F1'),
('S2', TIMESTAMP '2025-01-01 21:00:00', 5.0, 'F2'),
('S3', TIMESTAMP '2025-01-01 21:30:00', 4.9, 'F3'),
('S4', TIMESTAMP '2025-01-01 22:00:00', 4.8, 'F4'),
('S5', TIMESTAMP '2025-01-01 22:30:00', 4.7, 'F5'),
('S1', TIMESTAMP '2025-01-01 23:00:00', 5.0, 'F1'),
('S2', TIMESTAMP '2025-01-01 23:30:00', 5.1, 'F2'),
('S3', TIMESTAMP '2025-01-02 00:00:00', 5.2, 'F3'),
('S4', TIMESTAMP '2025-01-02 00:30:00', 5.3, 'F4'),
('S5', TIMESTAMP '2025-01-02 01:00:00', 1.5, 'F5'); -- Low

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
('L004', 'Shingles', 'F3', 100),
('L005', 'COVID', 'F4', 250),
('L006', 'Flu', 'F5', 150),
('L007', 'Shingles', 'F1', 100),
('L008', 'COVID', 'F2', 200),
('L009', 'Flu', 'F3', 300),
('L010', 'Shingles', 'F4', 400);

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
