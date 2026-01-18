/*
    Dremio High-Volume SQL Pattern: Government Meter Parking Revenue
    
    Business Scenario:
    Cities manage "Parking Zones" to maximize turnover and revenue.
    Tracking "Violations" vs "Meter Revenue" helps determine enforcement efficiency.
    
    Data Story:
    We track Meter Transactions and Ticket Issuance.
    
    Medallion Architecture:
    - Bronze: MeterTxns, Tickets.
      *Volume*: 50+ records.
    - Silver: ZoneRevenue (Meter + Ticket $$).
    - Gold: EnforcementEfficiency (Revenue per Space).
    
    Key Dremio Features:
    - Group By
    - Combined Revenue
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS GovernmentParkingDB;
CREATE FOLDER IF NOT EXISTS GovernmentParkingDB.Bronze;
CREATE FOLDER IF NOT EXISTS GovernmentParkingDB.Silver;
CREATE FOLDER IF NOT EXISTS GovernmentParkingDB.Gold;
USE GovernmentParkingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GovernmentParkingDB.Bronze.MeterTxns (
    TxnID STRING,
    MeterID STRING,
    ZoneID STRING,
    Amount DOUBLE,
    Date DATE
);

-- Bulk Meter Revenue
INSERT INTO GovernmentParkingDB.Bronze.MeterTxns
SELECT 
  'TXN' || CAST(rn + 1000 AS STRING),
  'M' || CAST((rn % 20) AS STRING),
  CASE WHEN rn % 2 = 0 THEN 'Downtown' ELSE 'Uptown' END,
  CASE WHEN rn % 5 = 0 THEN 5.0 ELSE 2.5 END, -- $2.50 or $5.00
  DATE_SUB(DATE '2025-01-20', CAST((rn % 10) AS INT))
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

CREATE OR REPLACE TABLE GovernmentParkingDB.Bronze.Tickets (
    TicketID STRING,
    ZoneID STRING,
    ViolationType STRING, -- Expired Meter, No Parking
    FineAmount DOUBLE,
    Date DATE
);

-- Bulk Tickets
INSERT INTO GovernmentParkingDB.Bronze.Tickets
SELECT 
  'T' || CAST(rn + 500 AS STRING),
  CASE WHEN rn % 2 = 0 THEN 'Downtown' ELSE 'Uptown' END,
  CASE WHEN rn % 2 = 0 THEN 'Expired Meter' ELSE 'No Parking' END,
  50.0,
  DATE_SUB(DATE '2025-01-20', CAST((rn % 10) AS INT))
FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
            (21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
            (41),(42),(43),(44),(45),(46),(47),(48),(49),(50)) AS t(rn);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Daily Totals
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentParkingDB.Silver.DailyZoneRevenue AS
SELECT
    ZoneID,
    Date,
    SUM(Amount) AS MeterRevenue
FROM GovernmentParkingDB.Bronze.MeterTxns
GROUP BY ZoneID, Date;

CREATE OR REPLACE VIEW GovernmentParkingDB.Silver.DailyZoneFines AS
SELECT
    ZoneID,
    Date,
    SUM(FineAmount) AS TicketRevenue
FROM GovernmentParkingDB.Bronze.Tickets
GROUP BY ZoneID, Date;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Revenue Mix
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW GovernmentParkingDB.Gold.TotalRevenueAnalysis AS
SELECT
    m.ZoneID,
    SUM(m.MeterRevenue) AS TotalMeterRev,
    SUM(f.TicketRevenue) AS TotalTicketRev,
    (SUM(m.MeterRevenue) + SUM(f.TicketRevenue)) AS GrandTotal
FROM GovernmentParkingDB.Silver.DailyZoneRevenue m
JOIN GovernmentParkingDB.Silver.DailyZoneFines f ON m.ZoneID = f.ZoneID AND m.Date = f.Date
GROUP BY m.ZoneID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Which Zone generated the most Ticket Revenue?"
    2. "Compare Meter Revenue vs Ticket Revenue for Downtown."
    3. "Show Grand Total revenue by Zone."
*/
