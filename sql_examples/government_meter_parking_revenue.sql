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

INSERT INTO GovernmentParkingDB.Bronze.MeterTxns VALUES
('TXN001', 'M101', 'Downtown', 5.0, DATE '2025-01-01'),
('TXN002', 'M102', 'Downtown', 2.5, DATE '2025-01-01'),
('TXN003', 'M103', 'Uptown', 1.0, DATE '2025-01-01'),
('TXN004', 'M101', 'Downtown', 5.0, DATE '2025-01-01'),
('TXN005', 'M104', 'Uptown', 2.0, DATE '2025-01-02'),
('TXN006', 'M105', 'Downtown', 7.5, DATE '2025-01-02'),
('TXN007', 'M102', 'Downtown', 2.5, DATE '2025-01-02'),
('TXN008', 'M106', 'Uptown', 1.5, DATE '2025-01-02'),
('TXN009', 'M101', 'Downtown', 5.0, DATE '2025-01-03'),
('TXN010', 'M103', 'Uptown', 3.0, DATE '2025-01-03'),
('TXN011', 'M107', 'Downtown', 4.0, DATE '2025-01-03'),
('TXN012', 'M108', 'Uptown', 2.0, DATE '2025-01-04'),
('TXN013', 'M101', 'Downtown', 5.0, DATE '2025-01-04'),
('TXN014', 'M102', 'Downtown', 2.5, DATE '2025-01-04'),
('TXN015', 'M104', 'Uptown', 1.0, DATE '2025-01-05'),
('TXN016', 'M105', 'Downtown', 6.0, DATE '2025-01-05'),
('TXN017', 'M109', 'Downtown', 3.0, DATE '2025-01-05'),
('TXN018', 'M103', 'Uptown', 2.0, DATE '2025-01-06'),
('TXN019', 'M101', 'Downtown', 5.0, DATE '2025-01-06'),
('TXN020', 'M106', 'Uptown', 1.5, DATE '2025-01-06'),
('TXN021', 'M102', 'Downtown', 2.5, DATE '2025-01-07'),
('TXN022', 'M104', 'Uptown', 4.0, DATE '2025-01-07'),
('TXN023', 'M107', 'Downtown', 5.0, DATE '2025-01-07'),
('TXN024', 'M108', 'Uptown', 2.0, DATE '2025-01-08'),
('TXN025', 'M101', 'Downtown', 5.0, DATE '2025-01-08'),
('TXN026', 'M105', 'Downtown', 3.0, DATE '2025-01-09'),
('TXN027', 'M103', 'Uptown', 1.0, DATE '2025-01-09'),
('TXN028', 'M102', 'Downtown', 2.5, DATE '2025-01-10'),
('TXN029', 'M109', 'Downtown', 4.0, DATE '2025-01-10'),
('TXN030', 'M106', 'Uptown', 2.0, DATE '2025-01-10'),
('TXN031', 'M101', 'Downtown', 5.0, DATE '2025-01-11'),
('TXN032', 'M104', 'Uptown', 3.0, DATE '2025-01-11'),
('TXN033', 'M107', 'Downtown', 6.0, DATE '2025-01-12'),
('TXN034', 'M102', 'Downtown', 2.5, DATE '2025-01-12'),
('TXN035', 'M108', 'Uptown', 1.5, DATE '2025-01-13'),
('TXN036', 'M103', 'Uptown', 2.0, DATE '2025-01-13'),
('TXN037', 'M101', 'Downtown', 5.0, DATE '2025-01-14'),
('TXN038', 'M105', 'Downtown', 4.0, DATE '2025-01-14'),
('TXN039', 'M106', 'Uptown', 3.0, DATE '2025-01-15'),
('TXN040', 'M102', 'Downtown', 2.5, DATE '2025-01-15'),
('TXN041', 'M109', 'Downtown', 5.0, DATE '2025-01-15'),
('TXN042', 'M104', 'Uptown', 2.0, DATE '2025-01-16'),
('TXN043', 'M107', 'Downtown', 3.0, DATE '2025-01-16'),
('TXN044', 'M101', 'Downtown', 5.0, DATE '2025-01-17'),
('TXN045', 'M103', 'Uptown', 1.0, DATE '2025-01-17'),
('TXN046', 'M108', 'Uptown', 2.5, DATE '2025-01-18'),
('TXN047', 'M102', 'Downtown', 2.5, DATE '2025-01-18'),
('TXN048', 'M105', 'Downtown', 6.0, DATE '2025-01-19'),
('TXN049', 'M106', 'Uptown', 3.5, DATE '2025-01-19'),
('TXN050', 'M101', 'Downtown', 5.0, DATE '2025-01-20');

CREATE OR REPLACE TABLE GovernmentParkingDB.Bronze.Tickets (
    TicketID STRING,
    ZoneID STRING,
    ViolationType STRING, -- Expired Meter, No Parking
    FineAmount DOUBLE,
    Date DATE
);

INSERT INTO GovernmentParkingDB.Bronze.Tickets VALUES
('T1', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-01'),
('T2', 'Uptown', 'No Parking', 75.0, DATE '2025-01-01'),
('T3', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-02'),
('T4', 'Downtown', 'No Parking', 75.0, DATE '2025-01-03'),
('T5', 'Uptown', 'Expired Meter', 50.0, DATE '2025-01-04'),
('T6', 'Downtown', 'No Parking', 75.0, DATE '2025-01-05'),
('T7', 'Uptown', 'No Parking', 75.0, DATE '2025-01-06'),
('T8', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-07'),
('T9', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-08'),
('T10', 'Uptown', 'Expired Meter', 50.0, DATE '2025-01-09'),
('T11', 'Downtown', 'No Parking', 75.0, DATE '2025-01-10'),
('T12', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-11'),
('T13', 'Uptown', 'No Parking', 75.0, DATE '2025-01-12'),
('T14', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-13'),
('T15', 'Uptown', 'Expired Meter', 50.0, DATE '2025-01-14'),
('T16', 'Downtown', 'No Parking', 75.0, DATE '2025-01-15'),
('T17', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-16'),
('T18', 'Uptown', 'No Parking', 75.0, DATE '2025-01-17'),
('T19', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-18'),
('T20', 'Uptown', 'Expired Meter', 50.0, DATE '2025-01-19'),
('T21', 'Downtown', 'No Parking', 75.0, DATE '2025-01-20'),
('T22', 'Downtown', 'Expired Meter', 50.0, DATE '2025-01-01'), -- Double ticket
('T23', 'Uptown', 'Expired Meter', 50.0, DATE '2025-01-02'),
('T24', 'Downtown', 'No Parking', 75.0, DATE '2025-01-04'),
('T25', 'Uptown', 'No Parking', 75.0, DATE '2025-01-05');

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
