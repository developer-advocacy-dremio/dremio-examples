/*
    Dremio High-Volume SQL Pattern: Services Cruise Spend
    
    Business Scenario:
    Cruise lines maximize "Onboard Revenue" per passenger day.
    We track spend across Dining, Casino, Spa, and Shore Excursions.
    
    Data Story:
    - Bronze: GuestManifest, TransactionLog.
    - Silver: DailySpend (Grouped by Category).
    - Gold: HighValueGuestAnalysis (Top spenders).
    
    Medallion Architecture:
    - Bronze: GuestManifest, TransactionLog.
      *Volume*: 50+ records.
    - Silver: DailySpend.
    - Gold: HighValueGuestAnalysis.
    
    Key Dremio Features:
    - SUM(Amount)
    - Ranking (ORDER BY DESC)
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ServicesDB;
CREATE FOLDER IF NOT EXISTS ServicesDB.Bronze;
CREATE FOLDER IF NOT EXISTS ServicesDB.Silver;
CREATE FOLDER IF NOT EXISTS ServicesDB.Gold;
USE ServicesDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ServicesDB.Bronze.GuestManifest (
    GuestID STRING,
    RoomType STRING, -- Suite, Balcony, Interior
    Nationality STRING
);

INSERT INTO ServicesDB.Bronze.GuestManifest VALUES
('G101', 'Suite', 'USA'), ('G102', 'Balcony', 'UK'),
('G103', 'Interior', 'USA'), ('G104', 'Suite', 'Canada'),
('G105', 'Balcony', 'Germany'), ('G106', 'Interior', 'France'),
('G107', 'Suite', 'USA'), ('G108', 'Balcony', 'UK'),
('G109', 'Interior', 'Spain'), ('G110', 'Suite', 'Japan');

CREATE OR REPLACE TABLE ServicesDB.Bronze.TransactionLog (
    TxnID INT,
    GuestID STRING,
    Date DATE,
    Category STRING, -- Dining, Casino, Spa, ShoreEx, Retail
    Amount DOUBLE
);

INSERT INTO ServicesDB.Bronze.TransactionLog VALUES
-- Guest 101 (Whale)
(1, 'G101', DATE '2025-01-20', 'Casino', 500.00),
(2, 'G101', DATE '2025-01-20', 'Dining', 150.00),
(3, 'G101', DATE '2025-01-21', 'Spa', 200.00),
(4, 'G101', DATE '2025-01-21', 'Casino', 1000.00),
(5, 'G101', DATE '2025-01-22', 'ShoreEx', 300.00),

-- Guest 102 (Moderate)
(6, 'G102', DATE '2025-01-20', 'Dining', 80.00),
(7, 'G102', DATE '2025-01-20', 'Retail', 50.00),
(8, 'G102', DATE '2025-01-21', 'ShoreEx', 150.00),
(9, 'G102', DATE '2025-01-22', 'Dining', 100.00),
(10, 'G102', DATE '2025-01-22', 'Retail', 40.00),

-- Guest 103 (Budget)
(11, 'G103', DATE '2025-01-20', 'Retail', 20.00),
(12, 'G103', DATE '2025-01-21', 'Retail', 15.00),
(13, 'G103', DATE '2025-01-22', 'Dining', 50.00), -- Buffet upcharge
(14, 'G103', DATE '2025-01-20', 'Retail', 10.00),
(15, 'G103', DATE '2025-01-21', 'Retail', 5.00),

-- Others (Bulk)
(16, 'G104', DATE '2025-01-20', 'Casino', 200.00),
(17, 'G104', DATE '2025-01-21', 'Spa', 150.00),
(18, 'G104', DATE '2025-01-22', 'Dining', 200.00),
(19, 'G105', DATE '2025-01-20', 'ShoreEx', 100.00),
(20, 'G105', DATE '2025-01-21', 'Dining', 80.00),

(21, 'G106', DATE '2025-01-20', 'Retail', 30.00),
(22, 'G106', DATE '2025-01-21', 'Retail', 20.00),
(23, 'G107', DATE '2025-01-20', 'Casino', 800.00),
(24, 'G107', DATE '2025-01-21', 'Casino', 500.00),
(25, 'G107', DATE '2025-01-22', 'Dining', 300.00),

(26, 'G108', DATE '2025-01-20', 'Spa', 120.00),
(27, 'G108', DATE '2025-01-21', 'Spa', 150.00),
(28, 'G109', DATE '2025-01-20', 'Retail', 40.00),
(29, 'G109', DATE '2025-01-21', 'Retail', 35.00),
(30, 'G110', DATE '2025-01-20', 'Dining', 200.00),
(31, 'G110', DATE '2025-01-21', 'Dining', 250.00),

-- Day 4 transactions
(32, 'G101', DATE '2025-01-23', 'Casino', 500.00),
(33, 'G102', DATE '2025-01-23', 'Dining', 90.00),
(34, 'G103', DATE '2025-01-23', 'Retail', 25.00),
(35, 'G104', DATE '2025-01-23', 'Casino', 100.00),
(36, 'G105', DATE '2025-01-23', 'ShoreEx', 120.00),
(37, 'G106', DATE '2025-01-23', 'Retail', 15.00),
(38, 'G107', DATE '2025-01-23', 'Casino', 600.00),
(39, 'G108', DATE '2025-01-23', 'Spa', 100.00),
(40, 'G109', DATE '2025-01-23', 'Retail', 50.00),
(41, 'G110', DATE '2025-01-23', 'Dining', 300.00),

-- Day 5 transactions
(42, 'G101', DATE '2025-01-24', 'Dining', 200.00),
(43, 'G102', DATE '2025-01-24', 'Retail', 60.00),
(44, 'G103', DATE '2025-01-24', 'Retail', 10.00),
(45, 'G104', DATE '2025-01-24', 'Spa', 180.00),
(46, 'G105', DATE '2025-01-24', 'Dining', 70.00),
(47, 'G106', DATE '2025-01-24', 'Retail', 22.00),
(48, 'G107', DATE '2025-01-24', 'Casino', 900.00),
(49, 'G108', DATE '2025-01-24', 'ShoreEx', 150.00),
(50, 'G109', DATE '2025-01-24', 'Retail', 30.00);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Daily Spend
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Silver.DailySpend AS
SELECT
    t.GuestID,
    g.RoomType,
    g.Nationality,
    t.Date,
    t.Category,
    SUM(t.Amount) AS DailyTotal
FROM ServicesDB.Bronze.TransactionLog t
JOIN ServicesDB.Bronze.GuestManifest g ON t.GuestID = g.GuestID
GROUP BY t.GuestID, g.RoomType, g.Nationality, t.Date, t.Category;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: High Value Guest Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ServicesDB.Gold.HighValueGuestAnalysis AS
SELECT
    GuestID,
    RoomType,
    Nationality,
    SUM(DailyTotal) AS TotalCruiseSpend,
    SUM(CASE WHEN Category = 'Casino' THEN DailyTotal ELSE 0 END) AS CasinoSpend,
    CASE WHEN SUM(DailyTotal) > 2000 THEN 'VIP' ELSE 'Standard' END AS GuestTier
FROM ServicesDB.Silver.DailySpend
GROUP BY GuestID, RoomType, Nationality;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "List VIP guests and their total spend."
    2. "Calculate average spend by Room Type."
    3. "Which nationality spends most on Dining?"
*/
