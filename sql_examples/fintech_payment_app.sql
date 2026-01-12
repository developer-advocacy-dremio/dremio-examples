/*
 * Fintech Payment App Demo
 * 
 * Scenario:
 * Tracking P2P transfer volumes and wallet balances to monitor user engagement.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Fintech;
CREATE FOLDER IF NOT EXISTS RetailDB.Fintech.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Fintech.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Fintech.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Fintech.Bronze.AppUsers (
    UserID INT,
    Username VARCHAR,
    SignupDate DATE,
    Region VARCHAR
);

INSERT INTO RetailDB.Fintech.Bronze.AppUsers VALUES
(1, 'user1', '2024-01-01', 'NA'),
(2, 'user2', '2024-01-05', 'NA'),
(3, 'user3', '2024-02-01', 'EU'),
(4, 'user4', '2024-02-10', 'EU'),
(5, 'user5', '2024-03-01', 'APAC'),
(6, 'user6', '2024-03-05', 'APAC'),
(7, 'user7', '2024-04-01', 'NA'),
(8, 'user8', '2024-04-10', 'LATAM'),
(9, 'user9', '2024-05-01', 'LATAM'),
(10, 'user10', '2024-05-15', 'EMEA');


CREATE TABLE IF NOT EXISTS RetailDB.Fintech.Bronze.Transfers (
    TransferID INT,
    SenderID INT,
    ReceiverID INT,
    Amount DOUBLE,
    Currency VARCHAR,
    TxDate TIMESTAMP
);

INSERT INTO RetailDB.Fintech.Bronze.Transfers VALUES
(101, 1, 2, 50.00, 'USD', '2025-01-01 10:00:00'),
(102, 2, 3, 25.00, 'USD', '2025-01-01 11:00:00'),
(103, 3, 1, 10.00, 'EUR', '2025-01-01 12:00:00'),
(104, 4, 5, 100.00, 'EUR', '2025-01-02 09:00:00'),
(105, 5, 6, 200.00, 'JPY', '2025-01-02 10:00:00'),
(106, 6, 1, 5000.00, 'JPY', '2025-01-02 11:00:00'),
(107, 7, 8, 30.00, 'USD', '2025-01-03 15:00:00'),
(108, 8, 9, 20.00, 'MXN', '2025-01-03 16:00:00'),
(109, 9, 10, 15.00, 'MXN', '2025-01-03 17:00:00'),
(110, 1, 4, 75.00, 'USD', '2025-01-04 10:00:00'),
(111, 2, 5, 85.00, 'USD', '2025-01-04 11:00:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Fintech.Silver.UnifiedTransfers AS
SELECT 
    t.TransferID,
    u.Region AS SenderRegion,
    t.SenderID,
    t.ReceiverID,
    t.Amount,
    t.Currency,
    -- Simple FX simulation
    CASE 
        WHEN t.Currency = 'EUR' THEN t.Amount * 1.1 
        WHEN t.Currency = 'JPY' THEN t.Amount * 0.007 
        WHEN t.Currency = 'MXN' THEN t.Amount * 0.05
        ELSE t.Amount 
    END AS AmountUSD
FROM RetailDB.Fintech.Bronze.Transfers t
JOIN RetailDB.Fintech.Bronze.AppUsers u ON t.SenderID = u.UserID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Fintech.Gold.RegionalVolume AS
SELECT 
    SenderRegion,
    COUNT(*) AS TxCount,
    SUM(AmountUSD) AS TotalVolumeUSD
FROM RetailDB.Fintech.Silver.UnifiedTransfers
GROUP BY SenderRegion;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which region has the highest transaction volume in USD according to RetailDB.Fintech.Gold.RegionalVolume?"
*/
