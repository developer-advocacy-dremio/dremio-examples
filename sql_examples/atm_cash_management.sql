/*
 * ATM Cash Management Demo
 * 
 * Scenario:
 * Managing cash liquidity across a network of ATMs to minimize "Out of Service" events
 * while optimizing the cost of cash (interest lost on idle cash) and replenishment logistics costs.
 * 
 * Data Context:
 * - AtmLogs: Transactional logs from each machine.
 * - AtmHardware: Location, model, and cash cassette capacity.
 * 
 * Analytical Goal:
 * Predict which ATMs will run out of cash within the next 24 hours based on withdrawal velocity.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ATMFleetDB;
CREATE FOLDER IF NOT EXISTS ATMFleetDB.Bronze;
CREATE FOLDER IF NOT EXISTS ATMFleetDB.Silver;
CREATE FOLDER IF NOT EXISTS ATMFleetDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ATMFleetDB.Bronze.ATMs (
    AtmID VARCHAR,
    Location VARCHAR,
    MaxCapacity DOUBLE,
    LastReplenished TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ATMFleetDB.Bronze.Transactions (
    TxnID INT,
    AtmID VARCHAR,
    "Timestamp" TIMESTAMP,
    TxnType VARCHAR, -- 'Withdrawal', 'BalanceInquiry'
    AmountWithdrawn DOUBLE
);

INSERT INTO ATMFleetDB.Bronze.ATMs VALUES
('ATM-001', 'Downtown Main St', 50000.0, '2025-09-01 06:00:00'),
('ATM-002', 'Airport Terminal 1', 100000.0, '2025-09-01 05:00:00'),
('ATM-003', 'Suburban Mall', 40000.0, '2025-09-01 07:00:00');

-- 10+ Transactions
INSERT INTO ATMFleetDB.Bronze.Transactions VALUES
(1, 'ATM-001', '2025-09-01 08:30:00', 'Withdrawal', 200.0),
(2, 'ATM-001', '2025-09-01 08:45:00', 'Withdrawal', 500.0),
(3, 'ATM-001', '2025-09-01 09:10:00', 'Withdrawal', 100.0),
(4, 'ATM-001', '2025-09-01 10:00:00', 'Withdrawal', 800.0), -- High velocity
(5, 'ATM-002', '2025-09-01 06:15:00', 'Withdrawal', 200.0),
(6, 'ATM-002', '2025-09-01 06:20:00', 'BalanceInquiry', 0.0),
(7, 'ATM-002', '2025-09-01 07:30:00', 'Withdrawal', 1000.0),
(8, 'ATM-003', '2025-09-01 11:00:00', 'Withdrawal', 60.0),
(9, 'ATM-001', '2025-09-01 11:30:00', 'Withdrawal', 300.0),
(10, 'ATM-001', '2025-09-01 12:00:00', 'Withdrawal', 400.0),
(11, 'ATM-002', '2025-09-01 08:00:00', 'Withdrawal', 200.0),
(12, 'ATM-003', '2025-09-01 13:00:00', 'Withdrawal', 100.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ATMFleetDB.Silver.AtmCurrentLevels AS
SELECT 
    a.AtmID,
    a.Location,
    a.MaxCapacity,
    a.LastReplenished,
    COALESCE(SUM(t.AmountWithdrawn), 0) AS TotalWithdrawnSinceRefill,
    (a.MaxCapacity - COALESCE(SUM(t.AmountWithdrawn), 0)) AS CurrentBalance
FROM ATMFleetDB.Bronze.ATMs a
LEFT JOIN ATMFleetDB.Bronze.Transactions t 
    ON a.AtmID = t.AtmID AND t."Timestamp" > a.LastReplenished
GROUP BY a.AtmID, a.Location, a.MaxCapacity, a.LastReplenished;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ATMFleetDB.Gold.ReplenishmentSchedule AS
SELECT 
    AtmID,
    Location,
    CurrentBalance,
    MaxCapacity,
    (CurrentBalance / MaxCapacity) * 100 AS PercentFull,
    CASE 
        WHEN (CurrentBalance / MaxCapacity) < 0.20 THEN 'Critical'
        WHEN (CurrentBalance / MaxCapacity) < 0.40 THEN 'Warning'
        ELSE 'OK'
    END AS Status
FROM ATMFleetDB.Silver.AtmCurrentLevels;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show me a pie chart of ATMs by Status from ATMFleetDB.Gold.ReplenishmentSchedule."

PROMPT:
"Which ATM has the lowest PercentFull right now and needs immediate replenishment?"
*/
