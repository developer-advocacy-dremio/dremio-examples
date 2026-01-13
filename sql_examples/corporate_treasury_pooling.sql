/*
 * Corporate Treasury Pooling Demo
 * 
 * Scenario:
 * A multinational corporation managing cash across multiple subsidiaries and currencies.
 * The goal is to perform a daily "Cash Sweep" to centralize liquidity and invest excess funds.
 * 
 * Data Context:
 * - SubAccounts: Local bank accounts for subsidiaries (USD, EUR, GBP).
 * - FXRates: Daily exchange rates to the functional currency (USD).
 * 
 * Analytical Goal:
 * Calculate the total Global Cash Position in USD to determine net borrowing or investment needs.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS TreasuryDB;
CREATE FOLDER IF NOT EXISTS TreasuryDB.Bronze;
CREATE FOLDER IF NOT EXISTS TreasuryDB.Silver;
CREATE FOLDER IF NOT EXISTS TreasuryDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TreasuryDB.Bronze.SubAccounts (
    AccountID VARCHAR,
    EntityName VARCHAR,
    Currency VARCHAR, -- 'USD', 'EUR', 'GBP', 'JPY'
    Balance DOUBLE,
    LastUpdated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS TreasuryDB.Bronze.FXRates (
    CurrencyPair VARCHAR, -- 'EURUSD', 'GBPUSD'
    Rate DOUBLE,
    RateDate DATE
);

INSERT INTO TreasuryDB.Bronze.SubAccounts VALUES
('ACC-001', 'US_HQ', 'USD', 5000000.0, '2025-10-01 17:00:00'),
('ACC-002', 'Germany_Ops', 'EUR', 200000.0, '2025-10-01 17:00:00'),
('ACC-003', 'France_Sales', 'EUR', -50000.0, '2025-10-01 17:00:00'), -- Overdraft
('ACC-004', 'UK_Marketing', 'GBP', 150000.0, '2025-10-01 17:00:00'),
('ACC-005', 'Japan_R&D', 'JPY', 10000000.0, '2025-10-01 17:00:00'),
('ACC-006', 'US_Sales_West', 'USD', 250000.0, '2025-10-01 17:00:00'),
('ACC-007', 'Singapore_Hub', 'SGD', 300000.0, '2025-10-01 17:00:00'),
('ACC-008', 'Canada_Logistics', 'CAD', -20000.0, '2025-10-01 17:00:00'), -- Overdraft
('ACC-009', 'Brazil_Mfg', 'BRL', 500000.0, '2025-10-01 17:00:00'),
('ACC-010', 'US_Sales_East', 'USD', 400000.0, '2025-10-01 17:00:00');

INSERT INTO TreasuryDB.Bronze.FXRates VALUES
('EURUSD', 1.08, '2025-10-01'),
('GBPUSD', 1.25, '2025-10-01'),
('JPYUSD', 0.0067, '2025-10-01'),
('USDUSD', 1.00, '2025-10-01'),
('SGDUSD', 0.73, '2025-10-01'),
('CADUSD', 0.74, '2025-10-01'),
('BRLUSD', 0.18, '2025-10-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TreasuryDB.Silver.NormalizedBalances AS
SELECT 
    a.AccountID,
    a.EntityName,
    a.Currency,
    a.Balance,
    COALESCE(r.Rate, 1.0) AS ExchangeRate,
    (a.Balance * COALESCE(r.Rate, 1.0)) AS BalanceUSD
FROM TreasuryDB.Bronze.SubAccounts a
LEFT JOIN TreasuryDB.Bronze.FXRates r 
    ON a.Currency || 'USD' = r.CurrencyPair 
    OR (a.Currency = 'USD' AND r.CurrencyPair = 'USDUSD')
WHERE r.RateDate = '2025-10-01' OR a.Currency = 'USD';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TreasuryDB.Gold.GlobalCashPosition AS
SELECT 
    'Global Corp Pool' AS Portfolio,
    SUM(BalanceUSD) AS TotalLiquidityUSD,
    SUM(CASE WHEN BalanceUSD < 0 THEN BalanceUSD ELSE 0 END) AS TotalOverdraftsUSD
FROM TreasuryDB.Silver.NormalizedBalances;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the TotalLiquidityUSD currently available in TreasuryDB.Gold.GlobalCashPosition?"

PROMPT:
"Visualize BalanceUSD by EntityName as a bar chart to see where our cash is sitting."
*/
