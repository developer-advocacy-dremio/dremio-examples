/*
 * Dremio "Messy Data" Challenge: Cross-Border Crypto Payments
 * 
 * Scenario: 
 * Crypto assets with high-precision decimals.
 * 3 Tables: WALLETS (KYC), TRANSACTIONS (Ledger), EXCHANGE_RATES (Forex).
 * 
 * Objective for AI Agent:
 * 1. Normalize Symbol: BTC/XBT -> BTC.
 * 2. Join Rates: Convert Crypto Amount to USD using Approx Rate.
 * 3. Flag High Risk: Transactions > $10k USD without KYC (Wallet joined to 'Unknown').
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Crypto_Ex;
CREATE FOLDER IF NOT EXISTS Crypto_Ex.Ledger;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Crypto_Ex.Ledger.WALLETS (
    WALLET_ADDR VARCHAR,
    OWNER_ID VARCHAR,
    KYC_STATUS VARCHAR -- 'Verified', 'Pending', 'None'
);

CREATE TABLE IF NOT EXISTS Crypto_Ex.Ledger.TRANSACTIONS (
    TX_HASH VARCHAR,
    FROM_ADDR VARCHAR,
    TO_ADDR VARCHAR,
    SYMBOL VARCHAR,
    AMOUNT DOUBLE,
    TS TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Crypto_Ex.Ledger.EXCHANGE_RATES (
    SYMBOL VARCHAR,
    RATE_USD DOUBLE,
    DATE_VAL DATE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- RATES
INSERT INTO Crypto_Ex.Ledger.EXCHANGE_RATES VALUES
('BTC', 30000.0, '2023-01-01'), ('ETH', 2000.0, '2023-01-01'), ('SOL', 20.0, '2023-01-01'),
('DOGE', 0.08, '2023-01-01'), ('XRP', 0.5, '2023-01-01'), ('ADA', 0.4, '2023-01-01');

-- WALLETS
INSERT INTO Crypto_Ex.Ledger.WALLETS VALUES
('1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', 'Alice', 'Verified'),
('3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy', 'Bob', 'None'),
('0x742d35Cc6634C0532925a3b844Bc454e4438f44e', 'Charlie', 'Pending'),
('bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq', 'Dave', 'Verified'),
('0x123', 'Mallory', 'None');

-- TRANSACTIONS (50 Rows)
INSERT INTO Crypto_Ex.Ledger.TRANSACTIONS VALUES
('TX-1', '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', '3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy', 'BTC', 0.5, '2023-01-01 10:00:00'),
('TX-2', 'bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq', '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', 'XBT', 1.5, '2023-01-01 10:05:00'), -- XBT
('TX-3', '0x742d35Cc6634C0532925a3b844Bc454e4438f44e', '0x123', 'ETH', 10.5, '2023-01-01 10:10:00'),
('TX-4', '0X742d35cc6634c0532925a3b844bc454e4438f44e', '0x456', 'Ether', 5.0, '2023-01-01 10:15:00'), -- Ether
('TX-5', 'InvalidAddr', '0x123', 'ETH', 1.0, '2023-01-01 10:20:00'),
('TX-6', 'A', 'B', 'BTC', 0.12345678, '2023-01-01 10:25:00'),
('TX-7', 'A', 'B', 'USDT', 100.0, '2023-01-01 10:30:00'),
('TX-8', 'A', 'B', 'USDC', 100.0, '2023-01-01 10:35:00'),
('TX-9', 'A', 'B', 'DAI', 100.0, '2023-01-01 10:40:00'),
('TX-10', 'A', 'B', 'DOGE', 1000.0, '2023-01-01 10:45:00'),
('TX-11', 'A', 'B', 'SHIB', 1000000.0, '2023-01-01 10:50:00'),
('TX-12', 'A', 'B', 'LTC', 5.0, '2023-01-01 10:55:00'),
('TX-13', 'A', 'B', 'Litecoin', 5.0, '2023-01-01 11:00:00'),
('TX-14', 'A', 'B', 'ETH', 1.0, '2023-01-01 11:05:00'),
('TX-15', 'A', 'B', 'ETH', 1.0, '2023-01-01 11:10:00'),
('TX-16', 'A', 'B', 'ETH', 1.0, '2023-01-01 11:15:00'),
('TX-17', 'A', 'B', 'BTC', -0.1, '2023-01-01 11:20:00'),
('TX-18', 'A', 'B', 'BTC', 0.0, '2023-01-01 11:25:00'),
('TX-19', 'A', 'B', 'BITCOIN', 1.0, '2023-01-01 11:30:00'),
('TX-20', 'A', 'B', 'btc', 1.0, '2023-01-01 11:35:00'),
('TX-21', 'A', 'B', 'Eth', 1.0, '2023-01-01 11:40:00'),
('TX-22', 'A', 'B', 'eth', 1.0, '2023-01-01 11:45:00'),
('TX-23', 'A', 'B', 'SOL', 10.0, '2023-01-01 11:50:00'),
('TX-24', 'A', 'B', 'Solana', 10.0, '2023-01-01 11:55:00'),
('TX-25', 'A', 'B', 'ADA', 100.0, '2023-01-01 12:00:00'),
('TX-26', 'A', 'B', 'Cardano', 100.0, '2023-01-01 12:05:00'),
('TX-27', 'A', 'B', 'XRP', 50.0, '2023-01-01 12:10:00'),
('TX-28', 'A', 'B', 'Ripple', 50.0, '2023-01-01 12:15:00'),
('TX-29', 'A', 'B', 'DOT', 10.0, '2023-01-01 12:20:00'),
('TX-30', 'A', 'B', 'Polkadot', 10.0, '2023-01-01 12:25:00'),
('TX-31', 'A', 'B', 'LINK', 20.0, '2023-01-01 12:30:00'),
('TX-32', 'A', 'B', 'UNI', 20.0, '2023-01-01 12:35:00'),
('TX-33', 'A', 'B', 'MATIC', 100.0, '2023-01-01 12:40:00'),
('TX-34', 'A', 'B', 'Polygon', 100.0, '2023-01-01 12:45:00'),
('TX-35', 'A', 'B', 'AVAX', 5.0, '2023-01-01 12:50:00'),
('TX-36', 'A', 'B', 'ATOM', 5.0, '2023-01-01 12:55:00'),
('TX-37', 'A', 'B', 'XMR', 2.0, '2023-01-01 13:00:00'),
('TX-38', 'A', 'B', 'Monero', 2.0, '2023-01-01 13:05:00'),
('TX-39', 'A', 'B', 'ETC', 10.0, '2023-01-01 13:10:00'),
('TX-40', 'A', 'B', 'BCH', 1.0, '2023-01-01 13:15:00'),
('TX-41', 'A', 'B', 'FIL', 10.0, '2023-01-01 13:20:00'),
('TX-42', 'A', 'B', 'TRX', 1000.0, '2023-01-01 13:25:00'),
('TX-43', 'A', 'B', 'VET', 1000.0, '2023-01-01 13:30:00'),
('TX-44', 'A', 'B', 'ALGO', 100.0, '2023-01-01 13:35:00'),
('TX-45', 'A', 'B', 'ICP', 5.0, '2023-01-01 13:40:00'),
('TX-46', 'A', 'B', 'FTM', 100.0, '2023-01-01 13:45:00'),
('TX-47', 'A', 'B', 'SAND', 100.0, '2023-01-01 13:50:00'),
('TX-48', 'A', 'B', 'MANA', 100.0, '2023-01-01 13:55:00'),
('TX-49', 'A', 'B', 'AXS', 10.0, '2023-01-01 14:00:00'),
('TX-50', 'A', 'B', 'XTZ', 10.0, '2023-01-01 14:05:00');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit crypto transactions in Crypto_Ex.Ledger.
 *  
 *  1. Bronze: Raw View of TRANSACTIONS, WALLETS, RATES.
 *  2. Silver: 
 *     - Standardize Symbol: 'Bit%' -> 'BTC', 'Eth%' -> 'ETH'.
 *     - Join Rates: Lookup RATE_USD based on Date (or latest).
 *     - Calc 'USD_Value': Amount * Rate_USD.
 *  3. Gold: 
 *     - Risk Report: Tranasctions > $10,000 where Wallet KYC = 'None'.
 *  
 *  Show the SQL."
 */
