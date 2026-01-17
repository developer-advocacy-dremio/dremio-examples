/*
 * Crypto Custody Reconciliation Demo
 * 
 * Scenario:
 * An institutional crypto custodian tracks client assets in an internal ledger.
 * This must be reconciled daily against the actual on-chain balances from blockchain nodes.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Generate a Proof of Reserves report and identify discrepancies.
 * 
 * Note: Assumes a catalog named 'CryptoCoreDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CryptoCoreDB;
CREATE FOLDER IF NOT EXISTS CryptoCoreDB.Bronze;
CREATE FOLDER IF NOT EXISTS CryptoCoreDB.Silver;
CREATE FOLDER IF NOT EXISTS CryptoCoreDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Balances
-------------------------------------------------------------------------------
-- Description: Ledger data (Off-chain) and Node data (On-chain).

CREATE TABLE IF NOT EXISTS CryptoCoreDB.Bronze.InternalLedger (
    AccountID VARCHAR,
    Asset VARCHAR, -- 'BTC', 'ETH', 'USDC'
    LedgerBalance DOUBLE,
    LastUpdated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS CryptoCoreDB.Bronze.BlockchainNodeData (
    WalletAddress VARCHAR,
    Asset VARCHAR,
    OnChainBalance DOUBLE,
    BlockHeight INT,
    ScanTime TIMESTAMP
);

-- Note: Mapping AccountID to WalletAddress usually happens in a lookup table. 
-- For simplicity, we'll assume 1:1 mapping where AccountID IS the WalletAddress for this demo.

-- 1.1 Populate InternalLedger (50+ Records)
INSERT INTO CryptoCoreDB.Bronze.InternalLedger (AccountID, Asset, LedgerBalance, LastUpdated) VALUES
('W-001', 'BTC', 10.5, TIMESTAMP '2025-01-20 00:00:00'),
('W-002', 'BTC', 5.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-003', 'ETH', 100.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-004', 'USDC', 50000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-005', 'BTC', 2.1, TIMESTAMP '2025-01-20 00:00:00'),
('W-006', 'ETH', 50.5, TIMESTAMP '2025-01-20 00:00:00'),
('W-007', 'BTC', 0.5, TIMESTAMP '2025-01-20 00:00:00'), -- Break
('W-008', 'USDC', 10000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-009', 'ETH', 20.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-010', 'BTC', 100.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-011', 'USDC', 2500.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-012', 'ETH', 15.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-013', 'BTC', 1.2, TIMESTAMP '2025-01-20 00:00:00'),
('W-014', 'USDC', 75000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-015', 'BTC', 8.8, TIMESTAMP '2025-01-20 00:00:00'),
('W-016', 'ETH', 200.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-017', 'BTC', 0.1, TIMESTAMP '2025-01-20 00:00:00'),
('W-018', 'USDC', 300.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-019', 'ETH', 10.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-020', 'BTC', 50.0, TIMESTAMP '2025-01-20 00:00:00'), -- Break
('W-021', 'USDC', 1500.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-022', 'ETH', 5.5, TIMESTAMP '2025-01-20 00:00:00'),
('W-023', 'BTC', 3.3, TIMESTAMP '2025-01-20 00:00:00'),
('W-024', 'USDC', 120000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-025', 'BTC', 0.9, TIMESTAMP '2025-01-20 00:00:00'),
('W-026', 'ETH', 80.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-027', 'BTC', 12.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-028', 'USDC', 4500.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-029', 'ETH', 35.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-030', 'BTC', 6.6, TIMESTAMP '2025-01-20 00:00:00'),
('W-031', 'USDC', 900.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-032', 'ETH', 12.5, TIMESTAMP '2025-01-20 00:00:00'),
('W-033', 'BTC', 4.4, TIMESTAMP '2025-01-20 00:00:00'),
('W-034', 'USDC', 60000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-035', 'BTC', 0.2, TIMESTAMP '2025-01-20 00:00:00'),
('W-036', 'ETH', 150.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-037', 'BTC', 7.7, TIMESTAMP '2025-01-20 00:00:00'),
('W-038', 'USDC', 3300.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-039', 'ETH', 44.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-040', 'BTC', 15.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-041', 'USDC', 100.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-042', 'ETH', 8.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-043', 'BTC', 1.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-044', 'USDC', 20000.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-045', 'BTC', 2.5, TIMESTAMP '2025-01-20 00:00:00'),
('W-046', 'ETH', 90.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-047', 'BTC', 0.3, TIMESTAMP '2025-01-20 00:00:00'),
('W-048', 'USDC', 5500.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-049', 'ETH', 25.0, TIMESTAMP '2025-01-20 00:00:00'),
('W-050', 'BTC', 9.9, TIMESTAMP '2025-01-20 00:00:00');

-- 1.2 Populate BlockchainNodeData (Mostly matches, some breaks)
INSERT INTO CryptoCoreDB.Bronze.BlockchainNodeData (WalletAddress, Asset, OnChainBalance, BlockHeight, ScanTime) VALUES
('W-001', 'BTC', 10.5, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-002', 'BTC', 5.0, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-003', 'ETH', 100.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-004', 'USDC', 50000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-005', 'BTC', 2.1, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-006', 'ETH', 50.5, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-007', 'BTC', 0.45, 800000, TIMESTAMP '2025-01-20 00:05:00'), -- Discrepancy (Fees?)
('W-008', 'USDC', 10000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-009', 'ETH', 20.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-010', 'BTC', 100.0, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-011', 'USDC', 2500.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-012', 'ETH', 15.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-013', 'BTC', 1.2, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-014', 'USDC', 75000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-015', 'BTC', 8.8, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-016', 'ETH', 200.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-017', 'BTC', 0.1, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-018', 'USDC', 300.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-019', 'ETH', 10.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-020', 'BTC', 49.5, 800000, TIMESTAMP '2025-01-20 00:05:00'), -- Discrepancy
('W-021', 'USDC', 1500.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-022', 'ETH', 5.5, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-023', 'BTC', 3.3, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-024', 'USDC', 120000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-025', 'BTC', 0.9, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-026', 'ETH', 80.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-027', 'BTC', 12.0, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-028', 'USDC', 4500.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-029', 'ETH', 35.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-030', 'BTC', 6.6, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-031', 'USDC', 900.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-032', 'ETH', 12.5, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-033', 'BTC', 4.4, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-034', 'USDC', 60000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-035', 'BTC', 0.2, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-036', 'ETH', 150.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-037', 'BTC', 7.7, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-038', 'USDC', 3300.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-039', 'ETH', 44.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-040', 'BTC', 15.0, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-041', 'USDC', 100.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-042', 'ETH', 8.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-043', 'BTC', 1.0, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-044', 'USDC', 20000.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-045', 'BTC', 2.5, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-046', 'ETH', 90.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-047', 'BTC', 0.3, 800000, TIMESTAMP '2025-01-20 00:05:00'),
('W-048', 'USDC', 5500.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-049', 'ETH', 25.0, 18000000, TIMESTAMP '2025-01-20 00:05:00'),
('W-050', 'BTC', 9.9, 800000, TIMESTAMP '2025-01-20 00:05:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Reconciliation
-------------------------------------------------------------------------------
-- Description: Comparing internal vs on-chain.
-- Transformation: Delta = OnChain - Internal.

CREATE OR REPLACE VIEW CryptoCoreDB.Silver.ReconciliationBreaks AS
SELECT
    l.AccountID AS WalletAddress,
    l.Asset,
    l.LedgerBalance,
    n.OnChainBalance,
    (n.OnChainBalance - l.LedgerBalance) AS Discrepancy,
    ABS(n.OnChainBalance - l.LedgerBalance) AS AbsDelta
FROM CryptoCoreDB.Bronze.InternalLedger l
JOIN CryptoCoreDB.Bronze.BlockchainNodeData n 
    ON l.AccountID = n.WalletAddress AND l.Asset = n.Asset;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Proof of Reserves
-------------------------------------------------------------------------------
-- Description: Aggregate holdings and flagged discrepancies.
-- Insight: Discrepancies > 0.001 are flagged.

CREATE OR REPLACE VIEW CryptoCoreDB.Gold.DailyProofOfReserves AS
SELECT
    Asset,
    SUM(LedgerBalance) AS TotalLiability,
    SUM(OnChainBalance) AS TotalReserves,
    SUM(Discrepancy) AS NetVariance,
    CASE 
        WHEN SUM(Discrepancy) >= 0 THEN 'Solvent' 
        ELSE 'Under-Reserved' 
    END AS Status
FROM CryptoCoreDB.Silver.ReconciliationBreaks
GROUP BY Asset;

CREATE OR REPLACE VIEW CryptoCoreDB.Gold.ExceptionsReport AS
SELECT * 
FROM CryptoCoreDB.Silver.ReconciliationBreaks
WHERE AbsDelta > 0.0001;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show the DailyProofOfReserves status for all assets."

PROMPT:
"List all wallets from CryptoCoreDB.Gold.ExceptionsReport that have a significant discrepancy."
*/
