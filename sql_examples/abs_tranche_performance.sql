/*
 * ABS Tranche Performance Demo
 * 
 * Scenario:
 * An Asset-Backed Security (ABS) is backed by a pool of auto loans.
 * Investors need to monitor the delinquency rate of the underlying loans 
 * to determine if the bond tranches are at risk of write-downs.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Calculate Pool Delinquency Rate.
 * 
 * Note: Assumes a catalog named 'SecuritizationDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SecuritizationDB;
CREATE FOLDER IF NOT EXISTS SecuritizationDB.Bronze;
CREATE FOLDER IF NOT EXISTS SecuritizationDB.Silver;
CREATE FOLDER IF NOT EXISTS SecuritizationDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Loan Tape
-------------------------------------------------------------------------------
-- Description: Loan-level data provided by the servicer.

CREATE TABLE IF NOT EXISTS SecuritizationDB.Bronze.UnderlyingLoans (
    LoanID VARCHAR,
    PoolID VARCHAR,
    OriginalBalance DOUBLE,
    CurrentBalance DOUBLE,
    Status VARCHAR -- 'Current', '30DaysLate', '60DaysLate', 'Default'
);

-- 1.1 Populate UnderlyingLoans (50+ Records)
INSERT INTO SecuritizationDB.Bronze.UnderlyingLoans (LoanID, PoolID, OriginalBalance, CurrentBalance, Status) VALUES
('L-001', 'POOL-A', 25000, 15000, 'Current'),
('L-002', 'POOL-A', 30000, 28000, '30DaysLate'),
('L-003', 'POOL-A', 15000, 5000, 'Current'),
('L-004', 'POOL-A', 22000, 18000, 'Current'),
('L-005', 'POOL-A', 40000, 39000, 'Default'), -- Impairment
('L-006', 'POOL-A', 18000, 10000, 'Current'),
('L-007', 'POOL-A', 26000, 24000, '60DaysLate'),
('L-008', 'POOL-A', 35000, 30000, 'Current'),
('L-009', 'POOL-A', 20000, 12000, 'Current'),
('L-010', 'POOL-A', 28000, 27000, '30DaysLate'),
('L-011', 'POOL-A', 24000, 14000, 'Current'),
('L-012', 'POOL-A', 32000, 29000, 'Current'),
('L-013', 'POOL-A', 16000, 6000, 'Current'),
('L-014', 'POOL-A', 23000, 19000, 'Current'),
('L-015', 'POOL-A', 42000, 41000, 'Current'),
('L-016', 'POOL-A', 19000, 11000, 'Current'),
('L-017', 'POOL-A', 27000, 25000, 'Current'),
('L-018', 'POOL-A', 36000, 31000, '60DaysLate'),
('L-019', 'POOL-A', 21000, 13000, 'Current'),
('L-020', 'POOL-A', 29000, 28000, 'Current'),
('L-021', 'POOL-A', 25500, 15500, 'Current'),
('L-022', 'POOL-A', 31000, 28500, '30DaysLate'),
('L-023', 'POOL-A', 15500, 5500, 'Current'),
('L-024', 'POOL-A', 22500, 18500, 'Current'),
('L-025', 'POOL-A', 41000, 39500, 'Default'),
('L-026', 'POOL-A', 18500, 10500, 'Current'),
('L-027', 'POOL-A', 26500, 24500, 'Current'),
('L-028', 'POOL-A', 35500, 30500, 'Current'),
('L-029', 'POOL-A', 20500, 12500, 'Current'),
('L-030', 'POOL-A', 28500, 27500, 'Current'),
('L-031', 'POOL-A', 25200, 15200, 'Current'),
('L-032', 'POOL-A', 30200, 28200, 'Current'),
('L-033', 'POOL-A', 15200, 5200, 'Current'),
('L-034', 'POOL-A', 22200, 18200, 'Current'),
('L-035', 'POOL-A', 40200, 39200, 'Current'),
('L-036', 'POOL-A', 18200, 10200, 'Current'),
('L-037', 'POOL-A', 26200, 24200, '60DaysLate'),
('L-038', 'POOL-A', 35200, 30200, 'Current'),
('L-039', 'POOL-A', 20200, 12200, 'Current'),
('L-040', 'POOL-A', 28200, 27200, 'Default'),
('L-041', 'POOL-A', 25100, 15100, 'Current'),
('L-042', 'POOL-A', 30100, 28100, 'Current'),
('L-043', 'POOL-A', 15100, 5100, 'Current'),
('L-044', 'POOL-A', 22100, 18100, 'Current'),
('L-045', 'POOL-A', 40100, 39100, 'Current'),
('L-046', 'POOL-A', 18100, 10100, 'Current'),
('L-047', 'POOL-A', 26100, 24100, 'Current'),
('L-048', 'POOL-A', 35100, 30100, 'Current'),
('L-049', 'POOL-A', 20100, 12100, 'Current'),
('L-050', 'POOL-A', 28100, 27100, '30DaysLate');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Pool Aggregates
-------------------------------------------------------------------------------
-- Description: Aggregating balances by status.
-- Transformation: Summing CurrentBalance.

CREATE OR REPLACE VIEW SecuritizationDB.Silver.PoolPerformance AS
SELECT
    PoolID,
    Status,
    COUNT(LoanID) AS LoanCount,
    SUM(CurrentBalance) AS BalanceByStatus
FROM SecuritizationDB.Bronze.UnderlyingLoans
GROUP BY PoolID, Status;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Tranche Health
-------------------------------------------------------------------------------
-- Description: Calculating overall delinquency percentage.
-- Insight: Should be < 5% typically.

CREATE OR REPLACE VIEW SecuritizationDB.Gold.TrancheImpairmentWarning AS
SELECT
    PoolID,
    SUM(CASE WHEN Status IN ('60DaysLate', 'Default') THEN BalanceByStatus ELSE 0 END) AS BadDebtBalance,
    SUM(BalanceByStatus) AS TotalPoolBalance,
    (SUM(CASE WHEN Status IN ('60DaysLate', 'Default') THEN BalanceByStatus ELSE 0 END) / SUM(BalanceByStatus)) * 100 AS DelinquencyRatePct,
    'Check Tranche Triggers' AS Action
FROM SecuritizationDB.Silver.PoolPerformance
GROUP BY PoolID;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the DelinquencyRatePct for Pool 'POOL-A' in SecuritizationDB.Gold.TrancheImpairmentWarning?"

PROMPT:
"Visualize the BalanceByStatus for 'POOL-A' using a pie chart from SecuritizationDB.Silver.PoolPerformance."
*/
