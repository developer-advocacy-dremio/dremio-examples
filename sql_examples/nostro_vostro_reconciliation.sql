/*
 * Nostro/Vostro Reconciliation Demo
 * 
 * Scenario:
 * Banks hold accounts at other banks (Correspondent Banking).
 * "Nostro" = Our money at your bank.
 * "Vostro" = Your money at our bank.
 * We must match our internal records of the Nostro account against the statement sent by the external bank.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify breaks (unmatched transactions).
 * 
 * Note: Assumes a catalog named 'CorrespondentBankingDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS CorrespondentBankingDB;
CREATE FOLDER IF NOT EXISTS CorrespondentBankingDB.Bronze;
CREATE FOLDER IF NOT EXISTS CorrespondentBankingDB.Silver;
CREATE FOLDER IF NOT EXISTS CorrespondentBankingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Ledgers
-------------------------------------------------------------------------------
-- Description: Internal system logs vs External MT950 Statements.

CREATE TABLE IF NOT EXISTS CorrespondentBankingDB.Bronze.InternalMirror (
    TxnRef VARCHAR,
    ValueDate DATE,
    Amount DOUBLE,
    Currency VARCHAR,
    Direction VARCHAR -- 'Debit', 'Credit'
);

CREATE TABLE IF NOT EXISTS CorrespondentBankingDB.Bronze.ExternalStatement (
    StatementID VARCHAR,
    RefID VARCHAR,
    BookingDate DATE,
    Amount DOUBLE,
    Currency VARCHAR,
    Direction VARCHAR
);

-- 1.1 Populate InternalMirror (50+ Records)
INSERT INTO CorrespondentBankingDB.Bronze.InternalMirror (TxnRef, ValueDate, Amount, Currency, Direction) VALUES
('TX-001', '2025-01-01', 10000.00, 'USD', 'Debit'),
('TX-002', '2025-01-01', 5000.00, 'USD', 'Credit'),
('TX-003', '2025-01-02', 2000.00, 'USD', 'Debit'),
('TX-004', '2025-01-02', 15000.00, 'USD', 'Credit'),
('TX-005', '2025-01-03', 300.00, 'USD', 'Debit'),
('TX-006', '2025-01-03', 7500.00, 'USD', 'Credit'),
('TX-007', '2025-01-04', 12000.00, 'USD', 'Debit'),
('TX-008', '2025-01-04', 800.00, 'USD', 'Credit'),
('TX-009', '2025-01-05', 4500.00, 'USD', 'Debit'),
('TX-010', '2025-01-05', 25000.00, 'USD', 'Credit'),
('TX-011', '2025-01-06', 600.00, 'USD', 'Debit'),
('TX-012', '2025-01-06', 9000.00, 'USD', 'Credit'),
('TX-013', '2025-01-07', 3500.00, 'USD', 'Debit'),
('TX-014', '2025-01-07', 18000.00, 'USD', 'Credit'),
('TX-015', '2025-01-08', 200.00, 'USD', 'Debit'),
('TX-016', '2025-01-08', 5500.00, 'USD', 'Credit'),
('TX-017', '2025-01-09', 100.00, 'USD', 'Debit'),
('TX-018', '2025-01-09', 40000.00, 'USD', 'Credit'),
('TX-019', '2025-01-10', 8500.00, 'USD', 'Debit'),
('TX-020', '2025-01-10', 3000.00, 'USD', 'Credit'),
('TX-021', '2025-01-11', 1500.00, 'USD', 'Debit'),
('TX-022', '2025-01-11', 22000.00, 'USD', 'Credit'),
('TX-023', '2025-01-12', 400.00, 'USD', 'Debit'),
('TX-024', '2025-01-12', 6500.00, 'USD', 'Credit'),
('TX-025', '2025-01-13', 900.00, 'USD', 'Debit'), -- Break (Missing in External)
('TX-026', '2025-01-13', 11000.00, 'USD', 'Credit'),
('TX-027', '2025-01-14', 5000.00, 'USD', 'Debit'),
('TX-028', '2025-01-14', 350.00, 'USD', 'Credit'),
('TX-029', '2025-01-15', 7000.00, 'USD', 'Debit'),
('TX-030', '2025-01-15', 1200.00, 'USD', 'Credit'),
('TX-031', '2025-01-16', 2500.00, 'USD', 'Debit'),
('TX-032', '2025-01-16', 8000.00, 'USD', 'Credit'),
('TX-033', '2025-01-17', 150.00, 'USD', 'Debit'),
('TX-034', '2025-01-17', 4500.00, 'USD', 'Credit'),
('TX-035', '2025-01-18', 3000.00, 'USD', 'Debit'),
('TX-036', '2025-01-18', 19000.00, 'USD', 'Credit'),
('TX-037', '2025-01-19', 6000.00, 'USD', 'Debit'),
('TX-038', '2025-01-19', 200.00, 'USD', 'Credit'),
('TX-039', '2025-01-20', 10000.00, 'USD', 'Debit'),
('TX-040', '2025-01-20', 500.00, 'USD', 'Credit'), -- Break (Amount Mismatch)
('TX-041', '2025-01-21', 4000.00, 'USD', 'Debit'),
('TX-042', '2025-01-21', 13000.00, 'USD', 'Credit'),
('TX-043', '2025-01-22', 750.00, 'USD', 'Debit'),
('TX-044', '2025-01-22', 2800.00, 'USD', 'Credit'),
('TX-045', '2025-01-23', 9500.00, 'USD', 'Debit'),
('TX-046', '2025-01-23', 150.00, 'USD', 'Credit'),
('TX-047', '2025-01-24', 2100.00, 'USD', 'Debit'),
('TX-048', '2025-01-24', 6800.00, 'USD', 'Credit'),
('TX-049', '2025-01-25', 300.00, 'USD', 'Debit'),
('TX-050', '2025-01-25', 14000.00, 'USD', 'Credit');

-- 1.2 Populate ExternalStatement (Mostly matches, some breaks)
INSERT INTO CorrespondentBankingDB.Bronze.ExternalStatement (StatementID, RefID, BookingDate, Amount, Currency, Direction) VALUES
('ST-001', 'TX-001', '2025-01-01', 10000.00, 'USD', 'Debit'),
('ST-001', 'TX-002', '2025-01-01', 5000.00, 'USD', 'Credit'),
('ST-001', 'TX-003', '2025-01-02', 2000.00, 'USD', 'Debit'),
('ST-001', 'TX-004', '2025-01-02', 15000.00, 'USD', 'Credit'),
('ST-001', 'TX-005', '2025-01-03', 300.00, 'USD', 'Debit'),
('ST-001', 'TX-006', '2025-01-03', 7500.00, 'USD', 'Credit'),
('ST-001', 'TX-007', '2025-01-04', 12000.00, 'USD', 'Debit'),
('ST-001', 'TX-008', '2025-01-04', 800.00, 'USD', 'Credit'),
('ST-001', 'TX-009', '2025-01-05', 4500.00, 'USD', 'Debit'),
('ST-001', 'TX-010', '2025-01-05', 25000.00, 'USD', 'Credit'),
('ST-001', 'TX-011', '2025-01-06', 600.00, 'USD', 'Debit'),
('ST-001', 'TX-012', '2025-01-06', 9000.00, 'USD', 'Credit'),
('ST-001', 'TX-013', '2025-01-07', 3500.00, 'USD', 'Debit'),
('ST-001', 'TX-014', '2025-01-07', 18000.00, 'USD', 'Credit'),
('ST-001', 'TX-015', '2025-01-08', 200.00, 'USD', 'Debit'),
('ST-001', 'TX-016', '2025-01-08', 5500.00, 'USD', 'Credit'),
('ST-001', 'TX-017', '2025-01-09', 100.00, 'USD', 'Debit'),
('ST-001', 'TX-018', '2025-01-09', 40000.00, 'USD', 'Credit'),
('ST-001', 'TX-019', '2025-01-10', 8500.00, 'USD', 'Debit'),
('ST-001', 'TX-020', '2025-01-10', 3000.00, 'USD', 'Credit'),
('ST-001', 'TX-021', '2025-01-11', 1500.00, 'USD', 'Debit'),
('ST-001', 'TX-022', '2025-01-11', 22000.00, 'USD', 'Credit'),
('ST-001', 'TX-023', '2025-01-12', 400.00, 'USD', 'Debit'),
('ST-001', 'TX-024', '2025-01-12', 6500.00, 'USD', 'Credit'),
-- TX-025 Missing
('ST-001', 'TX-026', '2025-01-13', 11000.00, 'USD', 'Credit'),
('ST-001', 'TX-027', '2025-01-14', 5000.00, 'USD', 'Debit'),
('ST-001', 'TX-028', '2025-01-14', 350.00, 'USD', 'Credit'),
('ST-001', 'TX-029', '2025-01-15', 7000.00, 'USD', 'Debit'),
('ST-001', 'TX-030', '2025-01-15', 1200.00, 'USD', 'Credit'),
('ST-001', 'TX-031', '2025-01-16', 2500.00, 'USD', 'Debit'),
('ST-001', 'TX-032', '2025-01-16', 8000.00, 'USD', 'Credit'),
('ST-001', 'TX-033', '2025-01-17', 150.00, 'USD', 'Debit'),
('ST-001', 'TX-034', '2025-01-17', 4500.00, 'USD', 'Credit'),
('ST-001', 'TX-035', '2025-01-18', 3000.00, 'USD', 'Debit'),
('ST-001', 'TX-036', '2025-01-18', 19000.00, 'USD', 'Credit'),
('ST-001', 'TX-037', '2025-01-19', 6000.00, 'USD', 'Debit'),
('ST-001', 'TX-038', '2025-01-19', 200.00, 'USD', 'Credit'),
('ST-001', 'TX-039', '2025-01-20', 10000.00, 'USD', 'Debit'),
('ST-001', 'TX-040', '2025-01-20', 50.00, 'USD', 'Credit'), -- Mismatch (50 vs 500)
('ST-001', 'TX-041', '2025-01-21', 4000.00, 'USD', 'Debit'),
('ST-001', 'TX-042', '2025-01-21', 13000.00, 'USD', 'Credit'),
('ST-001', 'TX-043', '2025-01-22', 750.00, 'USD', 'Debit'),
('ST-001', 'TX-044', '2025-01-22', 2800.00, 'USD', 'Credit'),
('ST-001', 'TX-045', '2025-01-23', 9500.00, 'USD', 'Debit'),
('ST-001', 'TX-046', '2025-01-23', 150.00, 'USD', 'Credit'),
('ST-001', 'TX-047', '2025-01-24', 2100.00, 'USD', 'Debit'),
('ST-001', 'TX-048', '2025-01-24', 6800.00, 'USD', 'Credit'),
('ST-001', 'TX-049', '2025-01-25', 300.00, 'USD', 'Debit'),
('ST-001', 'TX-050', '2025-01-25', 14000.00, 'USD', 'Credit');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Recon Engine
-------------------------------------------------------------------------------
-- Description: Outer join on Reference ID.

CREATE OR REPLACE VIEW CorrespondentBankingDB.Silver.UnmatchedEntries AS
SELECT
    i.TxnRef AS InternalRef,
    i.Amount AS InternalAmount,
    e.Amount AS ExternalAmount,
    e.RefID AS ExternalRef,
    CASE 
        WHEN e.RefID IS NULL THEN 'Missing in External'
        WHEN i.TxnRef IS NULL THEN 'Missing in Internal'
        WHEN i.Amount <> e.Amount THEN 'Amount Mismatch'
        ELSE 'Matched'
    END AS MatchStatus
FROM CorrespondentBankingDB.Bronze.InternalMirror i
FULL OUTER JOIN CorrespondentBankingDB.Bronze.ExternalStatement e ON i.TxnRef = e.RefID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Aging Breaks
-------------------------------------------------------------------------------
-- Description: Unresolved items report.

CREATE OR REPLACE VIEW CorrespondentBankingDB.Gold.AgingBreaks AS
SELECT
    COALESCE(InternalRef, ExternalRef) AS Reference,
    MatchStatus,
    COALESCE(InternalAmount, 0) AS OurBook,
    COALESCE(ExternalAmount, 0) AS TheirBook,
    (COALESCE(InternalAmount, 0) - COALESCE(ExternalAmount, 0)) AS Variance
FROM CorrespondentBankingDB.Silver.UnmatchedEntries
WHERE MatchStatus <> 'Matched';

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Show all discrepancies from CorrespondentBankingDB.Gold.AgingBreaks."

PROMPT:
"Count the number of matched vs unmatched transactions in CorrespondentBankingDB.Silver.UnmatchedEntries."
*/
