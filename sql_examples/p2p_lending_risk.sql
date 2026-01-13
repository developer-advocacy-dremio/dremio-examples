/*
 * P2P Lending Risk Demo
 * 
 * Scenario:
 * A Peer-to-Peer lending platform (like LendingClub) analyzing loan performance.
 * 
 * Data Context:
 * - Loans: Listings funded by investors.
 * - Payments: Monthly payments received.
 * 
 * Analytical Goal:
 * Calculate Annualized Return on Investment (ROI) adjusted for defaults.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS P2PLendingDB;
CREATE FOLDER IF NOT EXISTS P2PLendingDB.Bronze;
CREATE FOLDER IF NOT EXISTS P2PLendingDB.Silver;
CREATE FOLDER IF NOT EXISTS P2PLendingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS P2PLendingDB.Bronze.Listings (
    LoanID VARCHAR,
    Grade VARCHAR, -- 'A', 'B', 'C', 'D'
    Amount DOUBLE,
    InterestRate DOUBLE,
    TermMonths INT
);

CREATE TABLE IF NOT EXISTS P2PLendingDB.Bronze.PaymentHistory (
    PayID INT,
    LoanID VARCHAR,
    MonthNum INT,
    AmountPaid DOUBLE, -- 0 if defaulted
    Status VARCHAR -- 'Current', 'Default'
);

INSERT INTO P2PLendingDB.Bronze.Listings VALUES
('LN-01', 'A', 1000.0, 0.05, 12),
('LN-02', 'B', 1000.0, 0.08, 12),
('LN-03', 'C', 1000.0, 0.12, 12),
('LN-04', 'D', 1000.0, 0.20, 12), -- High risk
('LN-05', 'A', 2000.0, 0.05, 12),
('LN-06', 'C', 1500.0, 0.12, 12),
('LN-07', 'B', 1200.0, 0.08, 12),
('LN-08', 'D', 5000.0, 0.20, 12), -- Big high risk
('LN-09', 'A', 500.0, 0.05, 12),
('LN-10', 'B', 800.0, 0.08, 12);

INSERT INTO P2PLendingDB.Bronze.PaymentHistory VALUES
(1, 'LN-01', 1, 85.0, 'Current'),
(2, 'LN-02', 1, 90.0, 'Current'),
(3, 'LN-03', 1, 95.0, 'Current'),
(4, 'LN-04', 1, 0.0, 'Default'), -- Lost immediately
(5, 'LN-05', 1, 170.0, 'Current'),
(6, 'LN-06', 1, 130.0, 'Current'),
(7, 'LN-07', 1, 105.0, 'Current'),
(8, 'LN-08', 1, 450.0, 'Current'),
(9, 'LN-09', 1, 42.0, 'Current'),
(10, 'LN-10', 1, 70.0, 'Current');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW P2PLendingDB.Silver.LoanStatus AS
SELECT 
    l.Grade,
    l.LoanID,
    l.Amount,
    p.Status
FROM P2PLendingDB.Bronze.Listings l
JOIN P2PLendingDB.Bronze.PaymentHistory p ON l.LoanID = p.LoanID
WHERE p.MonthNum = 1; -- Snapshot at month 1

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW P2PLendingDB.Gold.GradePerformance AS
SELECT 
    Grade,
    COUNT(LoanID) AS TotalLoans,
    SUM(CASE WHEN Status = 'Default' THEN 1 ELSE 0 END) AS Defaults,
    (SUM(CASE WHEN Status = 'Default' THEN 1 ELSE 0 END) / COUNT(LoanID)) * 100 AS DefaultRatePct
FROM P2PLendingDB.Silver.LoanStatus
GROUP BY Grade;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize the DefaultRatePct by Grade as a bar chart from P2PLendingDB.Gold.GradePerformance."
*/
