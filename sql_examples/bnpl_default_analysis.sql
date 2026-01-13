/*
 * BNPL (Buy Now Pay Later) Default Analysis
 * 
 * Scenario:
 * Analyzing delinquencies in micro-loan installments typically split into 4 payments.
 * 
 * Data Context:
 * - Loans: Info on the purchase, merchant, and customer.
 * - Installments: Tracking scheduled vs actual payments.
 * 
 * Analytical Goal:
 * Identify delinquent cohorts (e.g., missed 2nd payment) to trigger collections or freeze accounts.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS FinTechBNPL_DB;
CREATE FOLDER IF NOT EXISTS FinTechBNPL_DB.Bronze;
CREATE FOLDER IF NOT EXISTS FinTechBNPL_DB.Silver;
CREATE FOLDER IF NOT EXISTS FinTechBNPL_DB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FinTechBNPL_DB.Bronze.Loans (
    LoanID VARCHAR,
    CustomerID VARCHAR,
    Merchant VARCHAR,
    PurchaseDate DATE,
    TotalAmount DOUBLE
);

CREATE TABLE IF NOT EXISTS FinTechBNPL_DB.Bronze.Installments (
    InstallmentID INT,
    LoanID VARCHAR,
    SequenceNumber INT, -- 1, 2, 3, 4
    DueDate DATE,
    Status VARCHAR -- 'Paid', 'Pending', 'Overdue'
);

INSERT INTO FinTechBNPL_DB.Bronze.Loans VALUES
('L-001', 'C-100', 'ClothesRUs', '2025-01-01', 200.0),
('L-002', 'C-101', 'TechStore', '2025-01-02', 1000.0),
('L-003', 'C-102', 'Shoes4U', '2025-01-03', 150.0),
('L-004', 'C-103', 'GymGear', '2025-01-05', 400.0),
('L-005', 'C-100', 'TechStore', '2025-02-01', 500.0);

INSERT INTO FinTechBNPL_DB.Bronze.Installments VALUES
-- Loan 1 (Good)
(1, 'L-001', 1, '2025-01-01', 'Paid'),
(2, 'L-001', 2, '2025-01-15', 'Paid'),
(3, 'L-001', 3, '2025-01-29', 'Paid'),
(4, 'L-001', 4, '2025-02-12', 'Paid'),
-- Loan 2 (Bad - Missed 2nd)
(5, 'L-002', 1, '2025-01-02', 'Paid'),
(6, 'L-002', 2, '2025-01-16', 'Overdue'),
(7, 'L-002', 3, '2025-01-30', 'Pending'),
(8, 'L-002', 4, '2025-02-13', 'Pending'),
-- Loan 3 (Paid first 2)
(9, 'L-003', 1, '2025-01-03', 'Paid'),
(10, 'L-003', 2, '2025-01-17', 'Paid'),
-- Loan 4 (Default on 1st!)
(11, 'L-004', 1, '2025-01-05', 'Overdue'),
(12, 'L-004', 2, '2025-01-19', 'Pending'),
-- Loan 5
(13, 'L-005', 1, '2025-02-01', 'Paid');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FinTechBNPL_DB.Silver.LoanHealth AS
SELECT 
    l.LoanID,
    l.CustomerID,
    l.TotalAmount,
    COUNT(CASE WHEN i.Status = 'Paid' THEN 1 END) AS PaidCount,
    COUNT(CASE WHEN i.Status = 'Overdue' THEN 1 END) AS OverdueCount
FROM FinTechBNPL_DB.Bronze.Loans l
JOIN FinTechBNPL_DB.Bronze.Installments i ON l.LoanID = i.LoanID
GROUP BY l.LoanID, l.CustomerID, l.TotalAmount;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW FinTechBNPL_DB.Gold.DelinquencyReport AS
SELECT 
    LoanID,
    CustomerID,
    TotalAmount,
    OverdueCount,
    CASE 
        WHEN OverdueCount >= 1 THEN 'At Risk'
        WHEN OverdueCount = 0 AND PaidCount = 4 THEN 'Completed'
        ELSE 'Good Standing'
    END AS AccountStatus
FROM FinTechBNPL_DB.Silver.LoanHealth;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"List all Loans marked as 'At Risk' in FinTechBNPL_DB.Gold.DelinquencyReport."

PROMPT:
"What is the total Outstanding Amount for all 'At Risk' loans?"
*/
