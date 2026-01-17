/*
 * Syndicated Loan Administration Demo
 * 
 * Scenario:
 * A syndicated loan allows multiple lenders to group together to fund a large borrower.
 * The "Agent Bank" collects payments and distributes them pro-rata to all lenders.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Calculate the exact distribution amount for each lender from a received payment.
 * 
 * Note: Assumes a catalog named 'SyndicateDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SyndicateDB;
CREATE FOLDER IF NOT EXISTS SyndicateDB.Bronze;
CREATE FOLDER IF NOT EXISTS SyndicateDB.Silver;
CREATE FOLDER IF NOT EXISTS SyndicateDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Contract & Payment Data
-------------------------------------------------------------------------------
-- Description: Details of the loan facility and individual payments.

CREATE TABLE IF NOT EXISTS SyndicateDB.Bronze.LoanContracts (
    FacilityID VARCHAR,
    BorrowerName VARCHAR,
    TotalFacilityAmount DOUBLE,
    Currency VARCHAR
);

CREATE TABLE IF NOT EXISTS SyndicateDB.Bronze.LenderShares (
    FacilityID VARCHAR,
    LenderID VARCHAR,
    LenderName VARCHAR,
    SharePercentage FLOAT -- e.g., 0.25 for 25%
);

CREATE TABLE IF NOT EXISTS SyndicateDB.Bronze.PaymentsReceived (
    PaymentID INT,
    FacilityID VARCHAR,
    PaymentDate DATE,
    Amount DOUBLE,
    Type VARCHAR -- 'Interest', 'Principal'
);

-- 1.1 Populate Data (50+ Records across tables)
INSERT INTO SyndicateDB.Bronze.LoanContracts (FacilityID, BorrowerName, TotalFacilityAmount, Currency) VALUES
('FAC-001', 'MegaCorp', 100000000, 'USD'),
('FAC-002', 'GlobalInd', 50000000, 'EUR'),
('FAC-003', 'TechGiant', 200000000, 'USD'),
('FAC-004', 'InfraBuild', 75000000, 'USD'),
('FAC-005', 'GreenEnergy', 30000000, 'EUR');

INSERT INTO SyndicateDB.Bronze.LenderShares (FacilityID, LenderID, LenderName, SharePercentage) VALUES
('FAC-001', 'L-01', 'Bank A', 0.50),
('FAC-001', 'L-02', 'Bank B', 0.30),
('FAC-001', 'L-03', 'Fund C', 0.20),
('FAC-002', 'L-01', 'Bank A', 0.40),
('FAC-002', 'L-04', 'Bank D', 0.60),
('FAC-003', 'L-02', 'Bank B', 0.25),
('FAC-003', 'L-05', 'Bank E', 0.25),
('FAC-003', 'L-06', 'Fund F', 0.50),
('FAC-004', 'L-01', 'Bank A', 0.10),
('FAC-004', 'L-02', 'Bank B', 0.10),
('FAC-004', 'L-03', 'Fund C', 0.80),
('FAC-005', 'L-04', 'Bank D', 0.50),
('FAC-005', 'L-05', 'Bank E', 0.50);

INSERT INTO SyndicateDB.Bronze.PaymentsReceived (PaymentID, FacilityID, PaymentDate, Amount, Type) VALUES
(1, 'FAC-001', '2025-01-01', 500000, 'Interest'),
(2, 'FAC-001', '2025-02-01', 500000, 'Interest'),
(3, 'FAC-001', '2025-03-01', 500000, 'Interest'),
(4, 'FAC-002', '2025-01-15', 200000, 'Interest'),
(5, 'FAC-002', '2025-02-15', 200000, 'Interest'),
(6, 'FAC-003', '2025-01-30', 1000000, 'Interest'),
(7, 'FAC-003', '2025-02-28', 1000000, 'Interest'),
(8, 'FAC-004', '2025-01-10', 300000, 'Interest'),
(9, 'FAC-004', '2025-02-10', 300000, 'Interest'),
(10, 'FAC-005', '2025-01-20', 150000, 'Interest'),
(11, 'FAC-005', '2025-02-20', 150000, 'Interest'),
(12, 'FAC-001', '2025-01-01', 1000000, 'Principal'), -- Paydown
(13, 'FAC-001', '2025-02-01', 1000000, 'Principal'),
(14, 'FAC-001', '2025-03-01', 1000000, 'Principal'),
(15, 'FAC-002', '2025-01-15', 500000, 'Principal'),
(16, 'FAC-002', '2025-02-15', 500000, 'Principal'),
(17, 'FAC-003', '2025-01-30', 2000000, 'Principal'),
(18, 'FAC-003', '2025-02-28', 2000000, 'Principal'),
(19, 'FAC-004', '2025-01-10', 750000, 'Principal'),
(20, 'FAC-004', '2025-02-10', 750000, 'Principal'),
(21, 'FAC-005', '2025-01-20', 300000, 'Principal'),
(22, 'FAC-005', '2025-02-20', 300000, 'Principal'),
(23, 'FAC-001', '2025-04-01', 500000, 'Interest'),
(24, 'FAC-002', '2025-03-15', 200000, 'Interest'),
(25, 'FAC-003', '2025-03-30', 1000000, 'Interest'),
(26, 'FAC-004', '2025-03-10', 300000, 'Interest'),
(27, 'FAC-005', '2025-03-20', 150000, 'Interest'),
(28, 'FAC-001', '2025-04-01', 1000000, 'Principal'),
(29, 'FAC-002', '2025-03-15', 500000, 'Principal'),
(30, 'FAC-003', '2025-03-30', 2000000, 'Principal'),
(31, 'FAC-004', '2025-03-10', 750000, 'Principal'),
(32, 'FAC-005', '2025-03-20', 300000, 'Principal'),
(33, 'FAC-001', '2025-05-01', 500000, 'Interest'),
(34, 'FAC-002', '2025-04-15', 200000, 'Interest'),
(35, 'FAC-003', '2025-04-30', 1000000, 'Interest'),
(36, 'FAC-004', '2025-04-10', 300000, 'Interest'),
(37, 'FAC-005', '2025-04-20', 150000, 'Interest'),
(38, 'FAC-001', '2025-05-01', 1000000, 'Principal'),
(39, 'FAC-002', '2025-04-15', 500000, 'Principal'),
(40, 'FAC-003', '2025-04-30', 2000000, 'Principal'),
(41, 'FAC-004', '2025-04-10', 750000, 'Principal'),
(42, 'FAC-005', '2025-04-20', 300000, 'Principal'),
(43, 'FAC-001', '2025-06-01', 500000, 'Interest'),
(44, 'FAC-002', '2025-05-15', 200000, 'Interest'),
(45, 'FAC-003', '2025-05-30', 1000000, 'Interest'),
(46, 'FAC-004', '2025-05-10', 300000, 'Interest'),
(47, 'FAC-005', '2025-05-20', 150000, 'Interest'),
(48, 'FAC-001', '2025-06-01', 1000000, 'Principal'),
(49, 'FAC-002', '2025-05-15', 500000, 'Principal'),
(50, 'FAC-003', '2025-05-30', 2000000, 'Principal');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Calculated Shares
-------------------------------------------------------------------------------
-- Description: Applying share % to payment amount.
-- Transformation: Distribution = Payment * %

CREATE OR REPLACE VIEW SyndicateDB.Silver.ProRataDistributions AS
SELECT
    p.PaymentID,
    p.FacilityID,
    p.PaymentDate,
    p.Type,
    l.LenderName,
    p.Amount AS TotalReceived,
    l.SharePercentage,
    (p.Amount * l.SharePercentage) AS LenderDistribution
FROM SyndicateDB.Bronze.PaymentsReceived p
JOIN SyndicateDB.Bronze.LenderShares l ON p.FacilityID = l.FacilityID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Statements
-------------------------------------------------------------------------------
-- Description: Aggregating for Lender Reporting.
-- Insight: Total Principal vs Interest per month.

CREATE OR REPLACE VIEW SyndicateDB.Gold.LenderPaymentStatement AS
SELECT
    LenderName,
    FacilityID,
    EXTRACT(MONTH FROM PaymentDate) AS Month,
    Type,
    SUM(LenderDistribution) AS TotalPayout
FROM SyndicateDB.Silver.ProRataDistributions
GROUP BY LenderName, FacilityID, EXTRACT(MONTH FROM PaymentDate), Type;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Generate a payment statement for 'Bank A' showing TotalPayout by Type from SyndicateDB.Gold.LenderPaymentStatement."

PROMPT:
"Using SyndicateDB.Silver.ProRataDistributions, show details of all distributions for 'FAC-001' on '2025-01-01'."
*/
