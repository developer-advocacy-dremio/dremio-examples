/*
 * Fixed Income Bond Laddering Demo
 * 
 * Scenario:
 * Portfolio managers build "Bond Ladders" where bonds mature at regular intervals 
 * to provide steady cash flow and reduce interest rate risk.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Visualize the maturity schedule and projected cash flows.
 * 
 * Note: Assumes a catalog named 'FixedIncomeDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS FixedIncomeDB;
CREATE FOLDER IF NOT EXISTS FixedIncomeDB.Bronze;
CREATE FOLDER IF NOT EXISTS FixedIncomeDB.Silver;
CREATE FOLDER IF NOT EXISTS FixedIncomeDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Portfolio Data
-------------------------------------------------------------------------------
-- Description: Current Holdings and Security Master details.

CREATE TABLE IF NOT EXISTS FixedIncomeDB.Bronze.BondHoldings (
    ISIN VARCHAR,
    PortfolioID VARCHAR,
    QtyHeld INT,
    PurchasePrice DOUBLE
);

CREATE TABLE IF NOT EXISTS FixedIncomeDB.Bronze.BondMaster (
    ISIN VARCHAR,
    Issuer VARCHAR,
    CouponRate DOUBLE, -- e.g., 0.05 for 5%
    MaturityDate DATE,
    FaceValue INT
);

-- 1.1 Populate Data (50+ Records)
INSERT INTO FixedIncomeDB.Bronze.BondHoldings (ISIN, PortfolioID, QtyHeld, PurchasePrice) VALUES
('US1234567890', 'P-001', 1000, 99.5),
('US0987654321', 'P-001', 500, 101.0),
('US1122334455', 'P-001', 2000, 98.0),
('US5544332211', 'P-001', 1500, 100.0),
('US6677889900', 'P-001', 750, 97.5),
('US9988776655', 'P-002', 3000, 99.0),
('US1231231234', 'P-002', 1200, 102.0),
('US3213214321', 'P-002', 2500, 96.0),
('US7778889999', 'P-001', 500, 100.5),
('US0001112223', 'P-001', 1800, 95.5),
('US1234567891', 'P-001', 1000, 99.5),
('US0987654322', 'P-001', 500, 101.0),
('US1122334456', 'P-001', 2000, 98.0),
('US5544332212', 'P-001', 1500, 100.0),
('US6677889901', 'P-001', 750, 97.5),
('US9988776656', 'P-002', 3000, 99.0),
('US1231231235', 'P-002', 1200, 102.0),
('US3213214322', 'P-002', 2500, 96.0),
('US7778889990', 'P-001', 500, 100.5),
('US0001112224', 'P-001', 1800, 95.5),
('US1234567892', 'P-001', 1000, 99.5),
('US0987654323', 'P-001', 500, 101.0),
('US1122334457', 'P-001', 2000, 98.0),
('US5544332213', 'P-001', 1500, 100.0),
('US6677889902', 'P-001', 750, 97.5),
('US9988776657', 'P-002', 3000, 99.0),
('US1231231236', 'P-002', 1200, 102.0),
('US3213214323', 'P-002', 2500, 96.0),
('US7778889991', 'P-001', 500, 100.5),
('US0001112225', 'P-001', 1800, 95.5),
('US1234567893', 'P-001', 1000, 99.5),
('US0987654324', 'P-001', 500, 101.0),
('US1122334458', 'P-001', 2000, 98.0),
('US5544332214', 'P-001', 1500, 100.0),
('US6677889903', 'P-001', 750, 97.5),
('US9988776658', 'P-002', 3000, 99.0),
('US1231231237', 'P-002', 1200, 102.0),
('US3213214324', 'P-002', 2500, 96.0),
('US7778889992', 'P-001', 500, 100.5),
('US0001112226', 'P-001', 1800, 95.5),
('US1234567894', 'P-001', 1000, 99.5),
('US0987654325', 'P-001', 500, 101.0),
('US1122334459', 'P-001', 2000, 98.0),
('US5544332215', 'P-001', 1500, 100.0),
('US6677889904', 'P-001', 750, 97.5),
('US9988776659', 'P-002', 3000, 99.0),
('US1231231238', 'P-002', 1200, 102.0),
('US3213214325', 'P-002', 2500, 96.0),
('US7778889993', 'P-001', 500, 100.5),
('US0001112227', 'P-001', 1800, 95.5);

INSERT INTO FixedIncomeDB.Bronze.BondMaster (ISIN, Issuer, CouponRate, MaturityDate, FaceValue) VALUES
('US1234567890', 'US_Gov', 0.04, '2025-06-30', 1000),
('US0987654321', 'Apple', 0.035, '2025-12-31', 1000),
('US1122334455', 'Microsoft', 0.045, '2026-06-30', 1000),
('US5544332211', 'Amazon', 0.03, '2026-12-31', 1000),
('US6677889900', 'Tesla', 0.05, '2027-06-30', 1000),
('US9988776655', 'US_Gov', 0.02, '2027-12-31', 1000),
('US1231231234', 'CocaCola', 0.025, '2028-06-30', 1000),
('US3213214321', 'Pepsi', 0.03, '2028-12-31', 1000),
('US7778889999', 'IBM', 0.04, '2029-06-30', 1000),
('US0001112223', 'Intel', 0.045, '2029-12-31', 1000),
('US1234567891', 'US_Gov', 0.04, '2030-06-30', 1000),
('US0987654322', 'Apple', 0.035, '2030-12-31', 1000),
('US1122334456', 'Microsoft', 0.045, '2031-06-30', 1000),
('US5544332212', 'Amazon', 0.03, '2031-12-31', 1000),
('US6677889901', 'Tesla', 0.05, '2032-06-30', 1000),
('US9988776656', 'US_Gov', 0.02, '2032-12-31', 1000),
('US1231231235', 'CocaCola', 0.025, '2033-06-30', 1000),
('US3213214322', 'Pepsi', 0.03, '2033-12-31', 1000),
('US7778889990', 'IBM', 0.04, '2034-06-30', 1000),
('US0001112224', 'Intel', 0.045, '2034-12-31', 1000),
('US1234567892', 'US_Gov', 0.04, '2035-06-30', 1000),
('US0987654323', 'Apple', 0.035, '2035-12-31', 1000),
('US1122334457', 'Microsoft', 0.045, '2036-06-30', 1000),
('US5544332213', 'Amazon', 0.03, '2036-12-31', 1000),
('US6677889902', 'Tesla', 0.05, '2037-06-30', 1000),
('US9988776657', 'US_Gov', 0.02, '2037-12-31', 1000),
('US1231231236', 'CocaCola', 0.025, '2038-06-30', 1000),
('US3213214323', 'Pepsi', 0.03, '2038-12-31', 1000),
('US7778889991', 'IBM', 0.04, '2039-06-30', 1000),
('US0001112225', 'Intel', 0.045, '2039-12-31', 1000),
('US1234567893', 'US_Gov', 0.04, '2040-06-30', 1000),
('US0987654324', 'Apple', 0.035, '2040-12-31', 1000),
('US1122334458', 'Microsoft', 0.045, '2041-06-30', 1000),
('US5544332214', 'Amazon', 0.03, '2041-12-31', 1000),
('US6677889903', 'Tesla', 0.05, '2042-06-30', 1000),
('US9988776658', 'US_Gov', 0.02, '2042-12-31', 1000),
('US1231231237', 'CocaCola', 0.025, '2043-06-30', 1000),
('US3213214324', 'Pepsi', 0.03, '2043-12-31', 1000),
('US7778889992', 'IBM', 0.04, '2044-06-30', 1000),
('US0001112226', 'Intel', 0.045, '2044-12-31', 1000),
('US1234567894', 'US_Gov', 0.04, '2045-06-30', 1000),
('US0987654325', 'Apple', 0.035, '2045-12-31', 1000),
('US1122334459', 'Microsoft', 0.045, '2046-06-30', 1000),
('US5544332215', 'Amazon', 0.03, '2046-12-31', 1000),
('US6677889904', 'Tesla', 0.05, '2047-06-30', 1000),
('US9988776659', 'US_Gov', 0.02, '2047-12-31', 1000),
('US1231231238', 'CocaCola', 0.025, '2048-06-30', 1000),
('US3213214325', 'Pepsi', 0.03, '2048-12-31', 1000),
('US7778889993', 'IBM', 0.04, '2049-06-30', 1000),
('US0001112227', 'Intel', 0.045, '2049-12-31', 1000);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Cash Flows
-------------------------------------------------------------------------------
-- Description: Calculating current valuation and projected principal return date.

CREATE OR REPLACE VIEW FixedIncomeDB.Silver.CashFlowSchedule AS
SELECT
    h.PortfolioID,
    h.ISIN,
    m.Issuer,
    h.QtyHeld,
    m.FaceValue,
    m.MaturityDate,
    (h.QtyHeld * m.FaceValue) AS PrincipalPayment,
    (h.QtyHeld * m.FaceValue * m.CouponRate) AS AnnualCouponPayment
FROM FixedIncomeDB.Bronze.BondHoldings h
JOIN FixedIncomeDB.Bronze.BondMaster m ON h.ISIN = m.ISIN;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Maturity Analysis
-------------------------------------------------------------------------------
-- Description: Aggregating inflows by Year to visualize the "Ladder".
-- Insight: Ensuring no huge gaps in cash availability.

CREATE OR REPLACE VIEW FixedIncomeDB.Gold.MaturityLadder AS
SELECT
    PortfolioID,
    EXTRACT(YEAR FROM MaturityDate) AS MaturityYear,
    SUM(PrincipalPayment) AS PrincipalInflow,
    SUM(AnnualCouponPayment) AS CouponInflow
FROM FixedIncomeDB.Silver.CashFlowSchedule
GROUP BY PortfolioID, EXTRACT(YEAR FROM MaturityDate)
ORDER BY MaturityYear ASC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Visualize the MaturityLadder for 'P-001' using a bar chart of PrincipalInflow by MaturityYear."

PROMPT:
"Which year has the highest total projected inflow for PortfolioID 'P-002'?"
*/
