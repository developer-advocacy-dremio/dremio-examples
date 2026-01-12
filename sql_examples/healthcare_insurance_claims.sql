/*
 * Healthcare Insurance Claims Demo
 * 
 * Scenario:
 * Analyzing claims adjudication speed and denial reasons.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Claims;
CREATE FOLDER IF NOT EXISTS RetailDB.Claims.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Claims.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Claims.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Claims.Bronze.RawClaims (
    ClaimID INT,
    SubmitDate DATE,
    ProcessDate DATE,
    BilledAmount DOUBLE,
    PaidAmount DOUBLE,
    ClaimStatus VARCHAR,
    DenialReason VARCHAR -- Nullable
);

INSERT INTO RetailDB.Claims.Bronze.RawClaims VALUES
(1, '2025-01-01', '2025-01-05', 500.00, 450.00, 'Paid', NULL),
(2, '2025-01-01', '2025-01-02', 1200.00, 0.00, 'Denied', 'Coding Error'),
(3, '2025-01-02', '2025-01-10', 300.00, 280.00, 'Paid', NULL),
(4, '2025-01-02', '2025-01-04', 15000.00, 0.00, 'Denied', 'Prior Auth Missing'),
(5, '2025-01-03', '2025-01-08', 75.00, 75.00, 'Paid', NULL),
(6, '2025-01-03', '2025-01-09', 200.00, 150.00, 'Paid', NULL),
(7, '2025-01-04', '2025-01-06', 4000.00, 0.00, 'Denied', 'Coding Error'),
(8, '2025-01-05', '2025-01-15', 120.00, 100.00, 'Paid', NULL),
(9, '2025-01-06', '2025-01-08', 600.00, 600.00, 'Paid', NULL),
(10, '2025-01-06', '2025-01-12', 45.00, 0.00, 'Denied', 'Duplicate Claim'),
(11, '2025-01-07', '2025-01-20', 1000.00, 900.00, 'Paid', NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Claims.Silver.ClaimMetrics AS
SELECT 
    ClaimID,
    SubmitDate,
    ProcessDate,
    DATE_DIFF(CAST(ProcessDate AS TIMESTAMP), CAST(SubmitDate AS TIMESTAMP)) AS DaysToProcess,
    BilledAmount,
    PaidAmount,
    ClaimStatus,
    COALESCE(DenialReason, 'None') AS ReasonCode
FROM RetailDB.Claims.Bronze.RawClaims;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Claims.Gold.PayerPerformance AS
SELECT 
    ClaimStatus,
    ReasonCode,
    COUNT(*) AS ClaimCount,
    AVG(DaysToProcess) AS AvgProcessingTime,
    SUM(BilledAmount) AS TotalBilled
FROM RetailDB.Claims.Silver.ClaimMetrics
GROUP BY ClaimStatus, ReasonCode;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the most common denial reason in RetailDB.Claims.Gold.PayerPerformance?"
*/
