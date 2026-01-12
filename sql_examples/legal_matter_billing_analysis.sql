/*
 * Legal Matter Billing Analysis Demo
 * 
 * Scenario:
 * Analyzing billable hours, realization rates, and matter budgeting.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.Legal;
CREATE FOLDER IF NOT EXISTS RetailDB.Legal.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Legal.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Legal.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.Legal.Bronze.TimeEntries (
    EntryID INT,
    MatterID INT,
    AttorneyID INT,
    HoursWorked DOUBLE,
    HourlyRate DOUBLE,
    EntryDate DATE,
    Status VARCHAR -- Billed, Unbilled, WriteOff
);

INSERT INTO RetailDB.Legal.Bronze.TimeEntries VALUES
(1, 1001, 501, 2.5, 450.00, '2025-01-01', 'Billed'),
(2, 1001, 502, 1.0, 300.00, '2025-01-01', 'Billed'),
(3, 1002, 501, 5.0, 450.00, '2025-01-02', 'Billed'),
(4, 1002, 503, 3.0, 350.00, '2025-01-02', 'WriteOff'), -- Non-billable work
(5, 1001, 501, 1.5, 450.00, '2025-01-03', 'Unbilled'),
(6, 1003, 502, 4.0, 300.00, '2025-01-03', 'Billed'),
(7, 1003, 503, 2.0, 350.00, '2025-01-04', 'Billed'),
(8, 1001, 501, 0.5, 450.00, '2025-01-05', 'Billed'),
(9, 1002, 502, 6.0, 300.00, '2025-01-05', 'Billed'),
(10, 1004, 501, 10.0, 450.00, '2025-01-06', 'Billed'),
(11, 1004, 503, 8.0, 350.00, '2025-01-06', 'Unbilled');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Legal.Silver.BillingMetrics AS
SELECT 
    MatterID,
    AttorneyID,
    HoursWorked,
    HourlyRate,
    (HoursWorked * HourlyRate) AS PotentialRevenue,
    CASE 
        WHEN Status = 'Billed' THEN (HoursWorked * HourlyRate)
        ELSE 0 
    END AS ActualRevenue,
    Status
FROM RetailDB.Legal.Bronze.TimeEntries;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.Legal.Gold.MatterProfitability AS
SELECT 
    MatterID,
    SUM(HoursWorked) AS TotalHours,
    SUM(PotentialRevenue) AS TotalValue,
    SUM(ActualRevenue) AS BilledValue,
    (SUM(ActualRevenue) / NULLIF(SUM(PotentialRevenue), 0)) * 100 AS RealizationRate
FROM RetailDB.Legal.Silver.BillingMetrics
GROUP BY MatterID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which MatterID has the lowest RealizationRate in RetailDB.Legal.Gold.MatterProfitability?"
*/
