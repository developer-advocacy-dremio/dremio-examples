/*
 * Telecommunications Customer Churn Demo
 * 
 * Scenario:
 * A telecom operator predicts churn based on call drops and declining usage.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Reduce churn and increase ARPU (Average Revenue Per User).
 * 
 * Note: Assumes a catalog named 'TelcoDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS TelcoDB.Bronze;
CREATE FOLDER IF NOT EXISTS TelcoDB.Silver;
CREATE FOLDER IF NOT EXISTS TelcoDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TelcoDB.Bronze.Subscribers (
    SubscriberID INT,
    PlanType VARCHAR, -- 'Basic', 'Premium', 'Unlimited'
    ContractStartDate DATE,
    MonthlyFee DOUBLE,
    Status VARCHAR -- 'Active', 'Churned'
);

CREATE TABLE IF NOT EXISTS TelcoDB.Bronze.CallDetailRecords (
    CallID INT,
    SubscriberID INT,
    CallDate DATE,
    DurationMinutes INT,
    DroppedCall BOOLEAN
);

CREATE TABLE IF NOT EXISTS TelcoDB.Bronze.DataUsage (
    UsageID INT,
    SubscriberID INT,
    UsageDate DATE,
    MBConsumed DOUBLE
);

-- 1.2 Populate Bronze Tables
INSERT INTO TelcoDB.Bronze.Subscribers (SubscriberID, PlanType, ContractStartDate, MonthlyFee, Status) VALUES
(1, 'Premium', '2023-01-15', 80.00, 'Active'),
(2, 'Basic', '2022-05-20', 40.00, 'Active'),
(3, 'Unlimited', '2021-08-10', 120.00, 'Churned'),
(4, 'Basic', '2024-02-01', 45.00, 'Active'),
(5, 'Premium', '2023-11-25', 85.00, 'Active');

INSERT INTO TelcoDB.Bronze.CallDetailRecords (CallID, SubscriberID, CallDate, DurationMinutes, DroppedCall) VALUES
(1, 1, '2025-05-01', 15, FALSE),
(2, 1, '2025-05-02', 3, TRUE), -- Dropped
(3, 2, '2025-05-01', 60, FALSE),
(4, 3, '2025-04-10', 2, TRUE),
(5, 3, '2025-04-11', 1, TRUE), -- High dropped calls
(6, 4, '2025-05-03', 45, FALSE),
(7, 5, '2025-05-01', 12, FALSE),
(8, 1, '2025-05-05', 5, TRUE);

INSERT INTO TelcoDB.Bronze.DataUsage (UsageID, SubscriberID, UsageDate, MBConsumed) VALUES
(1, 1, '2025-05-01', 500.5),
(2, 2, '2025-05-01', 200.0),
(3, 3, '2025-04-10', 50.0), -- Low usage
(4, 4, '2025-05-01', 1500.0),
(5, 5, '2025-05-01', 2500.0);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Usage Aggregation
-------------------------------------------------------------------------------

-- 2.1 View: Subscriber_Activity_Monthly
-- Aggregates calls and data usage per subscriber.
CREATE OR REPLACE VIEW TelcoDB.Silver.Subscriber_Activity_Monthly AS
SELECT
    s.SubscriberID,
    s.Status,
    s.MonthlyFee,
    COUNT(c.CallID) AS TotalCalls,
    SUM(CASE WHEN c.DroppedCall = TRUE THEN 1 ELSE 0 END) AS DroppedCalls,
    COALESCE(SUM(d.MBConsumed), 0) AS TotalDataMB
FROM TelcoDB.Bronze.Subscribers s
LEFT JOIN TelcoDB.Bronze.CallDetailRecords c ON s.SubscriberID = c.SubscriberID
LEFT JOIN TelcoDB.Bronze.DataUsage d ON s.SubscriberID = d.SubscriberID
GROUP BY s.SubscriberID, s.Status, s.MonthlyFee;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Churn Prediction
-------------------------------------------------------------------------------

-- 3.1 View: Churn_Risk_Analysis
-- Identifies high-risk customers: > 2 dropped calls OR < 100MB data.
CREATE OR REPLACE VIEW TelcoDB.Gold.Churn_Risk_Analysis AS
SELECT
    SubscriberID,
    MonthlyFee,
    DroppedCalls,
    TotalDataMB,
    CASE 
        WHEN DroppedCalls > 1 OR TotalDataMB < 100 THEN 'HIGH'
        ELSE 'LOW'
    END AS ChurnRisk
FROM TelcoDB.Silver.Subscriber_Activity_Monthly
WHERE Status = 'Active';

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Churn Watch):
"Who are my high-risk customers? List SubscriberIDs from TelcoDB.Gold.Churn_Risk_Analysis where ChurnRisk is 'HIGH'."

PROMPT 2 (Revenue Impact):
"What is the total MonthlyFee revenue at risk from high-churn customers?"
*/
