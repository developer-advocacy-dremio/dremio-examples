/*
 * Mental Health Services Demo
 * 
 * Scenario:
 * Tracking appointment availability and provider utilization for mental health services.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS RetailDB.MentalHealth;
CREATE FOLDER IF NOT EXISTS RetailDB.MentalHealth.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.MentalHealth.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.MentalHealth.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.MentalHealth.Bronze.Appointments (
    ApptID INT,
    ProviderID INT,
    PatientID INT,
    ApptType VARCHAR, -- Intake, Therapy, Med Management
    ApptDate DATE,
    ShowStatus VARCHAR -- Show, No-Show, Cancelled
);

INSERT INTO RetailDB.MentalHealth.Bronze.Appointments VALUES
(1, 10, 100, 'Intake', '2025-01-01', 'Show'),
(2, 10, 101, 'Therapy', '2025-01-01', 'Show'),
(3, 11, 102, 'Med Management', '2025-01-01', 'Show'),
(4, 11, 103, 'Therapy', '2025-01-02', 'No-Show'),
(5, 12, 104, 'Intake', '2025-01-02', 'Show'),
(6, 12, 105, 'Therapy', '2025-01-02', 'Cancelled'),
(7, 10, 100, 'Therapy', '2025-01-08', 'Show'),
(8, 11, 102, 'Med Management', '2025-02-01', 'Show'),
(9, 12, 104, 'Therapy', '2025-01-09', 'Show'),
(10, 10, 106, 'Intake', '2025-01-03', 'Show');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.MentalHealth.Silver.Utilization AS
SELECT 
    ProviderID,
    ApptType,
    CASE 
        WHEN ShowStatus = 'Show' THEN 1
        ELSE 0 
    END AS Billable,
    ShowStatus
FROM RetailDB.MentalHealth.Bronze.Appointments;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW RetailDB.MentalHealth.Gold.ProviderEfficiency AS
SELECT 
    ProviderID,
    COUNT(*) AS TotalSlots,
    SUM(Billable) AS BillableSlots,
    (CAST(SUM(Billable) AS DOUBLE) / COUNT(*)) * 100 AS UtilizationRate
FROM RetailDB.MentalHealth.Silver.Utilization
GROUP BY ProviderID;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which provider has the lowest UtilizationRate in RetailDB.MentalHealth.Gold.ProviderEfficiency?"
*/
