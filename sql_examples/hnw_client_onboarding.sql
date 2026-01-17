/*
 * High Net Worth (HNW) Client Onboarding Demo
 * 
 * Scenario:
 * Private banks have complex onboarding processes involves Source of Wealth verification, 
 * PEP (Politically Exposed Person) screening, and multiple approvals.
 * Delays risk losing high-value clients.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Identify bottlenecks in the onboarding pipeline.
 * 
 * Note: Assumes a catalog named 'PrivateBankDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS PrivateBankDB;
CREATE FOLDER IF NOT EXISTS PrivateBankDB.Bronze;
CREATE FOLDER IF NOT EXISTS PrivateBankDB.Silver;
CREATE FOLDER IF NOT EXISTS PrivateBankDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Pipeline Data
-------------------------------------------------------------------------------
-- Description: Prospect details and log of onboarding milestones.

CREATE TABLE IF NOT EXISTS PrivateBankDB.Bronze.Prospects (
    ClientID VARCHAR,
    Name VARCHAR,
    EstNetWorth DOUBLE,
    Region VARCHAR,
    PipelineEntryDate DATE
);

CREATE TABLE IF NOT EXISTS PrivateBankDB.Bronze.OnboardingSteps (
    LogID INT,
    ClientID VARCHAR,
    StepName VARCHAR, -- 'AMLCheck', 'SourceOfWealth', 'LegalReview', 'AccountOpen'
    Status VARCHAR, -- 'Completed', 'Pending'
    CompletionDate TIMESTAMP -- Null if pending
);

-- 1.1 Populate Prospects (50 Students)
INSERT INTO PrivateBankDB.Bronze.Prospects (ClientID, Name, EstNetWorth, Region, PipelineEntryDate) VALUES
('VIP-001', 'Client A', 15000000, 'NA', '2025-01-01'),
('VIP-002', 'Client B', 50000000, 'EU', '2025-01-01'),
('VIP-003', 'Client C', 25000000, 'APAC', '2025-01-02'),
('VIP-004', 'Client D', 10000000, 'NA', '2025-01-02'),
('VIP-005', 'Client E', 80000000, 'EU', '2025-01-03'),
('VIP-006', 'Client F', 12000000, 'NA', '2025-01-03'),
('VIP-007', 'Client G', 30000000, 'APAC', '2025-01-04'),
('VIP-008', 'Client H', 18000000, 'NA', '2025-01-04'),
('VIP-009', 'Client I', 55000000, 'EU', '2025-01-05'),
('VIP-010', 'Client J', 22000000, 'LATAM', '2025-01-05'),
('VIP-011', 'Client K', 14000000, 'NA', '2025-01-06'),
('VIP-012', 'Client L', 40000000, 'EU', '2025-01-06'),
('VIP-013', 'Client M', 16000000, 'APAC', '2025-01-07'),
('VIP-014', 'Client N', 90000000, 'NA', '2025-01-07'),
('VIP-015', 'Client O', 11000000, 'NA', '2025-01-08'),
('VIP-016', 'Client P', 35000000, 'EU', '2025-01-08'),
('VIP-017', 'Client Q', 28000000, 'APAC', '2025-01-09'),
('VIP-018', 'Client R', 19000000, 'NA', '2025-01-09'),
('VIP-019', 'Client S', 60000000, 'LATAM', '2025-01-10'),
('VIP-020', 'Client T', 20000000, 'NA', '2025-01-10'),
('VIP-021', 'Client U', 13000000, 'EU', '2025-01-11'),
('VIP-022', 'Client V', 45000000, 'NA', '2025-01-11'),
('VIP-023', 'Client W', 17000000, 'APAC', '2025-01-12'),
('VIP-024', 'Client X', 75000000, 'NA', '2025-01-12'),
('VIP-025', 'Client Y', 24000000, 'EU', '2025-01-13'),
('VIP-026', 'Client Z', 32000000, 'NA', '2025-01-13'),
('VIP-027', 'Client AA', 15000000, 'APAC', '2025-01-14'),
('VIP-028', 'Client AB', 52000000, 'NA', '2025-01-14'),
('VIP-029', 'Client AC', 21000000, 'EU', '2025-01-15'),
('VIP-030', 'Client AD', 10500000, 'LATAM', '2025-01-15'),
('VIP-031', 'Client AE', 38000000, 'NA', '2025-01-16'),
('VIP-032', 'Client AF', 26000000, 'EU', '2025-01-16'),
('VIP-033', 'Client AG', 14500000, 'APAC', '2025-01-17'),
('VIP-034', 'Client AH', 85000000, 'NA', '2025-01-17'),
('VIP-035', 'Client AI', 19500000, 'NA', '2025-01-18'),
('VIP-036', 'Client AJ', 42000000, 'EU', '2025-01-18'),
('VIP-037', 'Client AK', 29000000, 'APAC', '2025-01-19'),
('VIP-038', 'Client AL', 16500000, 'NA', '2025-01-19'),
('VIP-039', 'Client AM', 65000000, 'LATAM', '2025-01-20'),
('VIP-040', 'Client AN', 23000000, 'NA', '2025-01-20'),
('VIP-041', 'Client AO', 13500000, 'EU', '2025-01-21'),
('VIP-042', 'Client AP', 48000000, 'NA', '2025-01-21'),
('VIP-043', 'Client AQ', 17500000, 'APAC', '2025-01-22'),
('VIP-044', 'Client AR', 72000000, 'NA', '2025-01-22'),
('VIP-045', 'Client AS', 27000000, 'EU', '2025-01-23'),
('VIP-046', 'Client AT', 33000000, 'NA', '2025-01-23'),
('VIP-047', 'Client AU', 15500000, 'APAC', '2025-01-24'),
('VIP-048', 'Client AV', 58000000, 'NA', '2025-01-24'),
('VIP-049', 'Client AW', 20500000, 'EU', '2025-01-25'),
('VIP-050', 'Client AX', 11500000, 'LATAM', '2025-01-25');

-- 1.2 Populate OnboardingSteps (Samples)
INSERT INTO PrivateBankDB.Bronze.OnboardingSteps (LogID, ClientID, StepName, Status, CompletionDate) VALUES
(1, 'VIP-001', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-02 10:00:00'),
(2, 'VIP-001', 'SourceOfWealth', 'Completed', TIMESTAMP '2025-01-05 14:00:00'),
(3, 'VIP-002', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-03 11:00:00'),
(4, 'VIP-002', 'SourceOfWealth', 'Pending', NULL), -- Bottleneck
(5, 'VIP-003', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-04 09:00:00'),
(6, 'VIP-003', 'SourceOfWealth', 'Completed', TIMESTAMP '2025-01-10 16:00:00'), -- Long duration
(7, 'VIP-004', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-03 10:30:00'),
(8, 'VIP-004', 'SourceOfWealth', 'Completed', TIMESTAMP '2025-01-04 09:30:00'),
(9, 'VIP-005', 'AMLCheck', 'Pending', NULL),
(10, 'VIP-006', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-04 12:00:00'),
(11, 'VIP-006', 'SourceOfWealth', 'Completed', TIMESTAMP '2025-01-08 14:00:00'),
(12, 'VIP-007', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-05 15:00:00'),
(13, 'VIP-008', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-05 16:00:00'),
(14, 'VIP-009', 'AMLCheck', 'Completed', TIMESTAMP '2025-01-06 10:00:00'),
(15, 'VIP-009', 'SourceOfWealth', 'Pending', NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Duration Analysis
-------------------------------------------------------------------------------
-- Description: Calculating time taken for each completed step.
-- Logic: Join with Prospect EntryDate to find time-to-first-step, or just use raw dates.

CREATE OR REPLACE VIEW PrivateBankDB.Silver.OnboardingTimeline AS
SELECT
    s.ClientID,
    p.Name,
    s.StepName,
    s.CompletionDate,
    p.PipelineEntryDate,
    TIMESTAMPDIFF(DAY, CAST(p.PipelineEntryDate AS DATE), CAST(s.CompletionDate AS DATE)) AS DaysToComplete
FROM PrivateBankDB.Bronze.OnboardingSteps s
JOIN PrivateBankDB.Bronze.Prospects p ON s.ClientID = p.ClientID
WHERE s.Status = 'Completed';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Bottlenecks
-------------------------------------------------------------------------------
-- Description: Identifying steps taking longer than the SLA (e.g., 3 days).
-- Insight: 'SourceOfWealth' typically takes longest.

CREATE OR REPLACE VIEW PrivateBankDB.Gold.BottleneckAnalysis AS
SELECT
    StepName,
    AVG(DaysToComplete) AS AvgDays,
    MAX(DaysToComplete) AS MaxDays,
    COUNT(*) AS CompletedCount
FROM PrivateBankDB.Silver.OnboardingTimeline
GROUP BY StepName;

CREATE OR REPLACE VIEW PrivateBankDB.Gold.SLA_Breaches AS
SELECT
    ClientID,
    Name,
    StepName,
    DaysToComplete,
    'Breach' AS Status
FROM PrivateBankDB.Silver.OnboardingTimeline
WHERE DaysToComplete > 3;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Using PrivateBankDB.Gold.BottleneckAnalysis, which StepName has the highest AvgDays?"

PROMPT:
"List all clients from PrivateBankDB.Gold.SLA_Breaches where the StepName is 'SourceOfWealth'."
*/
