/*
 * POS Terminal Lifecycle Management Demo
 * 
 * Scenario:
 * A Merchant Acquirer manages thousands of Point-of-Sale (POS) terminals.
 * They need to track depreciation and predict when units will need replacement 
 * based on error frequency.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Forecast hardware replacement costs.
 * 
 * Note: Assumes a catalog named 'MerchantOpsDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MerchantOpsDB;
CREATE FOLDER IF NOT EXISTS MerchantOpsDB.Bronze;
CREATE FOLDER IF NOT EXISTS MerchantOpsDB.Silver;
CREATE FOLDER IF NOT EXISTS MerchantOpsDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Asset Management
-------------------------------------------------------------------------------
-- Description: Inventory list and field service logs.

CREATE TABLE IF NOT EXISTS MerchantOpsDB.Bronze.TerminalAssets (
    TerminalID VARCHAR,
    Model VARCHAR, -- 'Verifone_X', 'Ingenico_Y'
    InstallDate DATE,
    MerchantID VARCHAR
);

CREATE TABLE IF NOT EXISTS MerchantOpsDB.Bronze.MaintenanceLogs (
    LogID INT,
    TerminalID VARCHAR,
    EventDate DATE,
    EventType VARCHAR, -- 'Repair', 'SoftwareUpdate', 'Error'
    ErrorCode VARCHAR
);

-- 1.1 Populate TerminalAssets (50+ Records)
INSERT INTO MerchantOpsDB.Bronze.TerminalAssets (TerminalID, Model, InstallDate, MerchantID) VALUES
('T-001', 'Verifone_X', '2020-01-01', 'M-100'),
('T-002', 'Ingenico_Y', '2021-05-15', 'M-101'),
('T-003', 'Verifone_X', '2019-11-20', 'M-102'),
('T-004', 'Ingenico_Y', '2022-03-10', 'M-103'),
('T-005', 'Verifone_X', '2020-08-01', 'M-104'),
('T-006', 'Verifone_X', '2018-06-01', 'M-105'), -- Old
('T-007', 'Ingenico_Y', '2023-01-01', 'M-106'),
('T-008', 'Verifone_X', '2020-02-14', 'M-107'),
('T-009', 'Ingenico_Y', '2021-09-09', 'M-108'),
('T-010', 'Verifone_X', '2019-04-04', 'M-109'),
('T-011', 'Verifone_X', '2020-12-12', 'M-110'),
('T-012', 'Ingenico_Y', '2022-07-07', 'M-111'),
('T-013', 'Verifone_X', '2021-11-11', 'M-112'),
('T-014', 'Ingenico_Y', '2023-02-28', 'M-113'),
('T-015', 'Verifone_X', '2019-01-15', 'M-114'),
('T-016', 'Verifone_X', '2020-05-05', 'M-115'),
('T-017', 'Ingenico_Y', '2021-08-08', 'M-116'),
('T-018', 'Verifone_X', '2022-04-01', 'M-117'),
('T-019', 'Ingenico_Y', '2023-03-15', 'M-118'),
('T-020', 'Verifone_X', '2018-10-31', 'M-119'), -- Old
('T-021', 'Verifone_X', '2020-09-20', 'M-120'),
('T-022', 'Ingenico_Y', '2021-06-30', 'M-121'),
('T-023', 'Verifone_X', '2022-11-25', 'M-122'),
('T-024', 'Ingenico_Y', '2023-04-10', 'M-123'),
('T-025', 'Verifone_X', '2019-07-17', 'M-124'),
('T-026', 'Verifone_X', '2020-03-03', 'M-125'),
('T-027', 'Ingenico_Y', '2021-12-01', 'M-126'),
('T-028', 'Verifone_X', '2022-09-09', 'M-127'),
('T-029', 'Ingenico_Y', '2023-05-05', 'M-128'),
('T-030', 'Verifone_X', '2018-05-20', 'M-129'), -- Old
('T-031', 'Verifone_X', '2020-10-10', 'M-130'),
('T-032', 'Ingenico_Y', '2021-03-15', 'M-131'),
('T-033', 'Verifone_X', '2022-01-20', 'M-132'),
('T-034', 'Ingenico_Y', '2023-06-01', 'M-133'),
('T-035', 'Verifone_X', '2019-08-08', 'M-134'),
('T-036', 'Verifone_X', '2020-06-15', 'M-135'),
('T-037', 'Ingenico_Y', '2021-02-28', 'M-136'),
('T-038', 'Verifone_X', '2022-05-10', 'M-137'),
('T-039', 'Ingenico_Y', '2023-07-04', 'M-138'),
('T-040', 'Verifone_X', '2018-12-25', 'M-139'), -- Old
('T-041', 'Verifone_X', '2020-11-01', 'M-140'),
('T-042', 'Ingenico_Y', '2021-04-20', 'M-141'),
('T-043', 'Verifone_X', '2022-08-15', 'M-142'),
('T-044', 'Ingenico_Y', '2023-08-08', 'M-143'),
('T-045', 'Verifone_X', '2019-02-14', 'M-144'),
('T-046', 'Verifone_X', '2020-07-07', 'M-145'),
('T-047', 'Ingenico_Y', '2021-10-31', 'M-146'),
('T-048', 'Verifone_X', '2022-06-01', 'M-147'),
('T-049', 'Ingenico_Y', '2023-09-01', 'M-148'),
('T-050', 'Verifone_X', '2018-03-17', 'M-149'); -- Old

-- 1.2 Populate MaintenanceLogs (Samples)
INSERT INTO MerchantOpsDB.Bronze.MaintenanceLogs (LogID, TerminalID, EventDate, EventType, ErrorCode) VALUES
(1, 'T-001', '2024-01-01', 'Error', 'E-500'),
(2, 'T-001', '2024-02-01', 'Error', 'E-500'), -- Frequent error
(3, 'T-002', '2024-01-15', 'SoftwareUpdate', NULL),
(4, 'T-006', '2024-03-01', 'Repair', 'PrinterJam'),
(5, 'T-006', '2024-04-01', 'Error', 'HardwareFail'), -- Dying
(6, 'T-020', '2024-01-10', 'Error', 'Battery'),
(7, 'T-030', '2024-01-20', 'Repair', 'ScreenCrack'),
(8, 'T-030', '2024-02-20', 'Error', 'TouchFail'),
(9, 'T-040', '2024-01-05', 'Error', 'Connectivity'),
(10, 'T-050', '2024-01-25', 'Error', 'HardwareFail');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Health Scoring
-------------------------------------------------------------------------------
-- Description: Combining Age and Incident History.
-- Logic: If Age > 5 years OR > 3 errors in last year, flag "Poor".

CREATE OR REPLACE VIEW MerchantOpsDB.Silver.TerminalHealthHistory AS
SELECT
    t.TerminalID,
    t.Model,
    t.InstallDate,
    TIMESTAMPDIFF(YEAR, t.InstallDate, DATE '2025-01-01') AS AgeYears,
    COUNT(m.LogID) AS IncidentCount
FROM MerchantOpsDB.Bronze.TerminalAssets t
LEFT JOIN MerchantOpsDB.Bronze.MaintenanceLogs m ON t.TerminalID = m.TerminalID
GROUP BY t.TerminalID, t.Model, t.InstallDate;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Replacement Planning
-------------------------------------------------------------------------------
-- Description: List of terminals needing replacement budget.
-- Logic: Age > 5.

CREATE OR REPLACE VIEW MerchantOpsDB.Gold.ReplacementForecast AS
SELECT
    TerminalID,
    MerchantID,
    Model,
    AgeYears,
    IncidentCount,
    'Replace' AS Recommendation
FROM MerchantOpsDB.Silver.TerminalHealthHistory
WHERE AgeYears >= 5 OR IncidentCount >= 2;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Count how many terminals need replacement grouped by Model from MerchantOpsDB.Gold.ReplacementForecast."

PROMPT:
"List all terminals managed by MerchantID 'M-105' that are flagged for replacement."
*/
