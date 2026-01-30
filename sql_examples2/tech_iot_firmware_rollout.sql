/*
 * Technology: IoT Firmware Rollout
 * 
 * Scenario:
 * Monitoring the success/failure rates of Over-The-Air (OTA) updates across millions of connected devices.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ConnectedDeviceDB;
CREATE FOLDER IF NOT EXISTS ConnectedDeviceDB.Engineering;
CREATE FOLDER IF NOT EXISTS ConnectedDeviceDB.Engineering.Bronze;
CREATE FOLDER IF NOT EXISTS ConnectedDeviceDB.Engineering.Silver;
CREATE FOLDER IF NOT EXISTS ConnectedDeviceDB.Engineering.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Device Logs
-------------------------------------------------------------------------------

-- DeviceRegistry Table
CREATE TABLE IF NOT EXISTS ConnectedDeviceDB.Engineering.Bronze.DeviceRegistry (
    DeviceID VARCHAR,
    Model VARCHAR, -- SmartThermostat-V1, SmartCam-V2
    Region VARCHAR,
    HardwareRevision VARCHAR
);

INSERT INTO ConnectedDeviceDB.Engineering.Bronze.DeviceRegistry VALUES
('D-001', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-002', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-003', 'SmartThermostat-V1', 'EU', 'Rev1.1'),
('D-004', 'SmartThermostat-V2', 'NA', 'Rev2.0'),
('D-005', 'SmartCam-V1', 'NA', 'Rev1.0'),
('D-006', 'SmartCam-V2', 'EU', 'Rev2.0'),
('D-007', 'SmartLock-V1', 'NA', 'Rev1.0'),
('D-008', 'SmartHub-V1', 'APAC', 'Rev1.0'),
('D-009', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-010', 'SmartThermostat-V1', 'EU', 'Rev1.0'),
('D-011', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-012', 'SmartThermostat-V2', 'NA', 'Rev2.0'),
('D-013', 'SmartThermostat-V2', 'EU', 'Rev2.0'),
('D-014', 'SmartCam-V2', 'NA', 'Rev2.0'),
('D-015', 'SmartCam-V2', 'APAC', 'Rev2.0'),
('D-016', 'SmartLock-V1', 'NA', 'Rev1.1'),
('D-017', 'SmartLock-V1', 'EU', 'Rev1.1'),
('D-018', 'SmartHub-V1', 'NA', 'Rev1.0'),
('D-019', 'SmartHub-V2', 'EU', 'Rev2.0'),
('D-020', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-021', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-022', 'SmartThermostat-V1', 'EU', 'Rev1.0'),
('D-023', 'SmartThermostat-V1', 'APAC', 'Rev1.0'),
('D-024', 'SmartThermostat-V2', 'NA', 'Rev2.0'),
('D-025', 'SmartThermostat-V2', 'EU', 'Rev2.0'),
('D-026', 'SmartCam-V1', 'NA', 'Rev1.0'),
('D-027', 'SmartCam-V2', 'EU', 'Rev2.0'),
('D-028', 'SmartLock-V1', 'NA', 'Rev1.0'),
('D-029', 'SmartHub-V1', 'APAC', 'Rev1.0'),
('D-030', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-031', 'SmartThermostat-V1', 'EU', 'Rev1.0'),
('D-032', 'SmartThermostat-V2', 'NA', 'Rev2.0'),
('D-033', 'SmartThermostat-V2', 'EU', 'Rev2.0'),
('D-034', 'SmartCam-V2', 'NA', 'Rev2.0'),
('D-035', 'SmartCam-V2', 'APAC', 'Rev2.0'),
('D-036', 'SmartLock-V1', 'NA', 'Rev1.1'),
('D-037', 'SmartLock-V1', 'EU', 'Rev1.1'),
('D-038', 'SmartHub-V1', 'NA', 'Rev1.0'),
('D-039', 'SmartHub-V2', 'EU', 'Rev2.0'),
('D-040', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-041', 'SmartThermostat-V1', 'EU', 'Rev1.0'),
('D-042', 'SmartThermostat-V2', 'NA', 'Rev2.0'),
('D-043', 'SmartCam-V1', 'NA', 'Rev1.0'),
('D-044', 'SmartCam-V2', 'EU', 'Rev2.0'),
('D-045', 'SmartLock-V1', 'NA', 'Rev1.0'),
('D-046', 'SmartHub-V1', 'APAC', 'Rev1.0'),
('D-047', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-048', 'SmartThermostat-V1', 'EU', 'Rev1.0'),
('D-049', 'SmartThermostat-V1', 'NA', 'Rev1.0'),
('D-050', 'SmartThermostat-V2', 'NA', 'Rev2.0');

-- UpdateLogs Table
CREATE TABLE IF NOT EXISTS ConnectedDeviceDB.Engineering.Bronze.UpdateLogs (
    LogID INT,
    DeviceID VARCHAR,
    TargetFirmware VARCHAR, -- v2.0.1, v2.0.2
    Status VARCHAR, -- Success, Failed, Rollback, Bricked
    EventTimestamp TIMESTAMP
);

INSERT INTO ConnectedDeviceDB.Engineering.Bronze.UpdateLogs VALUES
(1, 'D-001', 'v2.0.1', 'Success', '2025-06-01 10:00:00'),
(2, 'D-002', 'v2.0.1', 'Success', '2025-06-01 10:05:00'),
(3, 'D-003', 'v2.0.1', 'Failed', '2025-06-01 10:10:00'), -- Retry needed
(4, 'D-004', 'v2.0.1', 'Success', '2025-06-01 10:15:00'),
(5, 'D-005', 'v2.0.1', 'Success', '2025-06-01 10:20:00'),
(6, 'D-006', 'v3.0.0', 'Failed', '2025-06-01 10:25:00'),
(7, 'D-007', 'v1.5.0', 'Success', '2025-06-01 10:30:00'),
(8, 'D-008', 'v4.0.0', 'Bricked', '2025-06-01 10:35:00'), -- Critical!
(9, 'D-009', 'v2.0.1', 'Success', '2025-06-01 10:40:00'),
(10, 'D-010', 'v2.0.1', 'Success', '2025-06-01 10:45:00'),
(11, 'D-011', 'v2.0.1', 'Rollback', '2025-06-01 10:50:00'), -- Auto rollback
(12, 'D-012', 'v2.0.1', 'Success', '2025-06-01 10:55:00'),
(13, 'D-013', 'v2.0.1', 'Success', '2025-06-01 11:00:00'),
(14, 'D-014', 'v3.0.0', 'Success', '2025-06-01 11:05:00'),
(15, 'D-015', 'v3.0.0', 'Failed', '2025-06-01 11:10:00'),
(16, 'D-016', 'v1.5.0', 'Success', '2025-06-01 11:15:00'),
(17, 'D-017', 'v1.5.0', 'Success', '2025-06-01 11:20:00'),
(18, 'D-018', 'v4.0.0', 'Success', '2025-06-01 11:25:00'),
(19, 'D-019', 'v4.1.0', 'Rollback', '2025-06-01 11:30:00'),
(20, 'D-020', 'v2.0.1', 'Success', '2025-06-01 11:35:00'),
(21, 'D-021', 'v2.0.1', 'Success', '2025-06-01 11:40:00'),
(22, 'D-022', 'v2.0.1', 'Bricked', '2025-06-01 11:45:00'), -- Critical
(23, 'D-023', 'v2.0.1', 'Success', '2025-06-01 11:50:00'),
(24, 'D-024', 'v2.0.1', 'Success', '2025-06-01 11:55:00'),
(25, 'D-025', 'v2.0.1', 'Success', '2025-06-01 12:00:00'),
(26, 'D-026', 'v3.0.0', 'Success', '2025-06-01 12:05:00'),
(27, 'D-027', 'v3.0.0', 'Success', '2025-06-01 12:10:00'),
(28, 'D-028', 'v1.5.0', 'Success', '2025-06-01 12:15:00'),
(29, 'D-029', 'v4.0.0', 'Failed', '2025-06-01 12:20:00'),
(30, 'D-030', 'v2.0.1', 'Success', '2025-06-01 12:25:00'),
(31, 'D-031', 'v2.0.1', 'Success', '2025-06-01 12:30:00'),
(32, 'D-032', 'v2.0.1', 'Success', '2025-06-01 12:35:00'),
(33, 'D-033', 'v2.0.1', 'Success', '2025-06-01 12:40:00'),
(34, 'D-034', 'v3.0.0', 'Success', '2025-06-01 12:45:00'),
(35, 'D-035', 'v3.0.0', 'Success', '2025-06-01 12:50:00'),
(36, 'D-036', 'v1.5.0', 'Success', '2025-06-01 12:55:00'),
(37, 'D-037', 'v1.5.0', 'Failed', '2025-06-01 13:00:00'),
(38, 'D-038', 'v4.0.0', 'Success', '2025-06-01 13:05:00'),
(39, 'D-039', 'v4.1.0', 'Success', '2025-06-01 13:10:00'),
(40, 'D-040', 'v2.0.1', 'Success', '2025-06-01 13:15:00'),
(41, 'D-041', 'v2.0.1', 'Rollback', '2025-06-01 13:20:00'),
(42, 'D-042', 'v2.0.1', 'Success', '2025-06-01 13:25:00'),
(43, 'D-043', 'v3.0.0', 'Success', '2025-06-01 13:30:00'),
(44, 'D-044', 'v3.0.0', 'Success', '2025-06-01 13:35:00'),
(45, 'D-045', 'v1.5.0', 'Success', '2025-06-01 13:40:00'),
(46, 'D-046', 'v4.0.0', 'Success', '2025-06-01 13:45:00'),
(47, 'D-047', 'v2.0.1', 'Success', '2025-06-01 13:50:00'),
(48, 'D-048', 'v2.0.1', 'Success', '2025-06-01 13:55:00'),
(49, 'D-049', 'v2.0.1', 'Failed', '2025-06-01 14:00:00'),
(50, 'D-050', 'v2.0.1', 'Success', '2025-06-01 14:05:00');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Update Analytics
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ConnectedDeviceDB.Engineering.Silver.UpdateAnalytics AS
SELECT 
    d.Model,
    u.TargetFirmware,
    u.Status,
    COUNT(u.LogID) AS UpdateCount
FROM ConnectedDeviceDB.Engineering.Bronze.UpdateLogs u
JOIN ConnectedDeviceDB.Engineering.Bronze.DeviceRegistry d ON u.DeviceID = d.DeviceID
GROUP BY d.Model, u.TargetFirmware, u.Status;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Critical Failures
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW ConnectedDeviceDB.Engineering.Gold.CriticalAlerts AS
SELECT 
    Model,
    TargetFirmware,
    SUM(UpdateCount) AS TotalFailures,
    'Immediate Investigation' AS ActionRequired
FROM ConnectedDeviceDB.Engineering.Silver.UpdateAnalytics
WHERE Status IN ('Bricked', 'Failed')
GROUP BY Model, TargetFirmware;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Show the breakdown of update statuses (Success, Failed, etc.) for 'SmartThermostat-V1' from the Silver layer."

PROMPT 2:
"List any firmware versions that resulted in a 'Bricked' status in the ConnectedDeviceDB.Engineering.Gold.CriticalAlerts view."

PROMPT 3:
"Calculate the overall Success Rate percentage for firmware 'v2.0.1'."
*/
