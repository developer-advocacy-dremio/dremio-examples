/*
 * Maritime: Shipping Container Demurrage Analysis
 * 
 * Scenario:
 * Monitoring container dwell times at ports to manage and predict demurrage (storage) fees for shipping lines.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MaritimeDB;
CREATE FOLDER IF NOT EXISTS MaritimeDB.Logistics;
CREATE FOLDER IF NOT EXISTS MaritimeDB.Logistics.Bronze;
CREATE FOLDER IF NOT EXISTS MaritimeDB.Logistics.Silver;
CREATE FOLDER IF NOT EXISTS MaritimeDB.Logistics.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------

-- Containers Table: Container details
CREATE TABLE IF NOT EXISTS MaritimeDB.Logistics.Bronze.Containers (
    ContainerID VARCHAR,
    Type VARCHAR,
    Size VARCHAR,
    ShippingLine VARCHAR,
    CargoType VARCHAR
);

INSERT INTO MaritimeDB.Logistics.Bronze.Containers VALUES
('MSKU1234567', 'Dry', '40ft', 'Maersk', 'Electronics'),
('MSKU2345678', 'Reefer', '40ft', 'Maersk', 'Fruits'),
('MSKU3456789', 'Dry', '20ft', 'Maersk', 'Textiles'),
('MSKU4567890', 'OpenTop', '40ft', 'Maersk', 'Machinery'),
('MSKU5678901', 'Dry', '40ft', 'Maersk', 'Furniture'),
('CMAU1234567', 'Dry', '20ft', 'CMA CGM', 'Auto Parts'),
('CMAU2345678', 'Reefer', '40ft', 'CMA CGM', 'Meat'),
('CMAU3456789', 'Dry', '40ft', 'CMA CGM', 'Toys'),
('CMAU4567890', 'FlatRack', '40ft', 'CMA CGM', 'Heavy Eq'),
('CMAU5678901', 'Dry', '20ft', 'CMA CGM', 'Chemicals'),
('MSCU1234567', 'Dry', '40ft', 'MSC', 'Grain'),
('MSCU2345678', 'Reefer', '20ft', 'MSC', 'Pharma'),
('MSCU3456789', 'Dry', '40ft', 'MSC', 'Paper'),
('MSCU4567890', 'Tank', '20ft', 'MSC', 'Oil'),
('MSCU5678901', 'Dry', '40ft', 'MSC', 'Steel'),
('COSU1234567', 'Dry', '40ft', 'COSCO', 'Plastic'),
('COSU2345678', 'Dry', '20ft', 'COSCO', 'Rubber'),
('COSU3456789', 'Reefer', '40ft', 'COSCO', 'Seafood'),
('COSU4567890', 'Dry', '40ft', 'COSCO', 'Glass'),
('COSU5678901', 'openTop', '20ft', 'COSCO', 'Marble'),
('HLCU1234567', 'Dry', '40ft', 'Hapag-Lloyd', 'Coffee'),
('HLCU2345678', 'Dry', '20ft', 'Hapag-Lloyd', 'Cocoa'),
('HLCU3456789', 'Reefer', '40ft', 'Hapag-Lloyd', 'Flowers'),
('HLCU4567890', 'Dry', '40ft', 'Hapag-Lloyd', 'Tea'),
('HLCU5678901', 'Tank', '20ft', 'Hapag-Lloyd', 'Wine'),
('ONEU1234567', 'Dry', '40ft', 'ONE', 'Apparel'),
('ONEU2345678', 'Dry', '20ft', 'ONE', 'Shoes'),
('ONEU3456789', 'Reefer', '40ft', 'ONE', 'Vegetables'),
('ONEU4567890', 'Dry', '40ft', 'ONE', 'Rice'),
('ONEU5678901', 'FlatRack', '40ft', 'ONE', 'Boats'),
('HMMU1234567', 'Dry', '40ft', 'HMM', 'Tires'),
('HMMU2345678', 'Dry', '20ft', 'HMM', 'Batteries'),
('HMMU3456789', 'Reefer', '20ft', 'HMM', 'Medicine'),
('HMMU4567890', 'Dry', '40ft', 'HMM', 'Solar Panels'),
('HMMU5678901', 'Dry', '40ft', 'HMM', 'Ceramics'),
('ZIMU1234567', 'Dry', '20ft', 'ZIM', 'Fertilizer'),
('ZIMU2345678', 'Dry', '40ft', 'ZIM', 'Wood'),
('ZIMU3456789', 'Reefer', '40ft', 'ZIM', 'Dairy'),
('ZIMU4567890', 'Tank', '20ft', 'ZIM', 'Juice'),
('ZIMU5678901', 'Dry', '40ft', 'ZIM', 'Hardware'),
('OOLU1234567', 'Dry', '40ft', 'OOCL', 'Textiles'),
('OOLU2345678', 'Reefer', '20ft', 'OOCL', 'Fish'),
('OOLU3456789', 'Dry', '40ft', 'OOCL', 'Machinery'),
('OOLU4567890', 'Dry', '20ft', 'OOCL', 'Chemicals'),
('OOLU5678901', 'FlatRack', '40ft', 'OOCL', 'Vehicles'),
('YMLU1234567', 'Dry', '40ft', 'Yang Ming', 'Electronics'),
('YMLU2345678', 'Dry', '20ft', 'Yang Ming', 'Toys'),
('YMLU3456789', 'Reefer', '40ft', 'Yang Ming', 'Fruit'),
('YMLU4567890', 'Dry', '40ft', 'Yang Ming', 'Grain'),
('YMLU5678901', 'OpenTop', '20ft', 'Yang Ming', 'Stone');

-- PortMovements Table: Gate in/out and discharge events
CREATE TABLE IF NOT EXISTS MaritimeDB.Logistics.Bronze.PortMovements (
    MovementID INT,
    ContainerID VARCHAR,
    PortCode VARCHAR,
    MovementType VARCHAR, -- 'Discharge', 'GateOut', 'GateIn', 'Load'
    EventTime TIMESTAMP
);

INSERT INTO MaritimeDB.Logistics.Bronze.PortMovements VALUES
(1, 'MSKU1234567', 'USNYC', 'Discharge', '2025-01-01 08:00:00'),
(2, 'MSKU1234567', 'USNYC', 'GateOut', '2025-01-03 10:00:00'),
(3, 'MSKU2345678', 'USSAV', 'Discharge', '2025-01-01 09:30:00'),
(4, 'MSKU2345678', 'USSAV', 'GateOut', '2025-01-05 14:00:00'),
(5, 'MSKU3456789', 'USLAX', 'Discharge', '2025-01-02 07:00:00'), -- No GateOut yet
(6, 'MSKU4567890', 'USLGB', 'Discharge', '2025-01-02 11:00:00'),
(7, 'MSKU4567890', 'USLGB', 'GateOut', '2025-01-15 08:30:00'), -- Long dwell
(8, 'MSKU5678901', 'USSEA', 'Discharge', '2025-01-03 05:00:00'),
(9, 'CMAU1234567', 'USNYC', 'Discharge', '2025-01-01 12:00:00'),
(10, 'CMAU1234567', 'USNYC', 'GateOut', '2025-01-02 09:00:00'),
(11, 'CMAU2345678', 'USMIA', 'Discharge', '2025-01-01 13:00:00'), -- Reefer check
(12, 'CMAU3456789', 'USOAK', 'Discharge', '2025-01-04 15:00:00'), -- No GateOut
(13, 'CMAU4567890', 'USHOU', 'Discharge', '2025-01-05 08:00:00'),
(14, 'CMAU5678901', 'USBAL', 'Discharge', '2025-01-01 10:00:00'),
(15, 'CMAU5678901', 'USBAL', 'GateOut', '2025-01-08 11:00:00'),
(16, 'MSCU1234567', 'USCHS', 'Discharge', '2025-01-02 06:00:00'),
(17, 'MSCU2345678', 'USORF', 'Discharge', '2025-01-03 09:00:00'),
(18, 'MSCU3456789', 'USNYC', 'Discharge', '2025-01-04 14:00:00'),
(19, 'MSCU4567890', 'USNYC', 'Discharge', '2025-01-05 16:00:00'), -- Tank
(20, 'MSCU5678901', 'USLAX', 'Discharge', '2025-01-01 02:00:00'),
(21, 'MSCU5678901', 'USLAX', 'GateOut', '2025-01-20 10:00:00'), -- Very long dwell
(22, 'COSU1234567', 'USSEA', 'Discharge', '2025-01-06 08:00:00'),
(23, 'COSU2345678', 'USSEA', 'Discharge', '2025-01-06 08:30:00'),
(24, 'COSU3456789', 'USOAK', 'Discharge', '2025-01-07 09:00:00'),
(25, 'COSU4567890', 'USLGB', 'Discharge', '2025-01-08 10:00:00'),
(26, 'COSU5678901', 'USLGB', 'Discharge', '2025-01-08 10:30:00'),
(27, 'HLCU1234567', 'USNYC', 'Discharge', '2025-01-02 11:00:00'),
(28, 'HLCU1234567', 'USNYC', 'GateOut', '2025-01-04 15:00:00'),
(29, 'HLCU2345678', 'USMIA', 'Discharge', '2025-01-03 12:00:00'),
(30, 'HLCU3456789', 'USMIA', 'Discharge', '2025-01-03 12:15:00'),
(31, 'HLCU4567890', 'USHOU', 'Discharge', '2025-01-05 07:00:00'),
(32, 'HLCU5678901', 'USHOU', 'Discharge', '2025-01-05 07:15:00'),
(33, 'ONEU1234567', 'USLAX', 'Discharge', '2025-01-01 04:00:00'),
(34, 'ONEU1234567', 'USLAX', 'GateOut', '2025-01-02 05:00:00'), -- Fast
(35, 'ONEU2345678', 'USLAX', 'Discharge', '2025-01-01 04:30:00'),
(36, 'ONEU3456789', 'USOAK', 'Discharge', '2025-01-09 10:00:00'),
(37, 'ONEU4567890', 'USSEA', 'Discharge', '2025-01-10 11:00:00'),
(38, 'ONEU5678901', 'USSEA', 'Discharge', '2025-01-10 11:15:00'),
(39, 'HMMU1234567', 'USNYC', 'Discharge', '2025-01-06 08:00:00'),
(40, 'HMMU2345678', 'USSAV', 'Discharge', '2025-01-07 09:00:00'),
(41, 'HMMU3456789', 'USSAV', 'Discharge', '2025-01-07 09:30:00'),
(42, 'HMMU4567890', 'USCHS', 'Discharge', '2025-01-08 10:00:00'),
(43, 'HMMU5678901', 'USCHS', 'Discharge', '2025-01-08 10:30:00'),
(44, 'ZIMU1234567', 'USMIA', 'Discharge', '2025-01-04 12:00:00'),
(45, 'ZIMU2345678', 'USMIA', 'Discharge', '2025-01-04 12:30:00'),
(46, 'ZIMU3456789', 'USHOU', 'Discharge', '2025-01-09 13:00:00'),
(47, 'ZIMU4567890', 'USHOU', 'Discharge', '2025-01-09 13:30:00'),
(48, 'ZIMU5678901', 'USBAL', 'Discharge', '2025-01-05 14:00:00'),
(49, 'OOLU1234567', 'USLAX', 'Discharge', '2025-01-01 01:00:00'),
(50, 'OOLU1234567', 'USLAX', 'GateOut', '2025-01-25 08:00:00'); -- Huge dwell

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Dwell Time Calculation
-------------------------------------------------------------------------------

-- Identify discharge events and link to subsequent GateOut events
CREATE OR REPLACE VIEW MaritimeDB.Logistics.Silver.ContainerDwell AS
SELECT 
    c.ContainerID,
    c.ShippingLine,
    c.Type,
    m_in.PortCode,
    m_in.EventTime AS DischargeTime,
    m_out.EventTime AS GateOutTime,
    CASE 
        WHEN m_out.EventTime IS NOT NULL THEN DATE_DIFF(CAST(m_out.EventTime AS DATE), CAST(m_in.EventTime AS DATE))
        ELSE DATE_DIFF(CURRENT_DATE, CAST(m_in.EventTime AS DATE)) -- If still in port, calculate days until now
    END AS DwellDays,
    CASE WHEN m_out.EventTime IS NULL THEN 'In Port' ELSE 'Gated Out' END AS Status
FROM MaritimeDB.Logistics.Bronze.Containers c
JOIN MaritimeDB.Logistics.Bronze.PortMovements m_in 
    ON c.ContainerID = m_in.ContainerID AND m_in.MovementType = 'Discharge'
LEFT JOIN MaritimeDB.Logistics.Bronze.PortMovements m_out 
    ON c.ContainerID = m_out.ContainerID AND m_out.MovementType = 'GateOut' AND m_out.EventTime > m_in.EventTime;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Demurrage Analysis
-------------------------------------------------------------------------------

-- Assume 5 free days allowed. Charge $100/day after that.
CREATE OR REPLACE VIEW MaritimeDB.Logistics.Gold.DemurrageLiability AS
SELECT 
    ShippingLine,
    PortCode,
    COUNT(*) AS TotalContainers,
    SUM(CASE WHEN DwellDays > 5 THEN 1 ELSE 0 END) AS ContainersOverLimit,
    SUM(CASE WHEN DwellDays > 5 THEN (DwellDays - 5) * 100 ELSE 0 END) AS EstimatedDemurrageFees
FROM MaritimeDB.Logistics.Silver.ContainerDwell
GROUP BY ShippingLine, PortCode;

CREATE OR REPLACE VIEW MaritimeDB.Logistics.Gold.LongDwellAlerts AS
SELECT *
FROM MaritimeDB.Logistics.Silver.ContainerDwell
WHERE DwellDays > 10;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"Identify the shipping line with the highest estimated demurrage fees using the MaritimeDB.Logistics.Gold.DemurrageLiability view."

PROMPT 2:
"List all containers currently 'In Port' at USLAX with a dwell time greater than 10 days."

PROMPT 3:
"Show the average dwell time for Refrigerated (Reefer) containers compared to Dry containers."
*/
