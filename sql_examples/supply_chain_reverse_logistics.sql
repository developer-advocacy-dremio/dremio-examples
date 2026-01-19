/*
    Dremio High-Volume SQL Pattern: Supply Chain Reverse Logistics
    
    Business Scenario:
    Handling product returns (RMAs) is a major cost center. 
    We need to track why items return (Defect vs Remorse), what happens to them
    (Refurbish, Scrap, Restock), and the associated costs.
    
    Data Story:
    - Bronze tables track the initial RMA request and the subsequent processing logs.
    - Silver focuses on aging (how long returns sit in the warehouse).
    - Gold aggregates recovery rates and costs.
    
    Medallion Architecture:
    - Bronze: ReturnRequests, RefurbishmentLogs.
      *Volume*: 50+ records.
    - Silver: ReturnsAging (Day Diff calculation).
    - Gold: RecoveryAnalysis (Cost vs Salvage Value).
    
    Key Dremio Features:
    - DATE_ADD / TIMESTAMPDIFF
    - JOINs on RMA_ID
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS SupplyChainDB;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Bronze;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Silver;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Gold;
USE SupplyChainDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SupplyChainDB.Bronze.ReturnRequests (
    RMA_ID STRING,
    RequestDate DATE,
    CustomerID STRING,
    ProductID STRING,
    Reason STRING -- Defect, WrongItem, Remorse
);

INSERT INTO SupplyChainDB.Bronze.ReturnRequests VALUES
('RMA001', DATE '2025-01-01', 'C001', 'P100', 'Defect'),
('RMA002', DATE '2025-01-01', 'C002', 'P101', 'Remorse'),
('RMA003', DATE '2025-01-02', 'C003', 'P100', 'Defect'),
('RMA004', DATE '2025-01-02', 'C004', 'P102', 'WrongItem'),
('RMA005', DATE '2025-01-03', 'C005', 'P100', 'Defect'),
('RMA006', DATE '2025-01-03', 'C006', 'P103', 'Remorse'),
('RMA007', DATE '2025-01-04', 'C007', 'P101', 'Remorse'),
('RMA008', DATE '2025-01-04', 'C008', 'P100', 'Defect'),
('RMA009', DATE '2025-01-05', 'C009', 'P102', 'WrongItem'),
('RMA010', DATE '2025-01-05', 'C010', 'P100', 'Defect'),
('RMA011', DATE '2025-01-06', 'C011', 'P104', 'Remorse'),
('RMA012', DATE '2025-01-06', 'C012', 'P100', 'Defect'),
('RMA013', DATE '2025-01-07', 'C013', 'P101', 'Remorse'),
('RMA014', DATE '2025-01-07', 'C014', 'P102', 'WrongItem'),
('RMA015', DATE '2025-01-08', 'C015', 'P100', 'Defect'),
('RMA016', DATE '2025-01-08', 'C016', 'P105', 'Remorse'),
('RMA017', DATE '2025-01-09', 'C017', 'P101', 'Remorse'),
('RMA018', DATE '2025-01-09', 'C018', 'P100', 'Defect'),
('RMA019', DATE '2025-01-10', 'C019', 'P103', 'WrongItem'),
('RMA020', DATE '2025-01-10', 'C020', 'P100', 'Defect'),
('RMA021', DATE '2025-01-11', 'C021', 'P101', 'Remorse'),
('RMA022', DATE '2025-01-11', 'C022', 'P100', 'Defect'),
('RMA023', DATE '2025-01-12', 'C023', 'P102', 'WrongItem'),
('RMA024', DATE '2025-01-12', 'C024', 'P100', 'Defect'),
('RMA025', DATE '2025-01-13', 'C025', 'P106', 'Remorse'),
('RMA026', DATE '2025-01-13', 'C026', 'P100', 'Defect'),
('RMA027', DATE '2025-01-14', 'C027', 'P101', 'Remorse'),
('RMA028', DATE '2025-01-14', 'C028', 'P100', 'Defect'),
('RMA029', DATE '2025-01-15', 'C029', 'P104', 'WrongItem'),
('RMA030', DATE '2025-01-15', 'C030', 'P100', 'Defect'),
('RMA031', DATE '2025-01-16', 'C031', 'P101', 'Remorse'),
('RMA032', DATE '2025-01-16', 'C032', 'P100', 'Defect'),
('RMA033', DATE '2025-01-17', 'C033', 'P107', 'WrongItem'),
('RMA034', DATE '2025-01-17', 'C034', 'P100', 'Defect'),
('RMA035', DATE '2025-01-18', 'C035', 'P101', 'Remorse'),
('RMA036', DATE '2025-01-18', 'C036', 'P100', 'Defect'),
('RMA037', DATE '2025-01-19', 'C037', 'P108', 'WrongItem'),
('RMA038', DATE '2025-01-19', 'C038', 'P100', 'Defect'),
('RMA039', DATE '2025-01-20', 'C039', 'P101', 'Remorse'),
('RMA040', DATE '2025-01-20', 'C040', 'P100', 'Defect'),
('RMA041', DATE '2025-01-21', 'C041', 'P102', 'Remorse'),
('RMA042', DATE '2025-01-21', 'C042', 'P100', 'Defect'),
('RMA043', DATE '2025-01-22', 'C043', 'P103', 'WrongItem'),
('RMA044', DATE '2025-01-22', 'C044', 'P100', 'Defect'),
('RMA045', DATE '2025-01-23', 'C045', 'P101', 'Remorse'),
('RMA046', DATE '2025-01-23', 'C046', 'P100', 'Defect'),
('RMA047', DATE '2025-01-24', 'C047', 'P105', 'WrongItem'),
('RMA048', DATE '2025-01-24', 'C048', 'P100', 'Defect'),
('RMA049', DATE '2025-01-25', 'C049', 'P101', 'Remorse'),
('RMA050', DATE '2025-01-25', 'C050', 'P100', 'Defect');

CREATE OR REPLACE TABLE SupplyChainDB.Bronze.RefurbishmentLogs (
    LogID INT,
    RMA_ID STRING,
    ProcessDate DATE,
    ActionTaken STRING, -- Restock, Refurbish, Scrap
    Cost DOUBLE
);

INSERT INTO SupplyChainDB.Bronze.RefurbishmentLogs VALUES
(1, 'RMA001', DATE '2025-01-05', 'Refurbish', 25.50), -- 4 days later
(2, 'RMA002', DATE '2025-01-03', 'Restock', 5.00), -- 2 days
(3, 'RMA003', DATE '2025-01-10', 'Scrap', 0.00), -- 8 days
(4, 'RMA004', DATE '2025-01-05', 'Restock', 5.00),
(5, 'RMA005', DATE '2025-01-12', 'Refurbish', 30.00),
(6, 'RMA006', DATE '2025-01-05', 'Restock', 5.00),
(7, 'RMA007', DATE '2025-01-08', 'Restock', 5.00),
(8, 'RMA008', DATE '2025-01-15', 'Scrap', 0.00),
(9, 'RMA009', DATE '2025-01-08', 'Restock', 5.00),
(10, 'RMA010', DATE '2025-01-18', 'Refurbish', 28.00),
(11, 'RMA011', DATE '2025-01-08', 'Restock', 5.00),
(12, 'RMA012', DATE '2025-01-15', 'Refurbish', 22.00),
(13, 'RMA013', DATE '2025-01-10', 'Restock', 5.00),
(14, 'RMA014', DATE '2025-01-12', 'Restock', 5.00),
(15, 'RMA015', DATE '2025-01-20', 'Scrap', 0.00),
(16, 'RMA016', DATE '2025-01-12', 'Restock', 5.00),
(17, 'RMA017', DATE '2025-01-14', 'Restock', 5.00),
(18, 'RMA018', DATE '2025-01-20', 'Refurbish', 35.00),
(19, 'RMA019', DATE '2025-01-15', 'Restock', 5.00),
(20, 'RMA020', DATE '2025-01-22', 'Scrap', 0.00),
(21, 'RMA021', DATE '2025-01-15', 'Restock', 5.00),
(22, 'RMA022', DATE '2025-01-25', 'Refurbish', 18.00),
(23, 'RMA023', DATE '2025-01-18', 'Restock', 5.00),
(24, 'RMA024', DATE '2025-01-28', 'Refurbish', 25.00),
(25, 'RMA025', DATE '2025-01-16', 'Restock', 5.00),
(26, 'RMA026', DATE '2025-01-28', 'Scrap', 0.00),
(27, 'RMA027', DATE '2025-01-18', 'Restock', 5.00),
(28, 'RMA028', NULL, NULL, NULL), -- Not yet processed
(29, 'RMA029', DATE '2025-01-18', 'Restock', 5.00),
(30, 'RMA030', NULL, NULL, NULL),
(31, 'RMA031', DATE '2025-01-20', 'Restock', 5.00),
(32, 'RMA032', NULL, NULL, NULL),
(33, 'RMA033', DATE '2025-01-20', 'Restock', 5.00),
(34, 'RMA034', NULL, NULL, NULL),
(35, 'RMA035', DATE '2025-01-22', 'Restock', 5.00),
(36, 'RMA036', NULL, NULL, NULL),
(37, 'RMA037', DATE '2025-01-22', 'Restock', 5.00),
(38, 'RMA038', NULL, NULL, NULL),
(39, 'RMA039', DATE '2025-01-25', 'Restock', 5.00),
(40, 'RMA040', NULL, NULL, NULL),
(41, 'RMA041', DATE '2025-01-25', 'Restock', 5.00),
(42, 'RMA042', NULL, NULL, NULL),
(43, 'RMA043', DATE '2025-01-26', 'Restock', 5.00),
(44, 'RMA044', NULL, NULL, NULL),
(45, 'RMA045', NULL, NULL, NULL),
(46, 'RMA046', NULL, NULL, NULL),
(47, 'RMA047', NULL, NULL, NULL),
(48, 'RMA048', NULL, NULL, NULL),
(49, 'RMA049', NULL, NULL, NULL),
(50, 'RMA050', NULL, NULL, NULL);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Returns Aging
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Silver.ReturnsAging AS
SELECT
    r.RMA_ID,
    r.RequestDate,
    l.ProcessDate,
    r.Reason,
    l.ActionTaken,
    TIMESTAMPDIFF(DAY, r.RequestDate, COALESCE(l.ProcessDate, DATE '2025-01-30')) AS DaysToProcess,
    CASE WHEN l.ProcessDate IS NULL THEN 'Pending' ELSE 'Processed' END AS Status
FROM SupplyChainDB.Bronze.ReturnRequests r
LEFT JOIN SupplyChainDB.Bronze.RefurbishmentLogs l ON r.RMA_ID = l.RMA_ID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Recovery Analysis
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW SupplyChainDB.Gold.RecoveryAnalysis AS
SELECT
    Reason,
    COUNT(RMA_ID) AS TotalReturns,
    SUM(CASE WHEN ActionTaken = 'Restock' THEN 1 ELSE 0 END) AS RestockCount,
    SUM(CASE WHEN ActionTaken = 'Scrap' THEN 1 ELSE 0 END) AS ScrapCount,
    AVG(DaysToProcess) AS AvgProcessingTime,
    SUM(COALESCE(Cost, 0)) AS TotalProcessingCost
FROM SupplyChainDB.Silver.ReturnsAging
WHERE Status = 'Processed'
GROUP BY Reason;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Calculate total processing cost by Return Reason."
    2. "List all pending returns that are older than 5 days."
    3. "What value of inventory was scrapped this month?"
*/
