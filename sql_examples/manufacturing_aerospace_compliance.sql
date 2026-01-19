/*
    Dremio High-Volume SQL Pattern: Aerospace Supplier Compliance
    
    Business Scenario:
    Aerospace parts (Bolts, Fasteners, O-Rings) must come from certified suppliers (AS9100).
    Using parts from a lapsed supplier is a major non-conformance.
    
    Data Story:
    - Bronze: PartsInventory, ApprovedSupplierList (ASL).
    - Silver: InventoryComplianceStatus (Joined on Expired Certs).
    - Gold: NonConformanceRisk (Items in stock from non-compliant sources).
    
    Medallion Architecture:
    - Bronze: PartsInventory, ApprovedSupplierList.
      *Volume*: 50+ records.
    - Silver: InventoryComplianceStatus.
    - Gold: NonConformanceRisk.
    
    Key Dremio Features:
    - Date Comparison (CertExpiry vs Today)
    - Filtering
*/

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS ManufacturingDB;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Bronze;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Silver;
CREATE FOLDER IF NOT EXISTS ManufacturingDB.Gold;
USE ManufacturingDB;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE ManufacturingDB.Bronze.ApprovedSupplierList (
    SupplierID STRING,
    SupplierName STRING,
    Certification STRING, -- AS9100, ISO9001
    CertExpiryDate DATE,
    Status STRING -- Active, Suspended
);

INSERT INTO ManufacturingDB.Bronze.ApprovedSupplierList VALUES
('S-001', 'AeroFasteners Inc', 'AS9100', DATE '2025-12-31', 'Active'),
('S-002', 'TitaniumWorks', 'AS9100', DATE '2024-12-31', 'Active'), -- Expired recently
('S-003', 'PrecisionMachining', 'ISO9001', DATE '2026-06-30', 'Active'),
('S-004', 'GlobalComposites', 'AS9100', DATE '2025-06-01', 'Active'),
('S-005', 'O-Ring Masters', 'AS9100', DATE '2023-01-01', 'Suspended'); -- Lapsed long ago

CREATE OR REPLACE TABLE ManufacturingDB.Bronze.PartsInventory (
    PartID STRING,
    PartName STRING,
    BatchID STRING,
    SupplierID STRING,
    QtyOnHand INT,
    ReceivedDate DATE
);

INSERT INTO ManufacturingDB.Bronze.PartsInventory VALUES
('P-100', 'Titanium Bolt 10mm', 'B-101', 'S-001', 500, DATE '2025-01-10'),
('P-100', 'Titanium Bolt 10mm', 'B-102', 'S-002', 300, DATE '2025-01-12'), -- Risk S-002 expired
('P-101', 'Alum Bracket', 'B-103', 'S-003', 100, DATE '2025-01-15'),
('P-102', 'Carbon Sheet', 'B-104', 'S-004', 50, DATE '2025-01-18'),
('P-103', 'Rubber Seal', 'B-105', 'S-005', 200, DATE '2024-12-01'), -- Risk S-005 Suspended

-- Bulk Data 
('P-100', 'Titanium Bolt 10mm', 'B-106', 'S-001', 500, DATE '2025-01-20'),
('P-100', 'Titanium Bolt 10mm', 'B-107', 'S-001', 500, DATE '2025-01-21'),
('P-100', 'Titanium Bolt 10mm', 'B-108', 'S-001', 500, DATE '2025-01-22'),
('P-101', 'Alum Bracket', 'B-109', 'S-003', 100, DATE '2025-01-20'),
('P-101', 'Alum Bracket', 'B-110', 'S-003', 100, DATE '2025-01-21'),
('P-102', 'Carbon Sheet', 'B-111', 'S-004', 50, DATE '2025-01-20'),
('P-103', 'Rubber Seal', 'B-112', 'S-005', 200, DATE '2024-12-10'), -- Risk
('P-104', 'Steel Pin', 'B-113', 'S-001', 1000, DATE '2025-01-10'),
('P-104', 'Steel Pin', 'B-114', 'S-002', 1000, DATE '2025-01-15'), -- Risk
('P-105', 'Nylon Washer', 'B-115', 'S-003', 2000, DATE '2025-01-05'),
('P-105', 'Nylon Washer', 'B-116', 'S-003', 2000, DATE '2025-01-06'),
('P-106', 'Copper Wire', 'B-117', 'S-004', 500, DATE '2025-01-05'),
('P-106', 'Copper Wire', 'B-118', 'S-004', 500, DATE '2025-01-06'),
('P-107', 'Heat Shield', 'B-119', 'S-001', 20, DATE '2025-01-05'),
('P-107', 'Heat Shield', 'B-120', 'S-002', 20, DATE '2025-01-06'), -- Risk
('P-108', 'Sensor Mount', 'B-121', 'S-001', 50, DATE '2025-01-10'),
('P-109', 'Hydraulic Pump', 'B-122', 'S-004', 5, DATE '2025-01-12'),
('P-110', 'Fuel Line', 'B-123', 'S-002', 25, DATE '2025-01-15'), -- Risk
('P-111', 'Avionics Case', 'B-124', 'S-001', 10, DATE '2025-01-15'),
('P-112', 'Landing Gear Bolt', 'B-125', 'S-001', 50, DATE '2025-01-15'),
('P-113', 'Wing Rib', 'B-126', 'S-004', 2, DATE '2025-01-15'),
('P-114', 'Tail Fin', 'B-127', 'S-004', 1, DATE '2025-01-15'),
('P-115', 'Cockpit Glass', 'B-128', 'S-003', 5, DATE '2025-01-15'),
('P-116', 'Seat Frame', 'B-129', 'S-003', 20, DATE '2025-01-15'),
('P-117', 'Cargo Net', 'B-130', 'S-003', 15, DATE '2025-01-15'),
('P-118', 'Oxygen Mask', 'B-131', 'S-001', 100, DATE '2025-01-15'),
('P-119', 'Life Vest', 'B-132', 'S-001', 100, DATE '2025-01-15'),
('P-120', 'Floor Panel', 'B-133', 'S-002', 10, DATE '2025-01-15'), -- Risk
('P-121', 'Overhead Bin', 'B-134', 'S-002', 10, DATE '2025-01-15'), -- Risk 
('P-122', 'Galley Cart', 'B-135', 'S-003', 5, DATE '2025-01-15'),
('P-123', 'Lavatory Unit', 'B-136', 'S-003', 2, DATE '2025-01-15'),
('P-124', 'Engine Mount', 'B-137', 'S-001', 4, DATE '2025-01-15'),
('P-125', 'Fan Blade', 'B-138', 'S-001', 30, DATE '2025-01-15'),
('P-126', 'Compressor Disc', 'B-139', 'S-001', 10, DATE '2025-01-15'),
('P-127', 'Turbine Shafe', 'B-140', 'S-001', 5, DATE '2025-01-15'),
('P-128', 'Exhaust Cone', 'B-141', 'S-002', 3, DATE '2025-01-15'), -- Risk
('P-129', 'Thrust Reverser', 'B-142', 'S-002', 2, DATE '2025-01-15'), -- Risk
('P-130', 'Nose Cone', 'B-143', 'S-004', 1, DATE '2025-01-15'),
('P-131', 'Radar Dish', 'B-144', 'S-004', 1, DATE '2025-01-15'),
('P-132', 'Antenna', 'B-145', 'S-004', 5, DATE '2025-01-15'),
('P-133', 'Pitot Tube', 'B-146', 'S-001', 10, DATE '2025-01-15'),
('P-134', 'Static Port', 'B-147', 'S-001', 10, DATE '2025-01-15'),
('P-135', 'AOA Sensor', 'B-148', 'S-001', 5, DATE '2025-01-15'),
('P-136', 'Temp Probe', 'B-149', 'S-001', 5, DATE '2025-01-15'),
('P-137', 'Pressure Valve', 'B-150', 'S-003', 20, DATE '2025-01-15');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Inventory Compliance Status
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Silver.InventoryComplianceStatus AS
SELECT
    i.PartID,
    i.PartName,
    i.BatchID,
    i.QtyOnHand,
    s.SupplierName,
    s.Certification,
    s.CertExpiryDate,
    s.Status AS SupplierStatus,
    CASE 
        WHEN s.Status = 'Suspended' THEN 'NON_COMPLIANT'
        WHEN s.CertExpiryDate < DATE '2025-01-20' THEN 'NON_COMPLIANT' -- Assumed Today
        ELSE 'COMPLIANT'
    END AS ComplianceCheck
FROM ManufacturingDB.Bronze.PartsInventory i
JOIN ManufacturingDB.Bronze.ApprovedSupplierList s ON i.SupplierID = s.SupplierID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Non-Conformance Risk
-------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ManufacturingDB.Gold.NonConformanceRisk AS
SELECT
    SupplierName,
    COUNT(DISTINCT PartID) AS ImpactedParts,
    SUM(QtyOnHand) AS TotalRiskQty,
    ARRAY_AGG(PartName) AS PartList
FROM ManufacturingDB.Silver.InventoryComplianceStatus
WHERE ComplianceCheck = 'NON_COMPLIANT'
GROUP BY SupplierName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------
/*
    Run the following prompts in Dremio's AI Chat to visualize these insights:

    1. "Show me all non-compliant inventory items."
    2. "List suppliers with expired certifications who have active inventory."
    3. "Calculate total quantity of risky parts by Supplier."
*/
