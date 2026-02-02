/*
 * Dremio Humanitarian Aid Distribution Example
 * 
 * Domain: Non-Profit & Crisis Management
 * Scenario: Managing resource allocation in refugee camps based on population needs and supply levels.
 * Complexity: Medium (Aggregated forecasting, inventory logic)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the source tables.
 * 2. Run the INSERT statements to seed the data (50+ records per table).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. DDL: Create Source Tables
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS "Aid_Org"."Camp_Population";
DROP TABLE IF EXISTS "Aid_Org"."Supply_Inventory";

CREATE TABLE "Aid_Org"."Camp_Population" (
    FamilyID VARCHAR,
    Camp_Zone VARCHAR, -- e.g., 'Zone-Alpha', 'Zone-Beta'
    Num_Adults INT,
    Num_Children INT,
    Num_Infants INT,
    Special_Needs_Flag BOOLEAN, -- Pregnant, Elderly, Disabled
    Arrival_Date DATE
);

CREATE TABLE "Aid_Org"."Supply_Inventory" (
    BatchID VARCHAR,
    Item_Category VARCHAR, -- 'Rice', 'Water', 'Medicine', 'Blankets'
    Quantity_Units INT,
    Expiry_Date DATE,
    Warehouse_Zone VARCHAR
);

-------------------------------------------------------------------------------
-- 2. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Population (Simulating families arriving)
INSERT INTO "Aid_Org"."Camp_Population" VALUES
('FAM-001', 'Zone-Alpha', 2, 3, 1, TRUE, '2023-01-01'),
('FAM-002', 'Zone-Alpha', 1, 2, 0, FALSE, '2023-01-02'),
('FAM-003', 'Zone-Alpha', 2, 0, 0, TRUE, '2023-01-02'),
('FAM-004', 'Zone-Alpha', 2, 4, 1, FALSE, '2023-01-03'),
('FAM-005', 'Zone-Alpha', 1, 1, 0, FALSE, '2023-01-04'),
('FAM-006', 'Zone-Beta', 2, 2, 0, FALSE, '2023-01-05'),
('FAM-007', 'Zone-Beta', 1, 3, 1, TRUE, '2023-01-06'),
('FAM-008', 'Zone-Beta', 2, 1, 0, FALSE, '2023-01-06'),
('FAM-009', 'Zone-Beta', 2, 5, 2, TRUE, '2023-01-07'),
('FAM-010', 'Zone-Beta', 1, 0, 0, TRUE, '2023-01-08'),
('FAM-011', 'Zone-Gamma', 2, 2, 0, FALSE, '2023-01-09'),
('FAM-012', 'Zone-Gamma', 2, 3, 1, FALSE, '2023-01-10'),
('FAM-013', 'Zone-Gamma', 1, 1, 0, TRUE, '2023-01-10'),
('FAM-014', 'Zone-Gamma', 2, 0, 0, FALSE, '2023-01-11'),
('FAM-015', 'Zone-Gamma', 2, 4, 0, FALSE, '2023-01-12'),
('FAM-016', 'Zone-Alpha', 2, 2, 0, FALSE, '2023-01-15'),
('FAM-017', 'Zone-Alpha', 1, 2, 1, TRUE, '2023-01-16'),
('FAM-018', 'Zone-Alpha', 2, 1, 0, FALSE, '2023-01-17'),
('FAM-019', 'Zone-Beta', 2, 3, 0, FALSE, '2023-01-18'),
('FAM-020', 'Zone-Beta', 1, 1, 0, TRUE, '2023-01-19'),
('FAM-021', 'Zone-Gamma', 2, 2, 1, FALSE, '2023-01-20'),
('FAM-022', 'Zone-Gamma', 2, 0, 0, FALSE, '2023-01-21'),
('FAM-023', 'Zone-Alpha', 1, 3, 2, TRUE, '2023-01-22'),
('FAM-024', 'Zone-Alpha', 2, 1, 0, FALSE, '2023-01-23'),
('FAM-025', 'Zone-Beta', 2, 4, 1, TRUE, '2023-01-24'),
('FAM-026', 'Zone-Beta', 1, 0, 0, FALSE, '2023-01-25'),
('FAM-027', 'Zone-Gamma', 2, 2, 0, FALSE, '2023-01-26'),
('FAM-028', 'Zone-Gamma', 1, 1, 1, TRUE, '2023-01-27'),
('FAM-029', 'Zone-Alpha', 2, 3, 0, FALSE, '2023-01-28'),
('FAM-030', 'Zone-Alpha', 1, 2, 0, FALSE, '2023-01-29'),
('FAM-031', 'Zone-Beta', 2, 1, 0, TRUE, '2023-01-30'),
('FAM-032', 'Zone-Beta', 2, 0, 0, FALSE, '2023-01-31'),
('FAM-033', 'Zone-Gamma', 1, 4, 2, TRUE, '2023-02-01'),
('FAM-034', 'Zone-Gamma', 2, 2, 0, FALSE, '2023-02-02'),
('FAM-035', 'Zone-Alpha', 2, 1, 0, FALSE, '2023-02-03'),
('FAM-036', 'Zone-Alpha', 1, 3, 1, TRUE, '2023-02-04'),
('FAM-037', 'Zone-Beta', 2, 0, 0, FALSE, '2023-02-05'),
('FAM-038', 'Zone-Beta', 2, 2, 0, FALSE, '2023-02-06'),
('FAM-039', 'Zone-Gamma', 1, 1, 0, TRUE, '2023-02-07'),
('FAM-040', 'Zone-Gamma', 2, 3, 0, FALSE, '2023-02-08'),
('FAM-041', 'Zone-Alpha', 1, 0, 0, TRUE, '2023-02-09'),
('FAM-042', 'Zone-Alpha', 2, 4, 1, FALSE, '2023-02-10'),
('FAM-043', 'Zone-Beta', 2, 2, 0, FALSE, '2023-02-11'),
('FAM-044', 'Zone-Beta', 1, 1, 0, FALSE, '2023-02-12'),
('FAM-045', 'Zone-Gamma', 2, 0, 0, TRUE, '2023-02-13'),
('FAM-046', 'Zone-Gamma', 2, 3, 1, FALSE, '2023-02-14'),
('FAM-047', 'Zone-Alpha', 1, 2, 0, FALSE, '2023-02-15'),
('FAM-048', 'Zone-Alpha', 2, 1, 0, TRUE, '2023-02-16'),
('FAM-049', 'Zone-Beta', 2, 2, 0, FALSE, '2023-02-17'),
('FAM-050', 'Zone-Beta', 1, 0, 0, FALSE, '2023-02-18');

-- Seed Inventory
INSERT INTO "Aid_Org"."Supply_Inventory" VALUES
('BAT-100', 'Rice', 500, '2025-01-01', 'Zone-Alpha'),
('BAT-101', 'Rice', 200, '2025-01-10', 'Zone-Beta'),
('BAT-102', 'Rice', 300, '2025-01-15', 'Zone-Gamma'),
('BAT-103', 'Water', 1000, '2024-06-01', 'Zone-Alpha'),
('BAT-104', 'Water', 800, '2024-06-05', 'Zone-Beta'),
('BAT-105', 'Water', 1200, '2024-06-10', 'Zone-Gamma'),
('BAT-106', 'Medicine', 50, '2024-12-01', 'Zone-Alpha'),
('BAT-107', 'Medicine', 30, '2024-12-05', 'Zone-Beta'),
('BAT-108', 'Medicine', 40, '2024-12-10', 'Zone-Gamma'),
('BAT-109', 'Blankets', 100, '2030-01-01', 'Zone-Alpha'),
('BAT-110', 'Blankets', 80, '2030-01-01', 'Zone-Beta'),
('BAT-111', 'Blankets', 90, '2030-01-01', 'Zone-Gamma'),
('BAT-112', 'Rice', 600, '2025-02-01', 'Zone-Alpha'),
('BAT-113', 'Rice', 250, '2025-02-05', 'Zone-Beta'),
('BAT-114', 'Rice', 350, '2025-02-10', 'Zone-Gamma'),
('BAT-115', 'Water', 1500, '2024-07-01', 'Zone-Alpha'),
('BAT-116', 'Water', 1100, '2024-07-05', 'Zone-Beta'),
('BAT-117', 'Water', 1600, '2024-07-10', 'Zone-Gamma'),
('BAT-118', 'Medicine', 60, '2025-01-01', 'Zone-Alpha'),
('BAT-119', 'Medicine', 40, '2025-01-05', 'Zone-Beta'),
('BAT-120', 'Medicine', 50, '2025-01-10', 'Zone-Gamma'),
('BAT-121', 'Blankets', 120, '2030-02-01', 'Zone-Alpha'),
('BAT-122', 'Blankets', 100, '2030-02-01', 'Zone-Beta'),
('BAT-123', 'Blankets', 110, '2030-02-01', 'Zone-Gamma'),
('BAT-124', 'Rice', 550, '2025-03-01', 'Zone-Alpha'),
('BAT-125', 'Rice', 220, '2025-03-05', 'Zone-Beta'),
('BAT-126', 'Rice', 320, '2025-03-10', 'Zone-Gamma'),
('BAT-127', 'Water', 1300, '2024-08-01', 'Zone-Alpha'),
('BAT-128', 'Water', 900, '2024-08-05', 'Zone-Beta'),
('BAT-129', 'Water', 1400, '2024-08-10', 'Zone-Gamma'),
('BAT-130', 'Medicine', 55, '2025-02-01', 'Zone-Alpha'),
('BAT-131', 'Medicine', 35, '2025-02-05', 'Zone-Beta'),
('BAT-132', 'Medicine', 45, '2025-02-10', 'Zone-Gamma'),
('BAT-133', 'Blankets', 110, '2030-03-01', 'Zone-Alpha'),
('BAT-134', 'Blankets', 90, '2030-03-01', 'Zone-Beta'),
('BAT-135', 'Blankets', 100, '2030-03-01', 'Zone-Gamma'),
('BAT-136', 'Hygiene Kits', 200, '2024-12-01', 'Zone-Alpha'),
('BAT-137', 'Hygiene Kits', 150, '2024-12-01', 'Zone-Beta'),
('BAT-138', 'Hygiene Kits', 180, '2024-12-01', 'Zone-Gamma'),
('BAT-139', 'Tents', 50, '2028-01-01', 'Zone-Alpha'),
('BAT-140', 'Tents', 30, '2028-01-01', 'Zone-Beta'),
('BAT-141', 'Tents', 40, '2028-01-01', 'Zone-Gamma'),
('BAT-142', 'Solar Lamps', 100, '2029-01-01', 'Zone-Alpha'),
('BAT-143', 'Solar Lamps', 80, '2029-01-01', 'Zone-Beta'),
('BAT-144', 'Solar Lamps', 90, '2029-01-01', 'Zone-Gamma'),
('BAT-145', 'Water Filters', 50, '2027-01-01', 'Zone-Alpha'),
('BAT-146', 'Water Filters', 30, '2027-01-01', 'Zone-Beta'),
('BAT-147', 'Water Filters', 40, '2027-01-01', 'Zone-Gamma'),
('BAT-148', 'Formula', 100, '2024-11-01', 'Zone-Alpha'),
('BAT-149', 'Formula', 80, '2024-11-01', 'Zone-Beta'),
('BAT-150', 'Formula', 90, '2024-11-01', 'Zone-Gamma');


-------------------------------------------------------------------------------
-- 3. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 3a. BRONZE LAYER
CREATE OR REPLACE VIEW "Aid_Org"."Bronze_Population" AS
SELECT * FROM "Aid_Org"."Camp_Population";

CREATE OR REPLACE VIEW "Aid_Org"."Bronze_Inventory" AS
SELECT * FROM "Aid_Org"."Supply_Inventory";

-- 3b. SILVER LAYER (Calculated Needs)
CREATE OR REPLACE VIEW "Aid_Org"."Silver_Zone_Demographics" AS
SELECT
    Camp_Zone,
    COUNT(*) as Total_Families,
    SUM(Num_Adults + Num_Children + Num_Infants) as Total_People,
    SUM(Num_Infants) as Infant_Count,
    SUM(CASE WHEN Special_Needs_Flag THEN 1 ELSE 0 END) as Vulnerable_Families,
    -- Standard Aid Metrics: 20L water/person/day, 0.5kg rice/person/day
    SUM(Num_Adults + Num_Children + Num_Infants) * 20 as Daily_Water_Liters_Need,
    SUM(Num_Adults + Num_Children + Num_Infants) * 0.5 as Daily_Rice_KG_Need
FROM "Aid_Org"."Bronze_Population"
GROUP BY Camp_Zone;

CREATE OR REPLACE VIEW "Aid_Org"."Silver_Zone_Inventory" AS
SELECT
    Warehouse_Zone as Camp_Zone,
    Item_Category,
    SUM(Quantity_Units) as Total_Available,
    MIN(Expiry_Date) as Next_Expiry
FROM "Aid_Org"."Bronze_Inventory"
GROUP BY Warehouse_Zone, Item_Category;

-- 3c. GOLD LAYER (Gap Analysis)
CREATE OR REPLACE VIEW "Aid_Org"."Gold_Supply_Gaps" AS
SELECT
    d.Camp_Zone,
    d.Total_People,
    inv_rice.Total_Available as Rice_Available,
    d.Daily_Rice_KG_Need,
    -- Days of supply remaining
    ROUND(inv_rice.Total_Available / NULLIF(d.Daily_Rice_KG_Need, 0), 1) as Rice_Days_coverage,
    inv_water.Total_Available as Water_Available,
    d.Daily_Water_Liters_Need,
    ROUND(inv_water.Total_Available / NULLIF(d.Daily_Water_Liters_Need, 0), 1) as Water_Days_coverage
FROM "Aid_Org"."Silver_Zone_Demographics" d
LEFT JOIN "Aid_Org"."Silver_Zone_Inventory" inv_rice
  ON d.Camp_Zone = inv_rice.Camp_Zone AND inv_rice.Item_Category = 'Rice'
LEFT JOIN "Aid_Org"."Silver_Zone_Inventory" inv_water
  ON d.Camp_Zone = inv_water.Camp_Zone AND inv_water.Item_Category = 'Water';

-------------------------------------------------------------------------------
-- 4. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Check 'Gold_Supply_Gaps' for any zones with < 3 days of water coverage. 
 * Identify if high infant populations ('Silver_Zone_Demographics') correlate with low formula supplies."
 */
