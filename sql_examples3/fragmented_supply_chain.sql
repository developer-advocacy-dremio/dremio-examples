/*
 * Dremio "Messy Data" Challenge: Fragmented Supply Chain
 * 
 * Scenario: 
 * Inventory data from US Supplier (Lbs, $) and EU Supplier (Kg, €).
 * 'SKU' codes overlap but might refer to different batch sizes.
 * 
 * Objective for AI Agent:
 * 1. Normalize Weight: Convert Lbs to Kg (1 Lb = 0.453592 Kg).
 * 2. Normalize Cost: Convert EUR to USD (approx rate).
 * 3. Union datasets into a Global Inventory view.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Supply_Chain_Silo;
CREATE FOLDER IF NOT EXISTS Supply_Chain_Silo.Regions;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Supply_Chain_Silo.Regions.US_WH (
    SKU VARCHAR,
    DESC_TXT VARCHAR,
    WEIGHT_LBS DOUBLE,
    COST_USD DOUBLE,
    QTY INT
);

CREATE TABLE IF NOT EXISTS Supply_Chain_Silo.Regions.EU_WH (
    ITEM_CODE VARCHAR, -- Same as SKU
    NAME_DE VARCHAR,
    WEIGHT_KG DOUBLE,
    COST_EUR DOUBLE,
    STOCK INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Unit Mismatches)
-------------------------------------------------------------------------------

-- US Data
INSERT INTO Supply_Chain_Silo.Regions.US_WH VALUES
('A-100', 'Steel Pipe', 10.0, 50.0, 100),
('A-101', 'Copper Wire', 5.0, 100.0, 50),
('B-200', 'Rubber Mat', 2.0, 20.0, 500);

-- EU Data
INSERT INTO Supply_Chain_Silo.Regions.EU_WH VALUES
('A-100', 'Stahlrohr', 4.5, 45.0, 200), -- 10lbs ~= 4.5kg
('C-300', 'Glass Sheet', 10.0, 80.0, 50);

-- Bulk US
INSERT INTO Supply_Chain_Silo.Regions.US_WH VALUES
('A-102', 'Aluminum Rod', 1.0, 5.0, 1000),
('A-103', 'Iron Bar', 20.0, 40.0, 50),
('A-104', 'Zinc Plate', 0.5, 10.0, 2000),
('B-201', 'Plastic Sheet', 1.0, 5.0, 500),
('B-202', 'Foam Block', 0.1, 1.0, 100),
('B-203', 'Tape Roll', 0.2, 2.0, 500),
('C-301', 'Wood Plank', 5.0, 15.0, 100),
('C-302', 'Plywood', 10.0, 25.0, 100),
('D-400', 'Nails Box', 1.0, 5.0, 500),
('D-401', 'Screws Box', 1.0, 6.0, 500),
('D-402', 'Bolts Box', 2.0, 8.0, 300),
('A-105', 'Steel Beam', 50.0, 200.0, 10),
('A-106', 'Steel Cable', 5.0, 30.0, 100),
('B-204', 'PVC Pipe', 2.0, 10.0, 200),
('C-303', 'Oak Log', 100.0, 150.0, 5),
('D-403', 'Washers', 0.1, 1.0, 1000),
('A-107', 'Titanium', 5.0, 500.0, 10),
('B-205', 'Nylon', 1.0, 10.0, 200),
('C-304', 'Pine', 10.0, 10.0, 200),
('D-404', 'Glue', 0.5, 5.0, 100),
('A-108', 'Lead', 10.0, 15.0, 50),
('A-109', 'Gold Leaf', 0.01, 1000.0, 5),
('B-206', 'Resin', 5.0, 50.0, 20),
('C-305', 'Bamboo', 1.0, 5.0, 500),
('D-405', 'Paint', 10.0, 40.0, 50);

-- Bulk EU
INSERT INTO Supply_Chain_Silo.Regions.EU_WH VALUES
('A-101', 'Kupferdraht', 2.2, 90.0, 60), -- Matches US A-101
('B-200', 'Gummimatte', 0.9, 18.0, 400), -- Matches US B-200
('A-102', 'Aluminium', 0.45, 4.5, 900),
('C-300', 'Glasplatte', 10.0, 80.0, 50),
('E-500', 'Keramik', 1.0, 20.0, 100),
('E-501', 'Ziegel', 2.0, 2.0, 1000),
('E-502', 'Beton', 25.0, 5.0, 500),
('A-103', 'Eisen', 9.0, 35.0, 40),
('D-400', 'Nagel', 0.45, 4.0, 400),
('B-201', 'Plastik', 0.45, 4.0, 400),
('F-600', 'Stoff', 0.5, 10.0, 200),
('F-601', 'Leder', 1.0, 50.0, 100),
('F-602', 'Seide', 0.1, 100.0, 50),
('D-401', 'Schrauben', 0.45, 5.0, 450),
('A-109', 'Blattgold', 0.005, 900.0, 4),
('G-700', 'Papier', 0.01, 0.1, 10000),
('G-701', 'Pappe', 0.1, 0.5, 5000),
('H-800', 'Öl', 1.0, 10.0, 100),
('H-801', 'Benzin', 0.8, 15.0, 100),
('A-105', 'Stahlträger', 22.0, 180.0, 10),
('E-503', 'Marmor', 50.0, 100.0, 5),
('C-301', 'Holz', 2.2, 14.0, 90),
('B-202', 'Schaum', 0.05, 0.9, 100),
('F-603', 'Wolle', 0.2, 5.0, 200);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Harmonize the US (Supply_Chain_Silo.Regions.US_WH) and EU (EU_WH) inventories.
 *  
 *  1. Bronze: Raw Views.
 *  2. Silver: 
 *     - Convert US weights from Lbs to Kg (x 0.453592).
 *     - Convert EU prices from EUR to USD (x 1.1).
 *     - Union both tables into 'Global_Stock'.
 *  3. Gold: 
 *     - Sum Total Stock (QTY) and Total Value (USD) by SKU/Item_Code.
 *  
 *  Show me the SQL."
 */
