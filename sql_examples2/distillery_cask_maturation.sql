/*
 * Dremio Distillery Cask Maturation Example
 * 
 * Domain: Enology & Sprit Production
 * Scenario: 
 * A premium Whisky Distillery manages thousands of casks (Barrels) aging in different warehouses.
 * They need to track "Angel's Share" (evaporation loss) over decades and monitor 
 * flavor profile development based on "Cask Type" (Sherry, Bourbon, Virgin Oak).
 * 
 * Complexity: Medium (Temporal projections, volume decay calculations)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Distillery_Ops;
CREATE FOLDER IF NOT EXISTS Distillery_Ops.Sources;
CREATE FOLDER IF NOT EXISTS Distillery_Ops.Bronze;
CREATE FOLDER IF NOT EXISTS Distillery_Ops.Silver;
CREATE FOLDER IF NOT EXISTS Distillery_Ops.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Distillery_Ops.Sources.Cask_Inventory (
    CaskID VARCHAR,
    Spirit_Type VARCHAR, -- 'Single Malt', 'Rye', 'Blend'
    Fill_Date DATE,
    Cask_Wood_Type VARCHAR, -- 'American Oak', 'Spanish Sherry', 'Mizunara'
    Warehouse_Location VARCHAR, -- 'Rickhouse-A', 'Dunnage-B'
    Original_Liters_Alcohol DOUBLE,
    ABV_Percent_Initial DOUBLE
);

CREATE TABLE IF NOT EXISTS Distillery_Ops.Sources.Maturation_Checks (
    CheckID VARCHAR,
    CaskID VARCHAR,
    Check_Date DATE,
    Current_Volume_Liters DOUBLE,
    Current_ABV_Percent DOUBLE,
    Tasting_Notes VARCHAR, -- 'Vanilla, Oak', 'Fruity, Spice'
    Master_Distiller_Score INT -- 1-100
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Casks (Some old, some new)
INSERT INTO Distillery_Ops.Sources.Cask_Inventory VALUES
('CSK-1990-001', 'Single Malt', '1990-01-01', 'Spanish Sherry', 'Dunnage-B', 200.0, 63.5),
('CSK-1995-002', 'Single Malt', '1995-06-01', 'American Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-2000-003', 'Rye',         '2000-01-01', 'Virgin Oak',     'Rickhouse-A', 0.0, 63.5), -- error in source? 200.0
('CSK-2005-004', 'Single Malt', '2005-03-15', 'Mizunara',       'Dunnage-B', 250.0, 63.5),
('CSK-2010-005', 'Blend',       '2010-11-20', 'American Oak',   'Rickhouse-C', 200.0, 63.5),
('CSK-2015-006', 'Single Malt', '2015-02-14', 'Spanish Sherry', 'Dunnage-B', 500.0, 63.5), -- Butt size
('CSK-2020-007', 'Peated',      '2020-01-01', 'Bourbon Ex',     'Rickhouse-A', 200.0, 63.5),
('CSK-2021-008', 'Peated',      '2021-06-01', 'Bourbon Ex',     'Rickhouse-A', 200.0, 63.5),
('CSK-2022-009', 'Single Malt', '2022-01-01', 'Wine Barrique',  'Dunnage-B', 225.0, 63.5),
('CSK-2023-010', 'Rye',         '2023-01-01', 'Virgin Oak',     'Rickhouse-C', 200.0, 62.0),
-- Filling more inventory...
('CSK-2012-011', 'Single Malt', '2012-05-01', 'American Oak', 'Rickhouse-A', 200.0, 63.5),
('CSK-2012-012', 'Single Malt', '2012-05-01', 'American Oak', 'Rickhouse-A', 200.0, 63.5),
('CSK-2012-013', 'Single Malt', '2012-05-01', 'American Oak', 'Rickhouse-A', 200.0, 63.5),
('CSK-2015-014', 'Grain',       '2015-08-01', 'Bourbon Ex',   'Rickhouse-C', 200.0, 64.0),
('CSK-2015-015', 'Grain',       '2015-08-01', 'Bourbon Ex',   'Rickhouse-C', 200.0, 64.0),
('CSK-2018-016', 'Single Malt', '2018-01-01', 'Port Pipe',    'Dunnage-B', 500.0, 63.5),
('CSK-2018-017', 'Single Malt', '2018-01-01', 'Port Pipe',    'Dunnage-B', 500.0, 63.5),
('CSK-2019-018', 'Rye',         '2019-04-01', 'Virgin Oak',   'Rickhouse-A', 200.0, 62.5),
('CSK-2019-019', 'Rye',         '2019-04-01', 'Virgin Oak',   'Rickhouse-A', 200.0, 62.5),
('CSK-2020-020', 'Peated',      '2020-10-31', 'Quarter Cask', 'Dunnage-B', 125.0, 63.5),
('CSK-2020-021', 'Peated',      '2020-10-31', 'Quarter Cask', 'Dunnage-B', 125.0, 63.5),
('CSK-2020-022', 'Peated',      '2020-10-31', 'Quarter Cask', 'Dunnage-B', 125.0, 63.5),
('CSK-2020-023', 'Peated',      '2020-10-31', 'Quarter Cask', 'Dunnage-B', 125.0, 63.5),
('CSK-2011-024', 'Single Malt', '2011-11-11', 'Oloroso',      'Dunnage-B', 500.0, 63.5),
('CSK-2011-025', 'Single Malt', '2011-11-11', 'PX Sherry',    'Dunnage-B', 500.0, 63.5),
('CSK-2008-026', 'Blend',       '2008-02-29', 'Refill',       'Rickhouse-C', 200.0, 63.0),
('CSK-2008-027', 'Blend',       '2008-02-29', 'Refill',       'Rickhouse-C', 200.0, 63.0),
('CSK-2008-028', 'Blend',       '2008-02-29', 'Refill',       'Rickhouse-C', 200.0, 63.0),
('CSK-2016-029', 'Single Malt', '2016-06-01', 'Madeira',      'Dunnage-B', 225.0, 63.5),
('CSK-2016-030', 'Single Malt', '2016-06-01', 'Madeira',      'Dunnage-B', 225.0, 63.5),
('CSK-2013-031', 'Corn',        '2013-07-04', 'American Oak', 'Rickhouse-C', 200.0, 65.0),
('CSK-2013-032', 'Corn',        '2013-07-04', 'American Oak', 'Rickhouse-C', 200.0, 65.0),
('CSK-2014-033', 'Single Malt', '2014-12-25', 'Cognac',       'Dunnage-B', 300.0, 63.5),
('CSK-2014-034', 'Single Malt', '2014-12-25', 'Cognac',       'Dunnage-B', 300.0, 63.5),
('CSK-2001-035', 'Single Malt', '2001-09-11', 'American Oak', 'Rickhouse-A', 200.0, 63.5),
('CSK-2024-036', 'New Make',    '2024-01-01', 'Virgin Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-2024-037', 'New Make',    '2024-01-02', 'Virgin Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-2024-038', 'New Make',    '2024-01-03', 'Virgin Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-2024-039', 'New Make',    '2024-01-04', 'Virgin Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-2024-040', 'New Make',    '2024-01-05', 'Virgin Oak',   'Rickhouse-A', 200.0, 63.5),
('CSK-1980-041', 'Single Malt', '1980-01-01', 'Sherry Butt',  'Dunnage-B', 500.0, 63.5), -- Rare!
('CSK-1985-042', 'Single Malt', '1985-01-01', 'Hogshead',     'Dunnage-B', 250.0, 63.5),
('CSK-1999-043', 'Single Malt', '1999-12-31', 'Millennium',   'Rickhouse-A', 200.0, 63.5),
('CSK-2017-044', 'Rye',         '2017-07-01', 'Rum Cask',     'Rickhouse-C', 200.0, 62.0),
('CSK-2017-045', 'Rye',         '2017-07-01', 'Rum Cask',     'Rickhouse-C', 200.0, 62.0),
('CSK-2009-046', 'Single Malt', '2009-03-17', 'Irish Oak',    'Dunnage-B', 200.0, 63.0),
('CSK-2004-047', 'Peated',      '2004-11-01', 'Islay Cask',   'Rickhouse-A', 200.0, 63.5),
('CSK-2006-048', 'Single Malt', '2006-05-05', 'Manzanilla',   'Dunnage-B', 500.0, 63.5),
('CSK-1992-049', 'Single Malt', '1992-02-29', 'Leap Year',    'Dunnage-B', 200.0, 63.5),
('CSK-2025-050', 'Future',      '2025-01-01', 'Space Wood',   'Lab-X',     200.0, 63.5);

-- Seed Maturation Checks
INSERT INTO Distillery_Ops.Sources.Maturation_Checks VALUES
('CHK-1001', 'CSK-1990-001', '2023-01-01', 110.0, 48.2, 'Rich dried fruits, leather', 95), -- High loss (angel's share)
('CHK-1002', 'CSK-1995-002', '2023-01-01', 130.0, 52.1, 'Vanilla bomb, coconut', 88),
('CHK-1003', 'CSK-2005-004', '2023-01-01', 180.0, 56.5, 'Spicy, sandalwood', 92),
('CHK-1004', 'CSK-2020-007', '2023-06-01', 190.0, 62.0, 'Smoke, young, harsh', 75),
('CHK-1005', 'CSK-2022-009', '2023-06-01', 220.0, 63.0, 'Red fruits, tannin', 80),
('CHK-1006', 'CSK-1980-041', '2023-01-01', 250.0, 42.5, 'Complex, rancio, old library', 98), -- 50% loss roughly
('CHK-1007', 'CSK-2015-006', '2023-02-01', 450.0, 59.0, 'Raisins, chocolate', 89),
('CHK-1008', 'CSK-2010-005', '2023-03-01', 160.0, 55.0, 'Toffee, grain', 82),
('CHK-1009', 'CSK-1990-001', '2010-01-01', 160.0, 55.0, 'Developing nicely', 85), -- Historic check
('CHK-1010', 'CSK-1990-001', '2020-01-01', 125.0, 50.0, 'Stunning depth', 92),
('CHK-1011', 'CSK-2000-003', '2023-01-01', 140.0, 54.0, 'Spicy rye, dill', 86), -- Correct volume from audit
('CHK-1012', 'CSK-2012-011', '2023-01-01', 170.0, 58.0, 'Standard bourbon profile', 84),
('CHK-1013', 'CSK-2018-016', '2023-01-01', 480.0, 61.0, 'Ruby color, sweet', 87),
('CHK-1014', 'CSK-2020-020', '2023-01-01', 115.0, 61.5, 'Intense peat, ash', 79), -- Quarter casks mature fast, evaporate fast
('CHK-1015', 'CSK-2011-024', '2023-01-01', 400.0, 57.0, 'Nutty, dry', 90),
('CHK-1016', 'CSK-2001-035', '2023-01-01', 135.0, 53.5, 'Floral, heather', 88),
('CHK-1017', 'CSK-2004-047', '2023-01-01', 145.0, 54.0, 'Medicinal, iodine', 91),
('CHK-1018', 'CSK-2014-033', '2023-01-01', 270.0, 60.0, 'Grape, floral', 85),
('CHK-1019', 'CSK-2016-029', '2023-01-01', 200.0, 61.0, 'Baked apple', 83),
('CHK-1020', 'CSK-2008-026', '2023-01-01', 150.0, 54.5, 'Balanced, smooth', 81);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Distillery_Ops.Bronze.Bronze_Inventory AS SELECT * FROM Distillery_Ops.Sources.Cask_Inventory;
CREATE OR REPLACE VIEW Distillery_Ops.Bronze.Bronze_Checks AS SELECT * FROM Distillery_Ops.Sources.Maturation_Checks;

-- 4b. SILVER LAYER (Calculated Evaporation / "Angel's Share")
CREATE OR REPLACE VIEW Distillery_Ops.Silver.Silver_Cask_Performance AS
SELECT
    c.CaskID,
    c.Spirit_Type,
    c.Cask_Wood_Type,
    c.Fill_Date,
    m.Check_Date,
    DATEDIFF(m.Check_Date, c.Fill_Date) / 365.25 as Age_Years,
    c.Original_Liters_Alcohol,
    m.Current_Volume_Liters,
    m.Current_ABV_Percent,
    -- Calculate Loss
    (c.Original_Liters_Alcohol - m.Current_Volume_Liters) as Loss_Liters,
    ROUND(((c.Original_Liters_Alcohol - m.Current_Volume_Liters) / NULLIF(c.Original_Liters_Alcohol, 0)) * 100, 2) as Loss_Pct,
    m.Master_Distiller_Score,
    m.Tasting_Notes
FROM Distillery_Ops.Bronze.Bronze_Inventory c
JOIN Distillery_Ops.Bronze.Bronze_Checks m ON c.CaskID = m.CaskID
-- Get only the latest check per cask logic would go here in a Gold view usually, 
-- but here we list all history.
;

-- 4c. GOLD LAYER (Selection Candidates for Bottling)
CREATE OR REPLACE VIEW Distillery_Ops.Gold.Gold_Bottling_Candidates AS
SELECT
    CaskID,
    Spirit_Type,
    Age_Years,
    Current_ABV_Percent,
    Master_Distiller_Score,
    Tasting_Notes,
    -- Logic: Rare Release if > 25 Years and Score > 90
    CASE 
        WHEN Age_Years >= 25 AND Master_Distiller_Score >= 95 THEN 'Platinum Series'
        WHEN Age_Years >= 18 AND Master_Distiller_Score >= 90 THEN 'Gold Series'
        WHEN Master_Distiller_Score >= 85 THEN 'Single Cask Release'
        ELSE 'Blend Component'
    END as Bottling_Tier
FROM Distillery_Ops.Silver.Silver_Cask_Performance
WHERE Current_ABV_Percent > 40.0 -- Must be legal whisky
QUALIFY ROW_NUMBER() OVER (PARTITION BY CaskID ORDER BY Check_Date DESC) = 1;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "Analyze 'Silver_Cask_Performance' to find the average 'Loss_Pct' per year for 'Spanish Sherry' vs 'American Oak'. 
 * Does the Warehouse_Location affect evaporation rates?"
 */
