/*
 * Dremio Rare Antiquarian Books Example
 * 
 * Domain: High-End Retail & Auctions
 * Scenario: 
 * An auction house tracks rare books.
 * We record "Binding Condition" (e.g., Foxing, Worming), "Provenance Chain" (Previous Owners),
 * and "Hammer Price".
 * The goal is to estimate value based on condition and rarity.
 * 
 * Complexity: Medium (Text parsing concepts, condition multipliers)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Bibliophile_Auctions;
CREATE FOLDER IF NOT EXISTS Bibliophile_Auctions.Sources;
CREATE FOLDER IF NOT EXISTS Bibliophile_Auctions.Bronze;
CREATE FOLDER IF NOT EXISTS Bibliophile_Auctions.Silver;
CREATE FOLDER IF NOT EXISTS Bibliophile_Auctions.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Bibliophile_Auctions.Sources.Catalog (
    BookID VARCHAR,
    Title VARCHAR,
    Author VARCHAR,
    Year_Published INT,
    Edition VARCHAR -- '1st', '2nd', 'Limited'
);

CREATE TABLE IF NOT EXISTS Bibliophile_Auctions.Sources.Auction_Results (
    LotID VARCHAR,
    BookID VARCHAR,
    Auction_Date DATE,
    Condition_Notes VARCHAR, -- 'Fine', 'Foxing', 'Detached Board'
    Provenance_Count INT, -- Number of notable previous owners
    Hammer_Price_USD DOUBLE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Books
INSERT INTO Bibliophile_Auctions.Sources.Catalog VALUES
('BK-001', 'The Great Gatsby', 'F. Scott Fitzgerald', 1925, '1st'),
('BK-002', 'The Hobbit', 'J.R.R. Tolkien', 1937, '1st'),
('BK-003', 'Ulysses', 'James Joyce', 1922, '1st');

-- Seed Results
INSERT INTO Bibliophile_Auctions.Sources.Auction_Results VALUES
('LOT-001', 'BK-001', '2023-01-01', 'Fine with Jacket', 3, 150000.0), -- Jacket makes the price
('LOT-002', 'BK-001', '2023-02-01', 'Good No Jacket', 1, 5000.0), -- Huge drop
('LOT-003', 'BK-002', '2023-01-15', 'Fine', 2, 50000.0),
('LOT-004', 'BK-003', '2023-03-01', 'Signed', 5, 200000.0),
('LOT-005', 'BK-002', '2023-04-01', 'Worming', 0, 1000.0),
-- Fill 50
('LOT-006', 'BK-001', '2023-05-01', 'Good', 1, 4000.0),
('LOT-007', 'BK-001', '2023-05-02', 'Good', 1, 4200.0),
('LOT-008', 'BK-001', '2023-05-03', 'Good', 1, 4100.0),
('LOT-009', 'BK-001', '2023-05-04', 'Fair', 0, 2000.0),
('LOT-010', 'BK-001', '2023-05-05', 'Poor', 0, 500.0),
('LOT-011', 'BK-002', '2023-05-06', 'Fine', 2, 55000.0),
('LOT-012', 'BK-002', '2023-05-07', 'Fine', 2, 52000.0),
('LOT-013', 'BK-002', '2023-05-08', 'Good', 1, 10000.0),
('LOT-014', 'BK-002', '2023-05-09', 'Good', 1, 11000.0),
('LOT-015', 'BK-002', '2023-05-10', 'Fair', 0, 3000.0),
('LOT-016', 'BK-003', '2023-05-11', 'Signed', 4, 180000.0),
('LOT-017', 'BK-003', '2023-05-12', 'Signed', 4, 190000.0),
('LOT-018', 'BK-003', '2023-05-13', 'Rebound', 1, 20000.0), -- Rebinding hurts value usually
('LOT-019', 'BK-003', '2023-05-14', 'Rebound', 1, 22000.0),
('LOT-020', 'BK-003', '2023-05-15', 'Poor', 0, 5000.0),
('LOT-021', 'BK-001', '2023-06-01', 'Fine with Jacket', 3, 160000.0),
('LOT-022', 'BK-001', '2023-06-02', 'Fine No Jacket', 2, 20000.0),
('LOT-023', 'BK-001', '2023-06-03', 'Fine No Jacket', 2, 21000.0),
('LOT-024', 'BK-001', '2023-06-04', 'Foxing', 1, 3000.0),
('LOT-025', 'BK-001', '2023-06-05', 'Foxing', 1, 2800.0),
('LOT-026', 'BK-002', '2023-06-06', 'Fine', 3, 60000.0),
('LOT-027', 'BK-002', '2023-06-07', 'Fine', 3, 61000.0),
('LOT-028', 'BK-002', '2023-06-08', 'Good', 2, 12000.0),
('LOT-029', 'BK-002', '2023-06-09', 'Good', 2, 12500.0),
('LOT-030', 'BK-002', '2023-06-10', 'Fair', 1, 4000.0),
('LOT-031', 'BK-003', '2023-06-11', 'Original Wraps', 5, 250000.0), -- Holy grail
('LOT-032', 'BK-003', '2023-06-12', 'Original Wraps', 5, 240000.0),
('LOT-033', 'BK-003', '2023-06-13', 'Rebound', 2, 25000.0),
('LOT-034', 'BK-003', '2023-06-14', 'Rebound', 2, 26000.0),
('LOT-035', 'BK-003', '2023-06-15', 'Poor', 1, 6000.0),
('LOT-036', 'BK-001', '2023-07-01', 'Good No Jacket', 1, 5500.0),
('LOT-037', 'BK-001', '2023-07-02', 'Good No Jacket', 1, 5600.0),
('LOT-038', 'BK-001', '2023-07-03', 'Good No Jacket', 1, 5400.0),
('LOT-039', 'BK-001', '2023-07-04', 'Good No Jacket', 1, 5200.0),
('LOT-040', 'BK-001', '2023-07-05', 'Good No Jacket', 1, 5300.0),
('LOT-041', 'BK-002', '2023-07-06', 'Good', 1, 11500.0),
('LOT-042', 'BK-002', '2023-07-07', 'Good', 1, 11600.0),
('LOT-043', 'BK-002', '2023-07-08', 'Good', 1, 11700.0),
('LOT-044', 'BK-002', '2023-07-09', 'Good', 1, 11800.0),
('LOT-045', 'BK-002', '2023-07-10', 'Good', 1, 11900.0),
('LOT-046', 'BK-003', '2023-07-11', 'Good', 3, 30000.0),
('LOT-047', 'BK-003', '2023-07-12', 'Good', 3, 31000.0),
('LOT-048', 'BK-003', '2023-07-13', 'Good', 3, 32000.0),
('LOT-049', 'BK-003', '2023-07-14', 'Good', 3, 33000.0),
('LOT-050', 'BK-003', '2023-07-15', 'Good', 3, 34000.0);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Bibliophile_Auctions.Bronze.Bronze_Catalog AS SELECT * FROM Bibliophile_Auctions.Sources.Catalog;
CREATE OR REPLACE VIEW Bibliophile_Auctions.Bronze.Bronze_Results AS SELECT * FROM Bibliophile_Auctions.Sources.Auction_Results;

-- 4b. SILVER LAYER (Valuation Logic)
CREATE OR REPLACE VIEW Bibliophile_Auctions.Silver.Silver_Valuations AS
SELECT
    r.LotID,
    c.Title,
    c.Author,
    c.Edition,
    r.Condition_Notes,
    r.Provenance_Count,
    r.Hammer_Price_USD,
    -- Simple categorization of condition affect
    CASE 
        WHEN r.Condition_Notes LIKE '%Fine%' THEN 'High Grade'
        WHEN r.Condition_Notes LIKE '%Good%' THEN 'Mid Grade'
        ELSE 'Low Grade'
    END as Grade_Bucket
FROM Bibliophile_Auctions.Bronze.Bronze_Results r
JOIN Bibliophile_Auctions.Bronze.Bronze_Catalog c ON r.BookID = c.BookID;

-- 4c. GOLD LAYER (Reference Pricing)
CREATE OR REPLACE VIEW Bibliophile_Auctions.Gold.Gold_Price_Guidance AS
SELECT
    Title,
    Edition,
    Grade_Bucket,
    COUNT(*) as Sales_Count,
    MIN(Hammer_Price_USD) as Low_Est,
    MAX(Hammer_Price_USD) as High_Est,
    AVG(Hammer_Price_USD) as Market_Avg
FROM Bibliophile_Auctions.Silver.Silver_Valuations
GROUP BY Title, Edition, Grade_Bucket
ORDER BY Title, Market_Avg DESC;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "From 'Gold_Price_Guidance', calculate the price multiplier between 'High Grade' and 'Low Grade' copies of 'The Great Gatsby'. 
 * This shows the premium paid for condition."
 */
