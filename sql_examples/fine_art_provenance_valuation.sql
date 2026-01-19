/*
 * Fine Art Provenance & Valuation Demo
 * 
 * Scenario:
 * An art advisory firm wants to track the provenance (ownership history) of artworks
 * and estimate their current market value based on auction results.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Artworks: Catalog of pieces (Title, Artist, Year, Medium).
 * - Auctions: Historical sales data (AuctionHouse, Price, Date).
 * - Provenance_Log: Chain of custody records (Owner, DateAcquired).
 * 
 * Silver Layer:
 * - Artwork_Valuation: Joins artworks with their most recent auction price.
 * - Ownership_Chain: Aggregates provenance to show current owner.
 * 
 * Gold Layer:
 * - Portfolio_Value: Total estimated value by Current Owner.
 * - Market_Trends: Average price trends by Artist and Year.
 * 
 * Note: Assumes a catalog named 'ArtDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS ArtDB;
CREATE FOLDER IF NOT EXISTS ArtDB.Bronze;
CREATE FOLDER IF NOT EXISTS ArtDB.Silver;
CREATE FOLDER IF NOT EXISTS ArtDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS ArtDB.Bronze.Artworks (
    ArtworkID INT,
    Title VARCHAR,
    Artist VARCHAR,
    YearCreated INT,
    Medium VARCHAR,
    Dimensions VARCHAR
);

CREATE TABLE IF NOT EXISTS ArtDB.Bronze.Auctions (
    AuctionID INT,
    ArtworkID INT,
    AuctionHouse VARCHAR,
    SaleDate DATE,
    HammerPriceUSD DOUBLE,
    BuyerID INT
);

CREATE TABLE IF NOT EXISTS ArtDB.Bronze.Provenance_Log (
    LogID INT,
    ArtworkID INT,
    OwnerName VARCHAR,
    AcquisitionDate DATE,
    DeaccessionDate DATE,
    Method VARCHAR -- Purchase, Inheritance, Gift
);

-- 1.2 Populate Bronze Tables

-- Insert 50 records into ArtDB.Bronze.Artworks
INSERT INTO ArtDB.Bronze.Artworks (ArtworkID, Title, Artist, YearCreated, Medium, Dimensions) VALUES
(1, 'Sunset over Seine', 'Monet', 1890, 'Oil on Canvas', '60x80cm'),
(2, 'Blue Dancer', 'Degas', 1895, 'Pastel', '50x40cm'),
(3, 'Abstract No. 5', 'Pollock', 1950, 'Enamel', '200x150cm'),
(4, 'The Old Guitarist Study', 'Picasso', 1903, 'Oil', '40x30cm'),
(5, 'Red Square', 'Malevich', 1915, 'Oil on Canvas', '60x60cm'),
(6, 'Campbell Soup Can II', 'Warhol', 1968, 'Screenprint', '90x60cm'),
(7, 'Girl with Pearl Earring Study', 'Vermeer', 1665, 'Oil', '20x15cm'),
(8, 'Starry Night Sketch', 'Van Gogh', 1889, 'Charcoal', '30x40cm'),
(9, 'Water Lilies Ref', 'Monet', 1915, 'Oil', '100x200cm'),
(10, 'Composition VIII', 'Kandinsky', 1923, 'Oil', '140x201cm'),
(11, 'Self Portrait', 'Frida Kahlo', 1940, 'Oil', '60x40cm'),
(12, 'The Kiss', 'Klimt', 1907, 'Oil and Gold Leaf', '180x180cm'),
(13, 'Balloon Dog (Blue)', 'Koons', 1994, 'Stainless Steel', '300x300cm'),
(14, 'Untitled (Skull)', 'Basquiat', 1981, 'Acrylic', '150x150cm'),
(15, 'Shot Sage Blue Marilyn', 'Warhol', 1964, 'Screenprint', '100x100cm'),
(16, 'Nighthawks Study', 'Hopper', 1942, 'Pencil', '20x30cm'),
(17, 'The Scream Lithograph', 'Munch', 1895, 'Lithograph', '50x40cm'),
(18, 'Guernica Study', 'Picasso', 1937, 'Pencil', '50x100cm'),
(19, 'Persistence of Memory', 'Dali', 1931, 'Oil', '24x33cm'),
(20, 'Son of Man', 'Magritte', 1964, 'Oil', '116x89cm'),
(21, 'American Gothic', 'Wood', 1930, 'Oil', '78x65cm'),
(22, 'Flower Bomber', 'Banksy', 2003, 'Stencil', 'Size Var'),
(23, 'Girl with Balloon', 'Banksy', 2002, 'Stencil', 'Size Var'),
(24, 'Video Installation 1', 'Nam June Paik', 1980, 'Mixed Media', 'Variable'),
(25, 'Infinity Room', 'Kusama', 2010, 'Installation', 'Room'),
(26, 'Mobile', 'Calder', 1950, 'Metal', 'Hanging'),
(27, 'Spider', 'Louise Bourgeois', 1996, 'Bronze', 'Large'),
(28, 'Cloud Gate Model', 'Anish Kapoor', 2004, 'Steel', 'Small'),
(29, 'Physical Impossibility', 'Hirst', 1991, 'Formaldehyde', 'Tank'),
(30, 'Pumpkin', 'Kusama', 2000, 'Sculpture', '100x100cm'),
(31, 'Untitled 1', 'Rothko', 1960, 'Oil', '200x150cm'),
(32, 'Untitled 2', 'Rothko', 1961, 'Oil', '200x150cm'),
(33, 'Number 1', 'Pollock', 1949, 'Enamel', '150x150cm'),
(34, 'Three Studies', 'Bacon', 1969, 'Oil', 'Triptych'),
(35, 'Woman III', 'De Kooning', 1953, 'Oil', '170x120cm'),
(36, 'Dora Maar', 'Picasso', 1941, 'Oil', '80x60cm'),
(37, 'White Center', 'Rothko', 1950, 'Oil', '200x150cm'),
(38, 'Green Car Crash', 'Warhol', 1963, 'Silkscreen', 'Large'),
(39, 'Flag', 'Jasper Johns', 1954, 'Encaustic', '100x150cm'),
(40, 'False Start', 'Jasper Johns', 1959, 'Oil', '150x120cm'),
(41, 'Nurse', 'Lichtenstein', 1964, 'Oil/Magna', '100x100cm'),
(42, 'Masterpiece', 'Lichtenstein', 1962, 'Oil', '120x120cm'),
(43, 'Portrait of Artist', 'Hockney', 1972, 'Acrylic', '200x300cm'),
(44, 'Rabbit', 'Koons', 1986, 'Steel', '100cm'),
(45, 'Jimson Weed', 'O Keeffe', 1936, 'Oil', 'Large'),
(46, 'Propped', 'Saville', 1992, 'Oil', 'Large'),
(47, 'Spider IV', 'Bourgeois', 1997, 'Bronze', 'Small'),
(48, 'Dog', 'Giacometti', 1951, 'Bronze', 'Thin'),
(49, 'Walking Man', 'Giacometti', 1960, 'Bronze', 'Life size'),
(50, 'Tete', 'Modigliani', 1910, 'Stone', 'Head');

-- Insert 50 records into ArtDB.Bronze.Auctions
INSERT INTO ArtDB.Bronze.Auctions (AuctionID, ArtworkID, AuctionHouse, SaleDate, HammerPriceUSD, BuyerID) VALUES
(1, 1, 'Sothebys', '2020-05-15', 5000000.0, 101),
(2, 2, 'Christies', '2019-11-20', 3500000.0, 102),
(3, 3, 'Phillips', '2021-03-10', 12000000.0, 103),
(4, 4, 'Sothebys', '2018-02-14', 800000.0, 101),
(5, 6, 'Christies', '2022-05-10', 1500000.0, 104),
(6, 13, 'Sothebys', '2023-01-20', 10000000.0, 105),
(7, 14, 'Phillips', '2021-06-15', 25000000.0, 106),
(8, 22, 'Sothebys', '2024-10-05', 500000.0, 107),
(9, 31, 'Christies', '2020-10-10', 45000000.0, 108),
(10, 43, 'Christies', '2018-11-15', 90000000.0, 109),
(11, 44, 'Christies', '2019-05-15', 91000000.0, 110),
(12, 5, 'Sothebys', '2015-02-01', 2000000.0, 111),
(13, 7, 'Sothebys', '2010-01-01', 100000.0, 112),
(14, 8, 'Phillips', '2012-05-05', 500000.0, 113),
(15, 9, 'Christies', '2015-11-10', 8000000.0, 114),
(16, 10, 'Sothebys', '2016-06-20', 15000000.0, 115),
(17, 11, 'Phillips', '2017-09-12', 4000000.0, 116),
(18, 12, 'Christies', '2013-03-14', 100000000.0, 117), -- Private sale logic usually, but here auction
(19, 15, 'Christies', '2022-05-09', 195000000.0, 118),
(20, 16, 'Sothebys', '2011-04-04', 300000.0, 119),
(21, 17, 'Sothebys', '2012-05-02', 119900000.0, 120),
(22, 18, 'Phillips', '2014-10-10', 900000.0, 121),
(23, 19, 'Sothebys', '2010-02-02', 2000000.0, 122),
(24, 20, 'Christies', '2015-05-05', 4000000.0, 123),
(25, 23, 'Sothebys', '2018-10-05', 1400000.0, 124), -- The shredding event
(26, 25, 'Phillips', '2019-11-11', 2000000.0, 125),
(27, 26, 'Christies', '2016-02-14', 3000000.0, 126),
(28, 27, 'Sothebys', '2021-04-20', 10000000.0, 127),
(29, 29, 'Christies', '2017-05-05', 6000000.0, 128),
(30, 30, 'Phillips', '2020-09-09', 1500000.0, 129),
(31, 32, 'Sothebys', '2019-11-14', 30000000.0, 130),
(32, 33, 'Christies', '2018-11-13', 25000000.0, 131),
(33, 34, 'Christies', '2013-11-12', 142400000.0, 132),
(34, 35, 'Priv', '2006-11-01', 137500000.0, 133), -- Private sale via gallery treated as auction event
(35, 36, 'Sothebys', '2006-05-03', 95200000.0, 134),
(36, 37, 'Sothebys', '2007-05-15', 72800000.0, 135),
(37, 38, 'Christies', '2007-05-16', 71700000.0, 136),
(38, 39, 'Priv', '2010-01-01', 110000000.0, 137),
(39, 40, 'Priv', '2006-10-12', 80000000.0, 138),
(40, 41, 'Christies', '2015-11-09', 95300000.0, 139),
(41, 42, 'Priv', '2017-01-01', 165000000.0, 140),
(42, 45, 'Sothebys', '2014-11-20', 44400000.0, 141),
(43, 46, 'Sothebys', '2018-10-05', 12400000.0, 142),
(44, 47, 'Christies', '2022-04-01', 5000000.0, 143),
(45, 48, 'Sothebys', '2023-05-05', 8000000.0, 144),
(46, 49, 'Sothebys', '2010-02-03', 104300000.0, 145),
(47, 50, 'Christies', '2014-11-04', 70700000.0, 146),
(48, 1, 'Christies', '2024-01-01', 5500000.0, 147), -- Resale of item 1
(49, 2, 'Sothebys', '2024-02-01', 3800000.0, 148), -- Resale of item 2
(50, 3, 'Phillips', '2024-03-01', 12500000.0, 149); -- Resale of item 3

-- Insert 50 records into ArtDB.Bronze.Provenance_Log
INSERT INTO ArtDB.Bronze.Provenance_Log (LogID, ArtworkID, OwnerName, AcquisitionDate, DeaccessionDate, Method) VALUES
(1, 1, 'Dealer A', '1890-01-01', '1950-01-01', 'Direct from Artist'),
(2, 1, 'Collector X', '1950-01-01', '2020-05-15', 'Purchase'),
(3, 1, 'Buyer 101', '2020-05-15', '2024-01-01', 'Auction'),
(4, 1, 'Buyer 147', '2024-01-01', NULL, 'Auction'), -- Current Owner
(5, 2, 'Degas Estate', '1895-01-01', '1918-05-01', 'Creation'),
(6, 2, 'Collector Y', '1918-05-01', '2019-11-20', 'Purchase'),
(7, 2, 'Buyer 102', '2019-11-20', '2024-02-01', 'Auction'),
(8, 2, 'Buyer 148', '2024-02-01', NULL, 'Auction'),
(9, 3, 'Gallery Z', '1950-06-01', '2021-03-10', 'Consignment'),
(10, 3, 'Buyer 103', '2021-03-10', '2024-03-01', 'Auction'),
(11, 3, 'Buyer 149', '2024-03-01', NULL, 'Auction'),
(12, 13, 'Gagosian', '1994-01-01', '2023-01-20', 'Exhibition'),
(13, 13, 'Buyer 105', '2023-01-20', NULL, 'Auction'),
(14, 14, 'Annina Nosei', '1981-05-01', '2021-06-15', 'Gallery'),
(15, 14, 'Buyer 106', '2021-06-15', NULL, 'Auction'),
(16, 30, 'Ota Fine Arts', '2000-01-01', '2020-09-09', 'Gallery'),
(17, 30, 'Buyer 129', '2020-09-09', NULL, 'Auction'),
(18, 44, 'Sonnabend', '1986-01-01', '2019-05-15', 'Collection'),
(19, 44, 'Buyer 110', '2019-05-15', NULL, 'Auction'),
(20, 15, 'Factory', '1964-01-01', '2022-05-09', 'Studio'),
(21, 15, 'Buyer 118', '2022-05-09', NULL, 'Auction'),
(22, 17, 'Munch Museum', '1895-01-01', '2012-05-02', 'Deaccession'),
(23, 17, 'Buyer 120', '2012-05-02', NULL, 'Auction'),
(24, 43, 'Hockney Studio', '1972-01-01', '2018-11-15', 'Studio'),
(25, 43, 'Buyer 109', '2018-11-15', NULL, 'Auction'),
(26, 33, 'Peggy Guggenheim', '1949-01-01', '2013-11-12', 'Gift'),
(27, 33, 'Buyer 132', '2013-11-12', NULL, 'Auction'),
(28, 49, 'Maeght', '1960-01-01', '2010-02-03', 'Gallery'),
(29, 49, 'Buyer 145', '2010-02-03', NULL, 'Auction'),
(30, 4, 'Picasso Estate', '1903-01-01', '2018-02-14', 'Inheritance'),
(31, 4, 'Buyer 101', '2018-02-14', NULL, 'Auction'),
(32, 23, 'Banksy', '2002-01-01', '2018-10-05', 'Street'),
(33, 23, 'Buyer 124', '2018-10-05', NULL, 'Auction'),
(34, 40, 'Castelli', '1959-01-01', '2006-10-12', 'Gallery'),
(35, 40, 'Buyer 138', '2006-10-12', NULL, 'Purchase'),
(36, 42, 'Castelli', '1962-01-01', '2017-01-01', 'Gallery'),
(37, 42, 'Buyer 140', '2017-01-01', NULL, 'Purchase'),
(38, 45, 'Steiglitz', '1936-01-01', '2014-11-20', 'Estate'),
(39, 45, 'Buyer 141', '2014-11-20', NULL, 'Auction'),
(40, 5, 'Malevich Estate', '1915-01-01', '2015-02-01', 'Inheritance'),
(41, 5, 'Buyer 111', '2015-02-01', NULL, 'Auction'),
(42, 6, 'Leo Castelli', '1968-01-01', '2022-05-10', 'Gallery'),
(43, 6, 'Buyer 104', '2022-05-10', NULL, 'Auction'),
(44, 22, 'Banksy', '2003-01-01', '2024-10-05', 'Studio'),
(45, 22, 'Buyer 107', '2024-10-05', NULL, 'Auction'),
(46, 31, 'Marlborough', '1960-01-01', '2020-10-10', 'Consignment'),
(47, 31, 'Buyer 108', '2020-10-10', NULL, 'Auction'),
(48, 7, 'Collection Q', '1900-01-01', '2010-01-01', 'Unknown'),
(49, 7, 'Buyer 112', '2010-01-01', NULL, 'Auction'),
(50, 8, 'Collection R', '1950-01-01', '2012-05-05', 'Gift');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Valuation & Chain of Custody
-------------------------------------------------------------------------------

-- 2.1 Artwork Valuation Metrics
-- Latest Price and CAGR based on Year Created (Simplified)
CREATE OR REPLACE VIEW ArtDB.Silver.Artwork_Valuation AS
SELECT
    a.ArtworkID,
    a.Title,
    a.Artist,
    a.YearCreated,
    auc.HammerPriceUSD AS LastPrice,
    auc.SaleDate AS LastSaleDate,
    auc.AuctionHouse
FROM ArtDB.Bronze.Artworks a
JOIN (
    -- Get most recent auction per artwork
    SELECT *, ROW_NUMBER() OVER(PARTITION BY ArtworkID ORDER BY SaleDate DESC) as rn
    FROM ArtDB.Bronze.Auctions
) auc ON a.ArtworkID = auc.ArtworkID
WHERE auc.rn = 1;

-- 2.2 Current Ownership
CREATE OR REPLACE VIEW ArtDB.Silver.Current_Ownership AS
SELECT
    l.ArtworkID,
    a.Title,
    l.OwnerName,
    l.AcquisitionDate
FROM ArtDB.Bronze.Provenance_Log l
JOIN ArtDB.Bronze.Artworks a ON l.ArtworkID = a.ArtworkID
WHERE l.DeaccessionDate IS NULL;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Market Insights
-------------------------------------------------------------------------------

-- 3.1 Portfolio Value by Owner (implied from last known sale)
CREATE OR REPLACE VIEW ArtDB.Gold.Portfolio_Value AS
SELECT
    co.OwnerName,
    COUNT(co.ArtworkID) AS TotalPieces,
    SUM(av.LastPrice) AS TotalHoldingsValue
FROM ArtDB.Silver.Current_Ownership co
JOIN ArtDB.Silver.Artwork_Valuation av ON co.ArtworkID = av.ArtworkID
GROUP BY co.OwnerName;

-- 3.2 Artist Market Rank
CREATE OR REPLACE VIEW ArtDB.Gold.Artist_Market_Rank AS
SELECT
    Artist,
    AVG(LastPrice) AS AvgSalePrice,
    MAX(LastPrice) AS RecordPrice,
    COUNT(ArtworkID) AS PiecesSold
FROM ArtDB.Silver.Artwork_Valuation
GROUP BY Artist;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (High Value):
"Who are the top 3 owners by TotalHoldingsValue in ArtDB.Gold.Portfolio_Value?"

PROMPT 2 (Artist Trends):
"What is the average sale price for 'Warhol' artworks in ArtDB.Gold.Artist_Market_Rank?"

PROMPT 3 (Provenance):
"Show the current owner of 'Sunset over Seine' from ArtDB.Silver.Current_Ownership."
*/
