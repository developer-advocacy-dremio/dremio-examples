/*
 * Online Auction Analytics Demo
 * 
 * Scenario:
 * Analyzing bidding velocity, ending prices, and seller performance.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Detect fraud (shill bidding) and optimize listing times.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS AuctionDB;
CREATE FOLDER IF NOT EXISTS AuctionDB.Bronze;
CREATE FOLDER IF NOT EXISTS AuctionDB.Silver;
CREATE FOLDER IF NOT EXISTS AuctionDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AuctionDB.Bronze.Listings (
    ListingID INT,
    SellerID INT,
    ItemTitle VARCHAR,
    StartPrice DOUBLE,
    StartTime TIMESTAMP,
    EndTime TIMESTAMP
);

CREATE TABLE IF NOT EXISTS AuctionDB.Bronze.Bids (
    BidID INT,
    ListingID INT,
    BidderID INT,
    BidAmount DOUBLE,
    BidTime TIMESTAMP
);

INSERT INTO AuctionDB.Bronze.Listings VALUES
(1, 101, 'Rare Stamp', 10.00, '2025-01-01 09:00:00', '2025-01-08 09:00:00');

INSERT INTO AuctionDB.Bronze.Bids VALUES
(1, 1, 500, 15.00, '2025-01-02 10:00:00'),
(2, 1, 501, 20.00, '2025-01-07 10:00:00'),
(3, 1, 502, 50.00, '2025-01-08 08:59:00'), -- Snipe
(4, 1, 500, 55.00, '2025-01-08 08:59:50'); -- Snipe war

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AuctionDB.Silver.BidVelocity AS
SELECT 
    l.ListingID,
    l.ItemTitle,
    l.EndTime,
    COUNT(b.BidID) AS TotalBids,
    MAX(b.BidAmount) AS CurrentPrice,
    SUM(CASE WHEN b.BidTime >= TIMESTAMPADD(MINUTE, -5, l.EndTime) THEN 1 ELSE 0 END) AS LastMinuteBids
FROM AuctionDB.Bronze.Listings l
LEFT JOIN AuctionDB.Bronze.Bids b ON l.ListingID = b.ListingID
GROUP BY l.ListingID, l.ItemTitle, l.EndTime;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AuctionDB.Gold.AuctionPerformance AS
SELECT 
    ItemTitle,
    CurrentPrice,
    TotalBids,
    LastMinuteBids,
    CASE 
        WHEN LastMinuteBids > 5 THEN 'SnipeWar' 
        ELSE 'Normal' 
    END AS ClosingType
FROM AuctionDB.Silver.BidVelocity;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Identify any 'SnipeWar' auctions in AuctionDB.Gold.AuctionPerformance."
*/
