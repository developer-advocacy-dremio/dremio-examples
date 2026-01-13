/*
 * Event Ticketing Sales Demo
 * 
 * Scenario:
 * Tracking ticket sales velocity, dynamic pricing, and seat inventory.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Maximize sell-through rate and revenue per seat.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS TicketingDB;
CREATE FOLDER IF NOT EXISTS TicketingDB.Bronze;
CREATE FOLDER IF NOT EXISTS TicketingDB.Silver;
CREATE FOLDER IF NOT EXISTS TicketingDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS TicketingDB.Bronze.Events (
    EventID INT,
    Name VARCHAR,
    Venue VARCHAR,
    EventDate DATE,
    TotalCapacity INT
);

CREATE TABLE IF NOT EXISTS TicketingDB.Bronze.TicketSales (
    TicketID INT,
    EventID INT,
    Section VARCHAR,
    PricePaid DOUBLE,
    SaleDate TIMESTAMP,
    CustomerRegion VARCHAR
);

INSERT INTO TicketingDB.Bronze.Events VALUES
(1, 'Rock Concert', 'Stadium A', '2025-09-01', 50000),
(2, 'Comedy Special', 'Theater B', '2025-09-05', 2000);

INSERT INTO TicketingDB.Bronze.TicketSales VALUES
(101, 1, 'Floor', 150.0, '2025-06-01 10:00:00', 'Metro'),
(102, 1, 'Stand', 80.0, '2025-06-01 10:05:00', 'Metro'),
(103, 1, 'Floor', 160.0, '2025-06-02 09:00:00', 'Suburbs'), -- Dynamic pricing up
(104, 2, 'Balcony', 45.0, '2025-07-01 12:00:00', 'Metro');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TicketingDB.Silver.DailySalesVelocity AS
SELECT 
    EventID,
    CAST(SaleDate AS DATE) AS SaleDay,
    COUNT(TicketID) AS TicketsSold,
    SUM(PricePaid) AS Revenue,
    AVG(PricePaid) AS AvgPrice
FROM TicketingDB.Bronze.TicketSales
GROUP BY EventID, CAST(SaleDate AS DATE);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW TicketingDB.Gold.SellThroughRate AS
SELECT 
    e.Name,
    e.Venue,
    e.TotalCapacity,
    SUM(d.TicketsSold) AS TotalSold,
    (SUM(d.TicketsSold) / e.TotalCapacity) * 100 AS PercentSold
FROM TicketingDB.Bronze.Events e
LEFT JOIN TicketingDB.Silver.DailySalesVelocity d ON e.EventID = d.EventID
GROUP BY e.Name, e.Venue, e.TotalCapacity;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which event has the highest PercentSold in TicketingDB.Gold.SellThroughRate?"
*/
