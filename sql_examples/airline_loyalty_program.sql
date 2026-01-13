/*
 * Airline Loyalty Program Demo
 * 
 * Scenario:
 * Analyzing frequent flyer mileage accrual, redemption patterns, and elite status tier progression.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize reward inventory and reduce liability from unredeemed miles.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS AirlineDB;
CREATE FOLDER IF NOT EXISTS AirlineDB.Bronze;
CREATE FOLDER IF NOT EXISTS AirlineDB.Silver;
CREATE FOLDER IF NOT EXISTS AirlineDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS AirlineDB.Bronze.Members (
    MemberID INT,
    Name VARCHAR,
    JoinDate DATE,
    HomeAirport VARCHAR,
    CurrentTier VARCHAR -- 'Member', 'Silver', 'Gold', 'Platinum'
);

CREATE TABLE IF NOT EXISTS AirlineDB.Bronze.Flights (
    FlightID INT,
    MemberID INT,
    FlightDate DATE,
    Origin VARCHAR,
    Destination VARCHAR,
    BaseMilesEarned INT,
    BonusMilesEarned INT
);

CREATE TABLE IF NOT EXISTS AirlineDB.Bronze.Redemptions (
    RedemptionID INT,
    MemberID INT,
    RedeemDate DATE,
    Type VARCHAR, -- 'Flight', 'Upgrade', 'Partner'
    MilesCost INT
);

INSERT INTO AirlineDB.Bronze.Members VALUES
(1, 'Alice Flyer', '2020-01-15', 'JFK', 'Platinum'),
(2, 'Bob Traveler', '2022-03-10', 'LHR', 'Gold'),
(3, 'Charlie Trip', '2023-06-20', 'LAX', 'Member'),
(4, 'Diana Jet', '2019-11-05', 'DXB', 'Silver');

INSERT INTO AirlineDB.Bronze.Flights VALUES
(101, 1, '2025-01-10', 'JFK', 'LHR', 3450, 3450), -- 100% bonus for Plat
(102, 1, '2025-01-20', 'LHR', 'JFK', 3450, 3450),
(103, 2, '2025-02-05', 'LHR', 'DXB', 3400, 1700), -- 50% bonus for Gold
(104, 3, '2025-03-12', 'LAX', 'NRT', 5450, 0),
(105, 4, '2025-04-01', 'DXB', 'SIN', 3600, 900); -- 25% bonus for Silver

INSERT INTO AirlineDB.Bronze.Redemptions VALUES
(1, 1, '2025-05-01', 'Flight', 50000), -- Business Class Upgrade
(2, 2, '2025-04-15', 'Partner', 5000); -- Hotel Booking

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AirlineDB.Silver.MemberActivity AS
SELECT 
    m.MemberID,
    m.Name,
    m.CurrentTier,
    COALESCE(SUM(f.BaseMilesEarned + f.BonusMilesEarned), 0) AS TotalMilesEarned,
    COALESCE(SUM(r.MilesCost), 0) AS TotalMilesRedeemed,
    (COALESCE(SUM(f.BaseMilesEarned + f.BonusMilesEarned), 0) - COALESCE(SUM(r.MilesCost), 0)) AS CurrentBalance
FROM AirlineDB.Bronze.Members m
LEFT JOIN AirlineDB.Bronze.Flights f ON m.MemberID = f.MemberID
LEFT JOIN AirlineDB.Bronze.Redemptions r ON m.MemberID = r.MemberID
GROUP BY m.MemberID, m.Name, m.CurrentTier;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW AirlineDB.Gold.ProgramLiability AS
SELECT 
    CurrentTier,
    COUNT(MemberID) AS MemberCount,
    SUM(CurrentBalance) AS OutstandingMiles,
    -- Estimating financial liability at $0.015 per mile
    SUM(CurrentBalance) * 0.015 AS EstLiabilityUSD
FROM AirlineDB.Silver.MemberActivity
GROUP BY CurrentTier;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"What is the total estimated liability in USD for 'Platinum' members in AirlineDB.Gold.ProgramLiability?"
*/
