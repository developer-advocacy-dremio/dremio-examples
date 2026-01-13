/*
 * E-commerce Clickstream Analytics Demo
 * 
 * Scenario:
 * An online retailer analyzes user journeys to improve conversion rates.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize the checkout funnel and reduce cart abandonment.
 * 
 * Note: Assumes a catalog named 'EcommerceDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS EcommerceDB;
CREATE FOLDER IF NOT EXISTS EcommerceDB.Bronze;
CREATE FOLDER IF NOT EXISTS EcommerceDB.Silver;
CREATE FOLDER IF NOT EXISTS EcommerceDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Clickstream
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS EcommerceDB.Bronze.UserSessions (
    SessionID VARCHAR,
    UserID INT,
    StartTime TIMESTAMP,
    DeviceType VARCHAR -- 'Mobile', 'Desktop'
);

CREATE TABLE IF NOT EXISTS EcommerceDB.Bronze.PageViews (
    ViewID INT,
    SessionID VARCHAR,
    PageURL VARCHAR, -- '/home', '/product', '/cart', '/checkout'
    ""Timestamp"" TIMESTAMP
);

CREATE TABLE IF NOT EXISTS EcommerceDB.Bronze.ConversionEvents (
    EventID INT,
    SessionID VARCHAR,
    Amount DOUBLE,
    Currency VARCHAR
);

-- 1.2 Populate Bronze Tables
INSERT INTO EcommerceDB.Bronze.UserSessions (SessionID, UserID, StartTime, DeviceType) VALUES
('S1', 101, '2025-11-25 10:00:00', 'Mobile'),
('S2', 102, '2025-11-25 10:05:00', 'Desktop'),
('S3', 103, '2025-11-25 10:15:00', 'Mobile'),
('S4', 104, '2025-11-25 11:00:00', 'Desktop'),
('S5', 105, '2025-11-25 11:20:00', 'Mobile');

INSERT INTO EcommerceDB.Bronze.PageViews (ViewID, SessionID, PageURL, ""Timestamp"") VALUES
(1, 'S1', '/home', '2025-11-25 10:00:05'),
(2, 'S1', '/product/A', '2025-11-25 10:01:00'),
(3, 'S1', '/cart', '2025-11-25 10:02:00'),
(4, 'S1', '/checkout', '2025-11-25 10:02:30'), -- Converted
(5, 'S2', '/home', '2025-11-25 10:05:10'),
(6, 'S2', '/product/B', '2025-11-25 10:06:00'), -- Bounce
(7, 'S3', '/home', '2025-11-25 10:15:05'),
(8, 'S3', '/cart', '2025-11-25 10:16:00'), -- Abandoned Cart
(9, 'S4', '/home', '2025-11-25 11:00:10'),
(10, 'S5', '/home', '2025-11-25 11:20:05');

INSERT INTO EcommerceDB.Bronze.ConversionEvents (EventID, SessionID, Amount, Currency) VALUES
(1, 'S1', 120.50, 'USD');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Session Context
-------------------------------------------------------------------------------

-- 2.1 View: Session_Funnel
-- Flags key steps in the funnel for each session.
CREATE OR REPLACE VIEW EcommerceDB.Silver.Session_Funnel AS
SELECT
    s.SessionID,
    s.DeviceType,
    MAX(CASE WHEN p.PageURL = '/cart' THEN 1 ELSE 0 END) AS AddedToCart,
    MAX(CASE WHEN p.PageURL = '/checkout' THEN 1 ELSE 0 END) AS ReachedCheckout,
    COUNT(c.EventID) AS Converted,
    MAX(c.Amount) AS Revenue
FROM EcommerceDB.Bronze.UserSessions s
LEFT JOIN EcommerceDB.Bronze.PageViews p ON s.SessionID = p.SessionID
LEFT JOIN EcommerceDB.Bronze.ConversionEvents c ON s.SessionID = c.SessionID
GROUP BY s.SessionID, s.DeviceType;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Conversion Metrics
-------------------------------------------------------------------------------

-- 3.1 View: Funnel_Performance
-- Aggregates conversion rates by Device.
CREATE OR REPLACE VIEW EcommerceDB.Gold.Funnel_Performance AS
SELECT
    DeviceType,
    COUNT(SessionID) AS TotalSessions,
    SUM(AddedToCart) AS Carts,
    SUM(Converted) AS Orders,
    (SUM(Converted) * 100.0 / NULLIF(COUNT(SessionID), 0)) AS ConversionRatePct,
    (SUM(AddedToCart) - SUM(Converted)) AS AbandonedCarts
FROM EcommerceDB.Silver.Session_Funnel
GROUP BY DeviceType;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Funnel Analysis):
"Using EcommerceDB.Gold.Funnel_Performance, show me the ConversionRatePct by DeviceType."

PROMPT 2 (Abandonment):
"How many abandoned carts were there on Mobile vs Desktop?"
*/
