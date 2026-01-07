/*
 * Hospitality Revenue Management Demo
 * 
 * Scenario:
 * A hotel chain optimizes room pricing and occupancy rates.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Maximize RevPAR (Revenue Per Available Room).
 * 
 * Note: Assumes a catalog named 'HospitalityDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HospitalityDB.Bronze;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Silver;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Booking Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS HospitalityDB.Bronze.Hotels (
    HotelID INT,
    Name VARCHAR,
    City VARCHAR,
    TotalRooms INT
);

CREATE TABLE IF NOT EXISTS HospitalityDB.Bronze.Bookings (
    BookingID INT,
    HotelID INT,
    CheckInDate DATE,
    CheckOutDate DATE,
    RoomRate DOUBLE,
    Status VARCHAR -- 'Completed', 'Cancelled', 'No-Show'
);

CREATE TABLE IF NOT EXISTS HospitalityDB.Bronze.GuestReviews (
    ReviewID INT,
    HotelID INT,
    Rating INT, -- 1-5
    Comment VARCHAR
);

-- 1.2 Populate Bronze Tables
INSERT INTO HospitalityDB.Bronze.Hotels (HotelID, Name, City, TotalRooms) VALUES
(1, 'Grand Plaza', 'New York', 200),
(2, 'Ocean View', 'Miami', 150),
(3, 'Mountain Lodge', 'Denver', 100);

INSERT INTO HospitalityDB.Bronze.Bookings (BookingID, HotelID, CheckInDate, CheckOutDate, RoomRate, Status) VALUES
(1, 1, '2025-06-01', '2025-06-05', 350.00, 'Completed'),
(2, 1, '2025-06-02', '2025-06-04', 320.00, 'Completed'),
(3, 2, '2025-06-01', '2025-06-07', 250.00, 'Completed'),
(4, 3, '2025-06-05', '2025-06-10', 180.00, 'Cancelled'),
(5, 1, '2025-06-05', '2025-06-06', 400.00, 'Completed');

INSERT INTO HospitalityDB.Bronze.GuestReviews (ReviewID, HotelID, Rating, Comment) VALUES
(1, 1, 5, 'Great stay!'),
(2, 1, 4, 'Good location.'),
(3, 2, 5, 'Loved the beach.'),
(4, 3, 2, 'Room was cold.');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Occupancy Metrics
-------------------------------------------------------------------------------

-- 2.1 View: Daily_Occupancy
-- Expands bookings into daily records to calculate occupancy.
-- Note: Simplified logic using just booking totals for range.
CREATE OR REPLACE VIEW HospitalityDB.Silver.Booking_Details AS
SELECT
    b.BookingID,
    h.Name AS HotelName,
    h.TotalRooms,
    b.CheckInDate,
    b.CheckOutDate,
    DATEDIFF(DAY, b.CheckInDate, b.CheckOutDate) AS Nights,
    b.RoomRate,
    (DATEDIFF(DAY, b.CheckInDate, b.CheckOutDate) * b.RoomRate) AS TotalRevenue
FROM HospitalityDB.Bronze.Bookings b
JOIN HospitalityDB.Bronze.Hotels h ON b.HotelID = h.HotelID
WHERE b.Status = 'Completed';

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: RevPAR Analysis
-------------------------------------------------------------------------------

-- 3.1 View: Hotel_Performance
-- Calculates RevPAR (Total Revenue / Total Rooms Available).
CREATE OR REPLACE VIEW HospitalityDB.Gold.Hotel_Performance AS
SELECT
    HotelName,
    COUNT(BookingID) AS TotalBookings,
    SUM(TotalRevenue) AS TotalRevenue,
    AVG(RoomRate) AS ADR, -- Average Daily Rate
    (SUM(TotalRevenue) / MAX(TotalRooms)) AS RevPAR_Approximation -- Simplified
FROM HospitalityDB.Silver.Booking_Details
GROUP BY HotelName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Revenue Leader):
"Which hotel has the highest TotalRevenue in HospitalityDB.Gold.Hotel_Performance?"

PROMPT 2 (Pricing):
"Show me the Average Daily Rate (ADR) for each hotel."
*/
