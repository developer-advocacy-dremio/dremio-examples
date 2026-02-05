/*
 * Dremio "Messy Data" Challenge: Hotel Booking Overlaps
 * 
 * Scenario: 
 * Hotel system allows overbooking.
 * 'Check_In' and 'Check_Out' dates sometimes overlap for the same Room ID.
 * Room Types are obfuscated codes ('K_OV', 'Q_STD').
 * 
 * Objective for AI Agent:
 * 1. Decode Room Types (K=King, Q=Queen, OV=Ocean View, STD=Standard).
 * 2. Identify Overbooked Rooms: Join table on itself where Room_ID matches and dates overlap.
 * 3. Calculate 'Occupancy Rate' per Room Type.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Grand_Hotel;
CREATE FOLDER IF NOT EXISTS Grand_Hotel.Reservations;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Grand_Hotel.Reservations.BOOKINGS (
    RES_ID VARCHAR,
    ROOM_ID INT,
    TYPE_CODE VARCHAR, -- 'K_OV', 'Q_STD'
    CHECK_IN DATE,
    CHECK_OUT DATE
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Double Bookings)
-------------------------------------------------------------------------------

-- Standard
INSERT INTO Grand_Hotel.Reservations.BOOKINGS VALUES
('R-100', 101, 'K_OV', '2023-01-01', '2023-01-05'),
('R-101', 101, 'K_OV', '2023-01-06', '2023-01-10'); -- No Overlap

-- Overlap!
INSERT INTO Grand_Hotel.Reservations.BOOKINGS VALUES
('R-102', 102, 'Q_STD', '2023-01-01', '2023-01-05'),
('R-103', 102, 'Q_STD', '2023-01-04', '2023-01-08'); -- Overlaps Jan 4-5

-- Type Codes
INSERT INTO Grand_Hotel.Reservations.BOOKINGS VALUES
('R-104', 103, 'S_STE', '2023-01-01', '2023-01-05'), -- Suite?
('R-105', 104, 'D_CTY', '2023-01-01', '2023-01-05'); -- Double City View

-- Bulk Fill
INSERT INTO Grand_Hotel.Reservations.BOOKINGS VALUES
('R-106', 201, 'K_STD', '2023-02-01', '2023-02-05'),
('R-107', 201, 'K_STD', '2023-02-04', '2023-02-10'), -- Overlap
('R-108', 201, 'K_STD', '2023-02-09', '2023-02-15'), -- Overlap
('R-109', 202, 'Q_OV', '2023-02-01', '2023-02-05'),
('R-110', 202, 'Q_OV', '2023-02-05', '2023-02-10'), -- Touch (Check out = Check in, Valid)
('R-111', 203, 'K_OV', '2023-02-01', '2023-02-05'),
('R-112', 203, 'K_OV', '2023-02-01', '2023-02-05'), -- Exact Dupe double booking
('R-113', 204, 'S_PREZ', '2023-02-01', '2023-02-28'),
('R-114', 204, 'S_PREZ', '2023-02-15', '2023-03-01'), -- Serious overlap
('R-115', 301, 'K_STD', '2023-03-01', '2023-03-05'),
('R-116', 302, 'K_STD', '2023-03-01', '2023-03-05'),
('R-117', 303, 'K_STD', '2023-03-01', '2023-03-05'),
('R-118', 304, 'K_STD', '2023-03-01', '2023-03-05'),
('R-119', 305, 'K_STD', '2023-03-01', '2023-03-05'),
('R-120', 401, 'Q_STD', '2023-03-01', '2023-03-05'),
('R-121', 402, 'Q_STD', '2023-03-01', '2023-03-05'),
('R-122', 403, 'Q_STD', '2023-03-01', '2023-03-05'),
('R-123', 404, 'Q_STD', '2023-03-01', '2023-03-05'),
('R-124', 405, 'Q_STD', '2023-03-01', '2023-03-05'),
('R-125', 101, 'K_OV', '2023-04-01', '2023-04-02'),
('R-126', 101, 'K_OV', '2023-04-02', '2023-04-03'), -- Valid
('R-127', 101, 'K_OV', '2023-04-03', '2023-04-04'), -- Valid
('R-128', 101, 'K_OV', '2023-04-04', '2023-04-05'), -- Valid
('R-129', 101, 'K_OV', '2023-04-05', '2023-04-06'), -- Valid
('R-130', 999, 'UNKNOWN', '2023-01-01', '2023-01-05'),
('R-131', 999, 'UNKNOWN', '2023-01-01', '2023-01-01'), -- Same day (Day use?)
('R-132', 888, 'K_STD', '2023-01-05', '2023-01-01'), -- Negative duration
('R-133', 777, 'K_STD', NULL, '2023-01-05'),
('R-134', 777, 'K_STD', '2023-01-01', NULL),
('R-135', 666, 'K_STD', '2023-01-01', '2023-01-05'),
('R-136', 666, 'K_STD', '2023-01-03', '2023-01-07'),
('R-137', 666, 'K_STD', '2023-01-06', '2023-01-10'), -- Chain of overlaps
('R-138', 500, 'K_STD', '2023-05-01', '2023-05-31'),
('R-139', 500, 'K_STD', '2023-05-15', '2023-06-15'),
('R-140', 501, 'D_CTY', '2023-06-01', '2023-06-05'),
('R-141', 502, 'D_CTY', '2023-06-01', '2023-06-05'),
('R-142', 503, 'D_CTY', '2023-06-01', '2023-06-05'),
('R-143', 504, 'D_CTY', '2023-06-01', '2023-06-05'),
('R-144', 505, 'D_CTY', '2023-06-01', '2023-06-05'),
('R-145', 102, 'Q_STD', '2023-07-01', '2023-07-05'),
('R-146', 102, 'Q_STD', '2023-07-03', '2023-07-08'),
('R-147', 103, 'S_STE', '2023-08-01', '2023-08-02'),
('R-148', 103, 'S_STE', '2023-08-01', '2023-08-02'), -- Dupe
('R-149', 105, 'K_OV', '2023-09-01', '2023-09-10'),
('R-150', 105, 'K_OV', '2023-09-02', '2023-09-03'); -- Nested booking

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit the bookings in Grand_Hotel.Reservations.BOOKINGS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Decode Room Type: 'K_'->'King', 'Q_'->'Queen', '_OV'->'Ocean', '_STD'->'Standard'.
 *     - Calculate 'Nights' = Check_Out - Check_In. Filter out negative durations.
 *  3. Gold: 
 *     - Identify 'Overbooked Rooms': JOIN table on itself (A.ROOM_ID = B.ROOM_ID AND A.RES_ID != B.RES_ID).
 *       Check if date ranges overlap.
 *     - Calculate Occupancy Rate % per Month.
 *  
 *  Show the SQL."
 */
