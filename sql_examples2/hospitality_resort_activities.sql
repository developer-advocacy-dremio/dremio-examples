/*
 * Hospitality: Resort Activity Analytics
 * 
 * Scenario:
 * Tracking guest ancillary spending on resort activities (Spa, Tours, Golf, Dining) to optimize packages.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HospitalityDB;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Resort;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Resort.Bronze;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Resort.Silver;
CREATE FOLDER IF NOT EXISTS HospitalityDB.Resort.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Guest Folios
-------------------------------------------------------------------------------

-- GuestFolios Table
CREATE TABLE IF NOT EXISTS HospitalityDB.Resort.Bronze.GuestFolios (
    FolioID INT,
    GuestName VARCHAR,
    CheckInDate DATE,
    CheckOutDate DATE,
    RoomType VARCHAR,
    Segment VARCHAR -- Family, Couple, Business, Solo
);

INSERT INTO HospitalityDB.Resort.Bronze.GuestFolios VALUES
(1001, 'Smith Family', '2025-06-01', '2025-06-07', 'Suite', 'Family'),
(1002, 'Jones Couple', '2025-06-02', '2025-06-05', 'Deluxe', 'Couple'),
(1003, 'Acme Corp Exec', '2025-06-03', '2025-06-04', 'Standard', 'Business'),
(1004, 'Doe Solo', '2025-06-01', '2025-06-10', 'Standard', 'Solo'),
(1005, 'Johnson Family', '2025-06-01', '2025-06-05', 'Villa', 'Family'),
(1006, 'Williams Pair', '2025-06-02', '2025-06-06', 'Deluxe', 'Couple'),
(1007, 'Tech Conf Attendee', '2025-06-03', '2025-06-05', 'Standard', 'Business'),
(1008, 'Brown Family', '2025-06-04', '2025-06-08', 'Suite', 'Family'),
(1009, 'Davis Couple', '2025-06-01', '2025-06-03', 'Deluxe', 'Couple'),
(1010, 'Miller Business', '2025-06-02', '2025-06-04', 'Standard', 'Business'),
(1011, 'Wilson Family', '2025-06-01', '2025-06-07', 'Villa', 'Family'),
(1012, 'Moore Couple', '2025-06-05', '2025-06-09', 'Deluxe', 'Couple'),
(1013, 'Taylor Solo', '2025-06-06', '2025-06-08', 'Standard', 'Solo'),
(1014, 'Anderson Family', '2025-06-02', '2025-06-06', 'Suite', 'Family'),
(1015, 'Thomas Couple', '2025-06-03', '2025-06-07', 'Deluxe', 'Couple'),
(1016, 'Jackson Biz', '2025-06-04', '2025-06-05', 'Standard', 'Business'),
(1017, 'White Family', '2025-06-01', '2025-06-05', 'Suite', 'Family'),
(1018, 'Harris Couple', '2025-06-02', '2025-06-05', 'Deluxe', 'Couple'),
(1019, 'Martin Solo', '2025-06-03', '2025-06-10', 'Standard', 'Solo'),
(1020, 'Thompson Family', '2025-06-04', '2025-06-08', 'Villa', 'Family'),
(1021, 'Garcia Couple', '2025-06-01', '2025-06-04', 'Deluxe', 'Couple'),
(1022, 'Martinez Biz', '2025-06-02', '2025-06-03', 'Standard', 'Business'),
(1023, 'Robinson Family', '2025-06-05', '2025-06-10', 'Suite', 'Family'),
(1024, 'Clark Couple', '2025-06-06', '2025-06-08', 'Deluxe', 'Couple'),
(1025, 'Rodriguez Solo', '2025-06-01', '2025-06-05', 'Standard', 'Solo'),
(1026, 'Lewis Family', '2025-06-02', '2025-06-06', 'Suite', 'Family'),
(1027, 'Lee Couple', '2025-06-03', '2025-06-05', 'Deluxe', 'Couple'),
(1028, 'Walker Biz', '2025-06-04', '2025-06-06', 'Standard', 'Business'),
(1029, 'Hall Family', '2025-06-01', '2025-06-05', 'Villa', 'Family'),
(1030, 'Allen Couple', '2025-06-02', '2025-06-07', 'Deluxe', 'Couple'),
(1031, 'Young Solo', '2025-06-03', '2025-06-06', 'Standard', 'Solo'),
(1032, 'Hernandez Family', '2025-06-04', '2025-06-09', 'Suite', 'Family'),
(1033, 'King Couple', '2025-06-05', '2025-06-08', 'Deluxe', 'Couple'),
(1034, 'Wright Biz', '2025-06-01', '2025-06-02', 'Standard', 'Business'),
(1035, 'Lopez Family', '2025-06-02', '2025-06-06', 'Suite', 'Family'),
(1036, 'Hill Couple', '2025-06-03', '2025-06-05', 'Deluxe', 'Couple'),
(1037, 'Scott Solo', '2025-06-04', '2025-06-10', 'Standard', 'Solo'),
(1038, 'Green Family', '2025-06-05', '2025-06-08', 'Villa', 'Family'),
(1039, 'Adams Couple', '2025-06-01', '2025-06-04', 'Deluxe', 'Couple'),
(1040, 'Baker Biz', '2025-06-02', '2025-06-03', 'Standard', 'Business'),
(1041, 'Gonzalez Family', '2025-06-03', '2025-06-07', 'Suite', 'Family'),
(1042, 'Nelson Couple', '2025-06-04', '2025-06-06', 'Deluxe', 'Couple'),
(1043, 'Carter Solo', '2025-06-05', '2025-06-08', 'Standard', 'Solo'),
(1044, 'Mitchell Family', '2025-06-01', '2025-06-05', 'Villa', 'Family'),
(1045, 'Perez Couple', '2025-06-02', '2025-06-05', 'Deluxe', 'Couple'),
(1046, 'Roberts Biz', '2025-06-03', '2025-06-04', 'Standard', 'Business'),
(1047, 'Turner Family', '2025-06-04', '2025-06-10', 'Suite', 'Family'),
(1048, 'Phillips Couple', '2025-06-05', '2025-06-09', 'Deluxe', 'Couple'),
(1049, 'Campbell Solo', '2025-06-01', '2025-06-03', 'Standard', 'Solo'),
(1050, 'Parker Family', '2025-06-02', '2025-06-06', 'Villa', 'Family');

-- ActivityCharges Table
CREATE TABLE IF NOT EXISTS HospitalityDB.Resort.Bronze.ActivityCharges (
    ChargeID INT,
    FolioID INT,
    Category VARCHAR, -- Spa, Golf, Dining, Tour, WaterSports
    Amount DOUBLE,
    ChargeDate DATE
);

INSERT INTO HospitalityDB.Resort.Bronze.ActivityCharges VALUES
(1, 1001, 'Dining', 150.00, '2025-06-02'),
(2, 1001, 'WaterSports', 200.00, '2025-06-03'),
(3, 1002, 'Spa', 300.00, '2025-06-03'),
(4, 1002, 'Dining', 250.00, '2025-06-04'),
(5, 1003, 'Dining', 50.00, '2025-06-03'),
(6, 1005, 'Golf', 500.00, '2025-06-02'),
(7, 1005, 'Dining', 300.00, '2025-06-03'),
(8, 1006, 'Tour', 100.00, '2025-06-03'),
(9, 1008, 'Dining', 200.00, '2025-06-05'),
(10, 1009, 'Spa', 400.00, '2025-06-02'),
(11, 1011, 'WaterSports', 300.00, '2025-06-02'),
(12, 1011, 'Dining', 180.00, '2025-06-03'),
(13, 1012, 'Golf', 250.00, '2025-06-06'),
(14, 1014, 'Tour', 150.00, '2025-06-04'),
(15, 1015, 'Dining', 300.00, '2025-06-04'),
(16, 1017, 'WaterSports', 100.00, '2025-06-03'),
(17, 1018, 'Spa', 150.00, '2025-06-03'),
(18, 1020, 'Dining', 400.00, '2025-06-05'),
(19, 1021, 'Dining', 120.00, '2025-06-02'),
(20, 1023, 'Golf', 300.00, '2025-06-06'),
(21, 1024, 'Spa', 200.00, '2025-06-07'),
(22, 1001, 'Dining', 100.00, '2025-06-04'),
(23, 1026, 'WaterSports', 250.00, '2025-06-03'),
(24, 1027, 'Tour', 120.00, '2025-06-04'),
(25, 1029, 'Golf', 400.00, '2025-06-02'),
(26, 1030, 'Dining', 200.00, '2025-06-03'),
(27, 1032, 'Dining', 150.00, '2025-06-05'),
(28, 1033, 'Spa', 350.00, '2025-06-06'),
(29, 1035, 'Tour', 180.00, '2025-06-03'),
(30, 1038, 'WaterSports', 150.00, '2025-06-06'),
(31, 1039, 'Dining', 100.00, '2025-06-02'),
(32, 1041, 'Golf', 600.00, '2025-06-04'),
(33, 1042, 'Spa', 100.00, '2025-06-05'),
(34, 1044, 'Dining', 220.00, '2025-06-03'),
(35, 1045, 'Tour', 90.00, '2025-06-03'),
(36, 1047, 'WaterSports', 200.00, '2025-06-05'),
(37, 1048, 'Dining', 150.00, '2025-06-06'),
(38, 1050, 'Golf', 450.00, '2025-06-03'),
(39, 1001, 'Tour', 100.00, '2025-06-05'),
(40, 1005, 'Spa', 200.00, '2025-06-04'),
(41, 1011, 'Golf', 250.00, '2025-06-04'),
(42, 1020, 'WaterSports', 150.00, '2025-06-06'),
(43, 1029, 'Dining', 300.00, '2025-06-04'),
(44, 1032, 'Tour', 120.00, '2025-06-07'),
(45, 1041, 'Dining', 250.00, '2025-06-05'),
(46, 1047, 'Spa', 150.00, '2025-06-08'),
(47, 1050, 'Dining', 180.00, '2025-06-05'),
(48, 1002, 'Tour', 80.00, '2025-06-02'),
(49, 1006, 'Dining', 150.00, '2025-06-05'),
(50, 1015, 'Golf', 200.00, '2025-06-05');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Guest Spend Profile
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HospitalityDB.Resort.Silver.GuestSpend AS
SELECT 
    f.FolioID,
    f.GuestName,
    f.Segment,
    ac.Category,
    ac.Amount,
    ac.ChargeDate
FROM HospitalityDB.Resort.Bronze.GuestFolios f
JOIN HospitalityDB.Resort.Bronze.ActivityCharges ac ON f.FolioID = ac.FolioID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Preference Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HospitalityDB.Resort.Gold.SegmentPreferences AS
SELECT 
    Segment,
    Category,
    SUM(Amount) AS TotalRevenue,
    COUNT(*) AS TransactionCount,
    AVG(Amount) AS AvgTransactionSize
FROM HospitalityDB.Resort.Silver.GuestSpend
GROUP BY Segment, Category;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"What is the most profitable activity category for the 'Family' segment in the HospitalityDB.Resort.Gold.SegmentPreferences view?"

PROMPT 2:
"Calculate total ancillary spend per Guest for the 'Couple' segment using the Silver layer."

PROMPT 3:
"List all guest folios that have spent more than $500 on 'Golf'."
*/
