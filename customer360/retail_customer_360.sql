/*
 * Omni-Channel Retail Customer 360 Demo
 * 
 * Scenario:
 * A retailer wants to create a unified view of their customers by combining data from:
 * 1. ERP System: Offline sales and customer master data.
 * 2. CRM System: Customer support tickets and loyalty program details.
 * 3. Web Analytics: Online clickstream, session duration, and cart abandonment.
 * 
 * Architecture: Medallion (Source -> Bronze -> Silver -> Gold)
 * 
 * Source Layer:
 * - Physical tables representing the raw extraction from source systems.
 * - Seeded with ~50 records per system to simulate a realistic dataset.
 * 
 * Bronze Layer (Views):
 * - Direct 1:1 views of the source data, handling basic type casting if necessary.
 * 
 * Silver Layer (Views):
 * - Cleaned, standardized, and enriched data.
 * - Integrates across sources (e.g., joining ERP Customer ID with CRM Member ID).
 * 
 * Gold Layer (Views):
 * - Aggregated business-level metric views.
 * - "C360_Profile": The master customer profile.
 * - "Churn_Risk_Analysis": Predictive indicators based on support and web activity.
 * 
 * Note: Assumes a root context where folders can be created.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Customer360;
CREATE FOLDER IF NOT EXISTS Customer360.Sources;
CREATE FOLDER IF NOT EXISTS Customer360.Bronze;
CREATE FOLDER IF NOT EXISTS Customer360.Silver;
CREATE FOLDER IF NOT EXISTS Customer360.Gold;

-------------------------------------------------------------------------------
-- 1. SOURCES: Physical Tables & Seed Data
-------------------------------------------------------------------------------

-- 1.1 ERP System (Sales & Customers)
CREATE TABLE IF NOT EXISTS Customer360.Sources.ERP_Customers (
    CustomerID INT,
    FullName VARCHAR,
    Email VARCHAR,
    Phone VARCHAR,
    JoinDate DATE,
    Region VARCHAR
);

CREATE TABLE IF NOT EXISTS Customer360.Sources.ERP_Orders (
    OrderID INT,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DOUBLE,
    StoreLocation VARCHAR,
    PaymentMethod VARCHAR
);

-- Seed ERP_Customers
INSERT INTO Customer360.Sources.ERP_Customers (CustomerID, FullName, Email, Phone, JoinDate, Region) VALUES
(1, 'Alice Johnson', 'alice.j@example.com', '555-0101', '2023-01-15', 'North'),
(2, 'Bob Smith', 'bob.smith@testmail.com', '555-0102', '2023-02-20', 'South'),
(3, 'Charlie Brown', 'charlie.b@example.org', '555-0103', '2022-11-10', 'East'),
(4, 'Diana Prince', 'diana.p@amaz.com', '555-0104', '2021-05-05', 'West'),
(5, 'Evan Wright', 'evan.w@test.com', '555-0105', '2024-01-01', 'North'),
(6, 'Fiona Le', 'fiona.l@example.com', '555-0106', '2023-06-12', 'South'),
(7, 'George King', 'g.king@email.com', '555-0107', '2022-08-30', 'East'),
(8, 'Hannah Scott', 'hannah.s@test.com', '555-0108', '2024-02-14', 'West'),
(9, 'Ian Miller', 'ian.m@example.com', '555-0109', '2023-09-09', 'North'),
(10, 'Julia Roberts', 'j.roberts@movie.com', '555-0110', '2021-12-01', 'South'),
(11, 'Kevin Hart', 'k.hart@funny.com', '555-0111', '2022-03-15', 'East'),
(12, 'Laura Croft', 'l.croft@tomb.com', '555-0112', '2023-07-20', 'West'),
(13, 'Mike Ross', 'm.ross@suit.com', '555-0113', '2024-03-10', 'North'),
(14, 'Nancy Drew', 'n.drew@detect.com', '555-0114', '2022-05-25', 'South'),
(15, 'Oscar Wilde', 'o.wilde@write.com', '555-0115', '2021-09-18', 'East'),
(16, 'Paul Rudd', 'p.rudd@ant.com', '555-0116', '2023-11-05', 'West'),
(17, 'Quinn Fabray', 'q.fabray@glee.com', '555-0117', '2022-01-30', 'North'),
(18, 'Rachel Green', 'r.green@friends.com', '555-0118', '2024-01-20', 'South'),
(19, 'Steve Rogers', 's.rogers@shield.com', '555-0119', '2021-07-04', 'East'),
(20, 'Tony Stark', 't.stark@iron.com', '555-0120', '2020-05-29', 'West'),
(21, 'Ursula K', 'u.k@sea.com', '555-0121', '2023-04-12', 'North'),
(22, 'Victor Hugo', 'v.hugo@mis.com', '555-0122', '2022-10-01', 'South'),
(23, 'Wanda Maximoff', 'w.maximoff@hex.com', '555-0123', '2024-02-28', 'East'),
(24, 'Xander Cage', 'x.cage@xxx.com', '555-0124', '2021-08-15', 'West'),
(25, 'Yara Greyjoy', 'y.greyjoy@iron.com', '555-0125', '2023-12-05', 'North'),
(26, 'Zack Snyder', 'z.snyder@cutt.com', '555-0126', '2022-06-20', 'South'),
(27, 'Amy Pond', 'a.pond@who.com', '555-0127', '2024-03-01', 'East'),
(28, 'Bruce Wayne', 'b.wayne@bat.com', '555-0128', '2021-02-10', 'West'),
(29, 'Clark Kent', 'c.kent@daily.com', '555-0129', '2023-10-10', 'North'),
(30, 'Donna Noble', 'd.noble@who.com', '555-0130', '2022-04-05', 'South'),
(31, 'Eleven Jane', 'eleven@strange.com', '555-0131', '2024-01-11', 'East'),
(32, 'Frodo Baggins', 'f.baggins@shire.com', '555-0132', '2021-12-25', 'West'),
(33, 'Gandalf Gray', 'gandalf@wiz.com', '555-0133', '2023-05-05', 'North'),
(34, 'Harry Potter', 'h.potter@hog.com', '555-0134', '2022-07-31', 'South'),
(35, 'Indiana Jones', 'i.jones@arch.com', '555-0135', '2024-02-01', 'East'),
(36, 'Jack Sparrow', 'j.sparrow@pirate.com', '555-0136', '2021-03-15', 'West'),
(37, 'Katniss Everdeen', 'k.everdeen@panem.com', '555-0137', '2023-11-20', 'North'),
(38, 'Luke Skywalker', 'l.skywalker@force.com', '555-0138', '2022-09-10', 'South'),
(39, 'Marty McFly', 'm.mcfly@time.com', '555-0139', '2024-03-15', 'East'),
(40, 'Neo Anderson', 'neo@matrix.com', '555-0140', '2021-06-01', 'West'),
(41, 'Optimus Prime', 'prime@autobot.com', '555-0141', '2023-08-08', 'North'),
(42, 'Peter Parker', 'p.parker@web.com', '555-0142', '2022-01-15', 'South'),
(43, 'Quentin Tarantino', 'q.tarantino@film.com', '555-0143', '2024-02-10', 'East'),
(44, 'Rocky Balboa', 'r.balboa@box.com', '555-0144', '2021-11-01', 'West'),
(45, 'Sherlock Holmes', 's.holmes@deduce.com', '555-0145', '2023-02-22', 'North'),
(46, 'Thor Odinson', 'thor@asgard.com', '555-0146', '2022-12-12', 'South'),
(47, 'Ulysses Grant', 'u.grant@hist.com', '555-0147', '2024-01-30', 'East'),
(48, 'Vito Corleone', 'v.corleone@mob.com', '555-0148', '2021-04-25', 'West'),
(49, 'Walter White', 'w.white@chem.com', '555-0149', '2023-09-20', 'North'),
(50, 'Xena Warrior', 'xena@war.com', '555-0150', '2022-05-10', 'South');

-- Seed ERP_Orders (Multiple orders per customer)
INSERT INTO Customer360.Sources.ERP_Orders (OrderID, CustomerID, OrderDate, TotalAmount, StoreLocation, PaymentMethod) VALUES
(101, 1, '2024-01-10', 150.00, 'NY-01', 'Credit Card'),
(102, 1, '2024-02-15', 200.50, 'NY-01', 'Credit Card'),
(103, 2, '2024-03-01', 50.00, 'TX-05', 'Cash'),
(104, 3, '2023-12-10', 500.00, 'Online', 'PayPal'),
(105, 4, '2024-01-05', 120.00, 'CA-02', 'Credit Card'),
(106, 5, '2024-02-28', 300.00, 'IL-03', 'Debit Card'),
(107, 1, '2024-03-20', 75.00, 'NY-01', 'Credit Card'),
(108, 6, '2023-11-15', 1000.00, 'Online', 'Amex'),
(109, 7, '2024-01-01', 25.00, 'FL-09', 'Cash'),
(110, 8, '2024-03-15', 450.00, 'Unknown', 'Credit Card'), -- Missing store
(111, 20, '2024-01-10', 20000.00, 'Online', 'Black Card'), -- High spender
(112, 20, '2024-02-01', 5000.00, 'CA-02', 'Black Card'),
(113, 30, '2024-01-25', 15.00, 'UK-01', 'Debit Card'),
(114, 40, '2024-02-14', 100.00, 'Online', 'Crypto'),
(115, 10, '2024-03-05', 350.00, 'LA-04', 'Credit Card'),
(116, 50, '2023-12-25', 90.00, 'Online', 'PayPal'),
(117, 3, '2024-01-15', 600.00, 'Online', 'PayPal'),
(118, 5, '2024-03-01', 150.00, 'IL-03', 'Debit Card'),
(119, 12, '2024-02-20', 220.00, 'UK-01', 'Credit Card'),
(120, 15, '2024-01-05', 45.00, 'NY-02', 'Cash'),
(121, 2, '2024-03-05', 60.00, 'TX-05', 'Cash'),
(122, 9, '2023-10-31', 80.00, 'Online', 'Apple Pay'),
(123, 11, '2024-02-02', 200.00, 'Online', 'Credit Card'),
(124, 13, '2024-03-12', 3000.00, 'NY-01', 'Corp Card'),
(125, 14, '2024-01-20', 25.00, 'Online', 'PayPal'),
(126, 16, '2024-03-01', 10.00, 'CA-02', 'Cash'),
(127, 18, '2024-02-15', 500.00, 'NY-01', 'Credit Card'),
(128, 22, '2023-11-20', 120.00, 'Online', 'Credit Card'),
(129, 25, '2024-01-08', 350.00, 'Online', 'Debit Card'),
(130, 28, '2024-02-28', 1000.00, 'Gotham-01', 'Credit Card'),
(131, 35, '2024-03-10', 90.00, 'Online', 'PayPal'),
(132, 45, '2024-01-12', 170.00, 'UK-01', 'Credit Card'),
(133, 49, '2024-02-05', 1000.00, 'NM-01', 'Cash'),
(134, 1, '2024-03-25', 50.00, 'NY-01', 'Credit Card'),
(135, 4, '2024-02-10', 80.00, 'CA-02', 'Debit Card'),
(136, 6, '2024-01-15', 900.00, 'Online', 'Amex'),
(137, 8, '2024-03-20', 300.00, 'Unknown', 'Credit Card'),
(138, 20, '2024-03-01', 15000.00, 'Online', 'Black Card'),
(139, 41, '2024-01-01', 500.00, 'Online', 'CyberTon'),
(140, 39, '2024-02-20', 88.00, 'CA-05', 'Credit Card'),
(141, 23, '2024-01-30', 440.00, 'NJ-01', 'Apple Pay'),
(142, 17, '2024-02-14', 120.00, 'OH-03', 'Debit Card'),
(143, 31, '2024-03-15', 11.00, 'IN-01', 'Cash'),
(144, 33, '2024-01-05', 750.00, 'Online', 'MagicDust'),
(145, 42, '2024-02-25', 20.00, 'NY-05', 'Cash'),
(146, 44, '2024-03-05', 99.00, 'PA-01', 'Credit Card'),
(147, 46, '2023-12-15', 400.00, 'Online', 'Gold'),
(148, 48, '2024-01-20', 2500.00, 'NY-LittleItaly', 'Cash'),
(149, 21, '2024-02-28', 190.00, 'Online', 'Shells'),
(150, 7, '2024-03-01', 40.00, 'FL-09', 'Credit Card');

-- 1.2 CRM System (Support & Loyalty)
CREATE TABLE IF NOT EXISTS Customer360.Sources.CRM_Tickets (
    TicketID VARCHAR,
    CustomerEmail VARCHAR,
    IssueCategory VARCHAR,
    Status VARCHAR, -- Open, Closed, Pending
    CSAT_Score INT -- 1-5, NULL if not rated
);

CREATE TABLE IF NOT EXISTS Customer360.Sources.CRM_Loyalty (
    MemberID VARCHAR,
    Email VARCHAR,
    TierLevel VARCHAR, -- Bronze, Silver, Gold, Platinum
    PointsBalance INT
);

-- Seed CRM_Tickets
INSERT INTO Customer360.Sources.CRM_Tickets (TicketID, CustomerEmail, IssueCategory, Status, CSAT_Score) VALUES
('T-001', 'alice.j@example.com', 'Returns', 'Closed', 5),
('T-002', 'bob.smith@testmail.com', 'Shipping', 'Closed', 2), -- Unhappy
('T-003', 'alice.j@example.com', 'Product info', 'Closed', 4),
('T-004', 'diana.p@amaz.com', 'Billing', 'Open', NULL),
('T-005', 'k.hart@funny.com', 'Returns', 'Closed', 5),
('T-006', 't.stark@iron.com', 'VIP Service', 'Closed', 1), -- Ironically unsatisfied
('T-007', 'hannah.s@test.com', 'Shipping', 'Pending', NULL),
('T-008', 'b.wayne@bat.com', 'Product Defect', 'Closed', 3),
('T-009', 'neo@matrix.com', 'Login Issue', 'Closed', 5),
('T-010', 's.rogers@shield.com', 'Discount Code', 'Closed', 5),
('T-011', 'g.king@email.com', 'Shipping', 'Closed', 1), -- Very unhappy
('T-012', 'g.king@email.com', 'Shipping', 'Open', NULL), -- Recurring issue
('T-013', 'm.ross@suit.com', 'Billing', 'Closed', 4),
('T-014', 'f.baggins@shire.com', 'Lost Package', 'Pending', NULL),
('T-015', 'c.kent@daily.com', 'Returns', 'Closed', 5),
('T-016', 'j.sparrow@pirate.com', 'Payment', 'Closed', 2),
('T-017', 'w.white@chem.com', 'Product Quality', 'Open', NULL),
('T-018', 'alice.j@example.com', 'Damaged Item', 'Closed', 3),
('T-019', 'ian.m@example.com', 'Shipping', 'Closed', 4),
('T-020', 'l.croft@tomb.com', 'Product Info', 'Closed', 5),
('T-021', 'v.hugo@mis.com', 'Website Bug', 'Closed', 3),
('T-022', 'q.fabray@glee.com', 'Returns', 'Closed', 4),
('T-023', 'r.green@friends.com', 'Size Exchange', 'Closed', 5),
('T-024', 'x.cage@xxx.com', 'Delivery', 'Closed', 2),
('T-025', 'y.greyjoy@iron.com', 'Damaged', 'Open', NULL),
('T-026', 'z.snyder@cutt.com', 'Slow Site', 'Closed', 3),
('T-027', 'a.pond@who.com', 'Missing Part', 'Closed', 5),
('T-028', 'd.noble@who.com', 'Rude Staff', 'Closed', 1),
('T-029', 'eleven@strange.com', 'Wrong Item', 'Closed', 4),
('T-030', 'h.potter@hog.com', 'Owl Delivery', 'Closed', 5),
('T-031', 'i.jones@arch.com', 'Old Product', 'Closed', 4),
('T-032', 'k.everdeen@panem.com', 'Defective Bow', 'Closed', 2),
('T-033', 'l.skywalker@force.com', 'Missing Hand', 'Open', NULL),
('T-034', 'm.mcfly@time.com', 'Late Delivery', 'Closed', 5),
('T-035', 'prime@autobot.com', 'Size Issue', 'Closed', 3),
('T-036', 'p.parker@web.com', 'Web Issue', 'Closed', 5),
('T-037', 'q.tarantino@film.com', 'Violence', 'Closed', 1),
('T-038', 'r.balboa@box.com', 'Damaged Box', 'Closed', 4),
('T-039', 's.holmes@deduce.com', 'Mystery Charge', 'Closed', 5),
('T-040', 'thor@asgard.com', 'Heavy Item', 'Closed', 4),
('T-041', 'u.grant@hist.com', 'History', 'Closed', 3),
('T-042', 'v.corleone@mob.com', 'Offer Refused', 'Closed', 1),
('T-043', 'xena@war.com', 'Broken Sword', 'Closed', 2),
('T-044', 'u.k@sea.com', 'Water Damage', 'Closed', 1),
('T-045', 'j.roberts@movie.com', 'VIP', 'Closed', 5),
('T-046', 'n.drew@detect.com', 'Lost Item', 'Closed', 4),
('T-047', 'o.wilde@write.com', 'Pen Ink', 'Closed', 3),
('T-048', 'p.rudd@ant.com', 'Size Small', 'Closed', 5),
('T-049', 'bob.smith@testmail.com', 'Billing', 'Open', NULL),
('T-050', 'diana.p@amaz.com', 'Return', 'Closed', 5);

-- Seed CRM_Loyalty (Matches ERP emails)
INSERT INTO Customer360.Sources.CRM_Loyalty (MemberID, Email, TierLevel, PointsBalance) VALUES
('M-1001', 'alice.j@example.com', 'Silver', 500),
('M-1002', 'bob.smith@testmail.com', 'Bronze', 100),
('M-1003', 'charlie.b@example.org', 'Gold', 1200),
('M-1004', 't.stark@iron.com', 'Platinum', 50000),
('M-1005', 'b.wayne@bat.com', 'Platinum', 45000),
('M-1006', 'diana.p@amaz.com', 'Silver', 600),
('M-1007', 'fiona.l@example.com', 'Gold', 2000),
('M-1008', 'm.ross@suit.com', 'Silver', 800),
('M-1009', 'w.white@chem.com', 'Bronze', 50),
('M-1010', 's.rogers@shield.com', 'Gold', 1500),
('M-1011', 'h.potter@hog.com', 'Silver', 777),
('M-1012', 'l.skywalker@force.com', 'Gold', 1980),
('M-1013', 'neo@matrix.com', 'Silver', 300),
('M-1014', 'hannah.s@test.com', 'Bronze', 20),
('M-1015', 'ian.m@example.com', 'Bronze', 150),
('M-1016', 'j.roberts@movie.com', 'Silver', 450),
('M-1017', 'k.hart@funny.com', 'Gold', 1100),
('M-1018', 'l.croft@tomb.com', 'Silver', 650),
('M-1019', 'n.drew@detect.com', 'Bronze', 10),
('M-1020', 'o.wilde@write.com', 'Bronze', 0),
('M-1021', 'p.rudd@ant.com', 'Bronze', 200),
('M-1022', 'q.fabray@glee.com', 'Silver', 400),
('M-1023', 'r.green@friends.com', 'Gold', 2500),
('M-1024', 'u.k@sea.com', 'Bronze', 50),
('M-1025', 'v.hugo@mis.com', 'Silver', 550),
('M-1026', 'w.maximoff@hex.com', 'Gold', 1300),
('M-1027', 'x.cage@xxx.com', 'Bronze', 100),
('M-1028', 'y.greyjoy@iron.com', 'Silver', 600),
('M-1029', 'z.snyder@cutt.com', 'Platinum', 10000),
('M-1030', 'a.pond@who.com', 'Silver', 350),
('M-1031', 'c.kent@daily.com', 'Bronze', 0),
('M-1032', 'd.noble@who.com', 'Bronze', 50),
('M-1033', 'eleven@strange.com', 'Bronze', 11),
('M-1034', 'gandalf@wiz.com', 'Platinum', 9000),
('M-1035', 'i.jones@arch.com', 'Silver', 880),
('M-1036', 'j.sparrow@pirate.com', 'Gold', 3000),
('M-1037', 'k.everdeen@panem.com', 'Silver', 400),
('M-1038', 'm.mcfly@time.com', 'Gold', 1985),
('M-1039', 'prime@autobot.com', 'Silver', 500),
('M-1040', 'p.parker@web.com', 'Bronze', 10),
('M-1041', 'q.tarantino@film.com', 'Silver', 600),
('M-1042', 'r.balboa@box.com', 'Bronze', 100),
('M-1043', 's.holmes@deduce.com', 'Gold', 2210),
('M-1044', 'thor@asgard.com', 'Platinum', 8000),
('M-1045', 'u.grant@hist.com', 'Bronze', 50),
('M-1046', 'v.corleone@mob.com', 'Platinum', 50000),
('M-1047', 'xena@war.com', 'Silver', 900),
('M-1048', 'g.king@email.com', 'Bronze', 0), -- Low value
('M-1049', 'evan.w@test.com', 'Silver', 300),
('M-1050', 'f.baggins@shire.com', 'Bronze', 20);

-- 1.3 Web Analytics System
CREATE TABLE IF NOT EXISTS Customer360.Sources.Web_Sessions (
    SessionID VARCHAR,
    UserEmail VARCHAR, -- Cookie resolved to email
    SessionDate DATE,
    DurationSeconds INT,
    CartAbandoned BOOLEAN,
    DeviceType VARCHAR
);

-- Seed Web_Sessions
INSERT INTO Customer360.Sources.Web_Sessions (SessionID, UserEmail, SessionDate, DurationSeconds, CartAbandoned, DeviceType) VALUES
('S-1', 'alice.j@example.com', '2024-03-24', 300, false, 'Mobile'),
('S-2', 'alice.j@example.com', '2024-03-25', 120, false, 'Mobile'),
('S-3', 'bob.smith@testmail.com', '2024-03-20', 600, true, 'Desktop'), -- Abandoned
('S-4', 'bob.smith@testmail.com', '2024-03-22', 45, true, 'Desktop'), -- Abandoned again (Risk)
('S-5', 't.stark@iron.com', '2024-03-01', 50, false, 'Tablet'), -- Quick buy
('S-6', 'g.king@email.com', '2024-03-01', 900, true, 'Mobile'), -- Long session, abandon
('S-7', 'g.king@email.com', '2024-03-05', 10, true, 'Desktop'), -- Bounce
('S-8', 'charlie.b@example.org', '2024-01-15', 1500, false, 'Desktop'),
('S-9', 'diana.p@amaz.com', '2024-02-10', 300, false, 'Mobile'),
('S-10', 'm.ross@suit.com', '2024-03-10', 200, false, 'Tablet'),
('S-11', 'w.white@chem.com', '2023-09-20', 1000, true, 'Desktop'),
('S-12', 'hannah.s@test.com', '2024-02-14', 60, false, 'Mobile'),
('S-13', 'l.croft@tomb.com', '2024-02-20', 400, false, 'Desktop'),
('S-14', 'neo@matrix.com', '2024-02-14', 800, false, 'Desktop'),
('S-15', 's.rogers@shield.com', '2024-02-02', 300, false, 'Mobile'),
('S-16', 'p.parker@web.com', '2024-02-25', 150, true, 'Mobile'),
('S-17', 'b.wayne@bat.com', '2024-03-01', 60, false, 'Desktop'),
('S-18', 'c.kent@daily.com', '2024-03-10', 500, false, 'Tablet'),
('S-19', 'fiona.l@example.com', '2023-11-15', 2000, false, 'Desktop'),
('S-20', 'ian.m@example.com', '2023-09-09', 100, true, 'Mobile'),
('S-21', 'j.roberts@movie.com', '2024-03-05', 400, false, 'Tablet'),
('S-22', 'k.hart@funny.com', '2024-02-02', 350, false, 'Desktop'),
('S-23', 'n.drew@detect.com', '2024-01-20', 1200, true, 'Mobile'),
('S-24', 'o.wilde@write.com', '2024-01-05', 600, false, 'Desktop'),
('S-25', 'p.rudd@ant.com', '2024-03-20', 50, false, 'Mobile'),
('S-26', 'q.fabray@glee.com', '2024-01-30', 900, false, 'Tablet'),
('S-27', 'r.green@friends.com', '2024-02-15', 700, false, 'Desktop'),
('S-28', 'u.k@sea.com', '2024-03-01', 100, true, 'Mobile'),
('S-29', 'v.hugo@mis.com', '2022-10-01', 300, false, 'Desktop'),
('S-30', 'w.maximoff@hex.com', '2024-02-28', 500, false, 'Tablet'),
('S-31', 'x.cage@xxx.com', '2021-08-15', 200, false, 'Mobile'),
('S-32', 'y.greyjoy@iron.com', '2023-12-05', 800, true, 'Desktop'),
('S-33', 'z.snyder@cutt.com', '2024-03-01', 1500, false, 'Desktop'),
('S-34', 'a.pond@who.com', '2024-03-01', 250, false, 'Mobile'),
('S-35', 'd.noble@who.com', '2024-03-02', 300, true, 'Tablet'),
('S-36', 'eleven@strange.com', '2024-01-11', 100, false, 'Desktop'),
('S-37', 'f.baggins@shire.com', '2021-12-25', 1200, false, 'Mobile'),
('S-38', 'gandalf@wiz.com', '2023-05-05', 600, false, 'Desktop'),
('S-39', 'h.potter@hog.com', '2024-03-01', 400, false, 'Tablet'),
('S-40', 'i.jones@arch.com', '2024-02-01', 900, false, 'Desktop'),
('S-41', 'j.sparrow@pirate.com', '2024-03-15', 200, true, 'Mobile'),
('S-42', 'k.everdeen@panem.com', '2023-11-20', 500, false, 'Tablet'),
('S-43', 'l.skywalker@force.com', '2022-09-10', 800, true, 'Desktop'),
('S-44', 'm.mcfly@time.com', '2024-03-15', 300, false, 'Mobile'),
('S-45', 'prime@autobot.com', '2023-08-08', 600, false, 'Desktop'),
('S-46', 'q.tarantino@film.com', '2024-02-10', 1000, true, 'Desktop'),
('S-47', 'r.balboa@box.com', '2021-11-01', 150, false, 'Mobile'),
('S-48', 's.holmes@deduce.com', '2023-02-22', 1200, false, 'Tablet'),
('S-49', 'thor@asgard.com', '2024-03-01', 400, false, 'Desktop'),
('S-50', 'u.grant@hist.com', '2024-01-30', 200, false, 'Mobile');

-------------------------------------------------------------------------------
-- 2. BRONZE LAYER (Logic Views)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW Customer360.Bronze.ERP_Customers_View AS SELECT * FROM Customer360.Sources.ERP_Customers;
CREATE OR REPLACE VIEW Customer360.Bronze.ERP_Orders_View AS SELECT * FROM Customer360.Sources.ERP_Orders;
CREATE OR REPLACE VIEW Customer360.Bronze.CRM_Tickets_View AS SELECT * FROM Customer360.Sources.CRM_Tickets;
CREATE OR REPLACE VIEW Customer360.Bronze.CRM_Loyalty_View AS SELECT * FROM Customer360.Sources.CRM_Loyalty;
CREATE OR REPLACE VIEW Customer360.Bronze.Web_Sessions_View AS SELECT * FROM Customer360.Sources.Web_Sessions;

-------------------------------------------------------------------------------
-- 3. SILVER LAYER (Enriched & Standardized)
-------------------------------------------------------------------------------

-- 3.1 Unify Customers (Left Join ERP to CRM)
CREATE OR REPLACE VIEW Customer360.Silver.Customers_Unified AS
SELECT
    e.CustomerID,
    e.FullName,
    e.Email,
    e.Region,
    l.MemberID,
    COALESCE(l.TierLevel, 'None') AS LoyaltyTier,
    COALESCE(l.PointsBalance, 0) AS LoyaltyPoints
FROM Customer360.Bronze.ERP_Customers_View e
LEFT JOIN Customer360.Bronze.CRM_Loyalty_View l ON e.Email = l.Email;

-- 3.2 Enrich Orders with Store Info (Simulated logic if Store table existed, here just passing through)
CREATE OR REPLACE VIEW Customer360.Silver.Orders_Enriched AS
SELECT
    o.OrderID,
    o.CustomerID,
    o.OrderDate,
    o.TotalAmount,
    o.StoreLocation,
    CASE 
        WHEN o.StoreLocation = 'Online' THEN 'Digital'
        ELSE 'Physical'
    END AS Channel
FROM Customer360.Bronze.ERP_Orders_View o;

-- 3.3 Engagement Scores (Web Behavior + Support Satisfaction)
CREATE OR REPLACE VIEW Customer360.Silver.Engagement_Scores AS
SELECT
    u.Email,
    -- Web Metrics
    COUNT(s.SessionID) AS TotalSessions,
    AVG(s.DurationSeconds) AS AvgSessionRef,
    SUM(CASE WHEN s.CartAbandoned THEN 1 ELSE 0 END) AS AbandonedCarts,
    -- Support Metrics
    COUNT(t.TicketID) AS TotalTickets,
    AVG(t.CSAT_Score) AS AvgCSAT
FROM Customer360.Sources.ERP_Customers u
LEFT JOIN Customer360.Bronze.Web_Sessions_View s ON u.Email = s.UserEmail
LEFT JOIN Customer360.Bronze.CRM_Tickets_View t ON u.Email = t.CustomerEmail
GROUP BY u.Email;

-------------------------------------------------------------------------------
-- 4. GOLD LAYER (Business Aggregates)
-------------------------------------------------------------------------------

-- 4.1 Master C360 Profile
CREATE OR REPLACE VIEW Customer360.Gold.C360_Profile AS
SELECT
    cu.CustomerID,
    cu.FullName,
    cu.Email,
    cu.LoyaltyTier,
    cu.LoyaltyPoints,
    -- Financials
    COUNT(oe.OrderID) AS LifetimeOrders,
    SUM(oe.TotalAmount) AS LifetimeSpend,
    MAX(oe.OrderDate) AS LastPurchaseDate,
    -- Engagement
    es.TotalSessions,
    es.AvgCSAT,
    -- Churn Logic
    CASE 
        WHEN es.AvgCSAT < 2 AND es.TotalTickets > 1 THEN 'High Risk'
        WHEN DATEDIFF(day, MAX(oe.OrderDate), CURRENT_DATE) > 180 THEN 'At Risk (Dormant)'
        WHEN es.AbandonedCarts > 3 THEN 'Medium Risk'
        ELSE 'Healthy'
    END AS ChurnStatus
FROM Customer360.Silver.Customers_Unified cu
LEFT JOIN Customer360.Silver.Orders_Enriched oe ON cu.CustomerID = oe.CustomerID
LEFT JOIN Customer360.Silver.Engagement_Scores es ON cu.Email = es.Email
GROUP BY cu.CustomerID, cu.FullName, cu.Email, cu.LoyaltyTier, cu.LoyaltyPoints, es.TotalSessions, es.AvgCSAT, es.TotalTickets, es.AbandonedCarts;

-- 4.2 High Value Concierge List
CREATE OR REPLACE VIEW Customer360.Gold.High_Value_Concierge_List AS
SELECT
    FullName,
    Email,
    LifetimeSpend,
    ChurnStatus 
FROM Customer360.Gold.C360_Profile
WHERE LifetimeSpend > 5000 OR LoyaltyTier = 'Platinum';

-------------------------------------------------------------------------------
-- 5. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Churn Analysis):
"Count the number of customers in each ChurnStatus group using Customer360.Gold.C360_Profile."

PROMPT 2 (Sales Outreach):
"List the names and emails of high value customers who are 'At Risk (Dormant)' from Customer360.Gold.High_Value_Concierge_List."

PROMPT 3 (Loyalty Insight):
"What is the average LifetimeSpend for each LoyaltyTier in Customer360.Gold.C360_Profile?"
*/
