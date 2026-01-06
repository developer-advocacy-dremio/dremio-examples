/*
 * Retail Customer Analytics Demo
 * 
 * Scenario:
 * A retail chain wants to segment customers by loyalty and analyze store performance.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Drive customer retention and optimize store operations.
 * 
 * Note: Assumes a catalog named 'RetailDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RetailDB.Bronze;
CREATE FOLDER IF NOT EXISTS RetailDB.Silver;
CREATE FOLDER IF NOT EXISTS RetailDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS RetailDB.Bronze.Stores (
    StoreID INT,
    Name VARCHAR,
    Location VARCHAR,
    Manager VARCHAR,
    OpenDate DATE
);

CREATE TABLE IF NOT EXISTS RetailDB.Bronze.Products (
    ProductID INT,
    Name VARCHAR,
    Category VARCHAR,
    Price FLOAT,
    Cost FLOAT
);

CREATE TABLE IF NOT EXISTS RetailDB.Bronze.Customers (
    CustomerID INT,
    Name VARCHAR,
    LoyaltyTier VARCHAR,
    Email VARCHAR,
    JoinDate DATE
);

CREATE TABLE IF NOT EXISTS RetailDB.Bronze.Transactions (
    TransactionID INT,
    CustomerID INT,
    StoreID INT,
    ProductID INT,
    Quantity INT,
    TxDate DATE,
    TotalAmount FLOAT
);

-- 1.2 Populate Bronze Tables
-- Insert 5 records into RetailDB.Bronze.Stores
INSERT INTO RetailDB.Bronze.Stores (StoreID, Name, Location, Manager, OpenDate) VALUES
(1, 'Downtown Flagship', '31.7086, -93.1588', 'Manager_1', '2016-09-13'),
(2, 'West Mall', '36.2128, -96.4434', 'Manager_2', '2019-08-16'),
(3, 'Northside Plaza', '33.2079, -94.0901', 'Manager_3', '2017-03-25'),
(4, 'Airport Kiosk', '30.5787, -97.995', 'Manager_4', '2018-02-17'),
(5, 'Online Store', '42.5455, -92.7617', 'Manager_5', '2016-01-27');

-- Insert 50 records into RetailDB.Bronze.Products
INSERT INTO RetailDB.Bronze.Products (ProductID, Name, Category, Price, Cost) VALUES
(1, 'Electronics_Item_1', 'Electronics', 77.71, 41.61),
(2, 'Sports_Item_2', 'Sports', 122.66, 75.22),
(3, 'Beauty_Item_3', 'Beauty', 87.21, 35.19),
(4, 'Sports_Item_4', 'Sports', 185.73, 124.32),
(5, 'Sports_Item_5', 'Sports', 100.45, 42.87),
(6, 'Sports_Item_6', 'Sports', 148.16, 76.72),
(7, 'Beauty_Item_7', 'Beauty', 177.13, 114.04),
(8, 'Electronics_Item_8', 'Electronics', 188.96, 90.33),
(9, 'Beauty_Item_9', 'Beauty', 194.25, 96.41),
(10, 'Home_Item_10', 'Home', 45.17, 30.33),
(11, 'Electronics_Item_11', 'Electronics', 120.49, 49.24),
(12, 'Sports_Item_12', 'Sports', 97.44, 55.67),
(13, 'Beauty_Item_13', 'Beauty', 70.62, 44.34),
(14, 'Sports_Item_14', 'Sports', 77.67, 31.69),
(15, 'Apparel_Item_15', 'Apparel', 146.88, 58.98),
(16, 'Electronics_Item_16', 'Electronics', 79.95, 41.25),
(17, 'Sports_Item_17', 'Sports', 32.85, 15.55),
(18, 'Sports_Item_18', 'Sports', 71.98, 47.11),
(19, 'Sports_Item_19', 'Sports', 160.51, 110.06),
(20, 'Home_Item_20', 'Home', 54.08, 37.51),
(21, 'Sports_Item_21', 'Sports', 194.25, 100.24),
(22, 'Beauty_Item_22', 'Beauty', 131.12, 60.96),
(23, 'Electronics_Item_23', 'Electronics', 150.3, 70.15),
(24, 'Sports_Item_24', 'Sports', 94.6, 56.2),
(25, 'Apparel_Item_25', 'Apparel', 113.58, 46.55),
(26, 'Beauty_Item_26', 'Beauty', 156.72, 97.5),
(27, 'Sports_Item_27', 'Sports', 24.29, 10.69),
(28, 'Beauty_Item_28', 'Beauty', 171.38, 113.31),
(29, 'Electronics_Item_29', 'Electronics', 88.24, 55.19),
(30, 'Sports_Item_30', 'Sports', 197.93, 137.11),
(31, 'Beauty_Item_31', 'Beauty', 87.12, 56.01),
(32, 'Apparel_Item_32', 'Apparel', 70.79, 32.42),
(33, 'Apparel_Item_33', 'Apparel', 66.37, 35.42),
(34, 'Apparel_Item_34', 'Apparel', 68.95, 35.36),
(35, 'Electronics_Item_35', 'Electronics', 32.92, 21.28),
(36, 'Sports_Item_36', 'Sports', 153.76, 80.32),
(37, 'Beauty_Item_37', 'Beauty', 161.47, 69.04),
(38, 'Beauty_Item_38', 'Beauty', 165.63, 114.17),
(39, 'Beauty_Item_39', 'Beauty', 60.61, 42.28),
(40, 'Apparel_Item_40', 'Apparel', 87.25, 43.35),
(41, 'Sports_Item_41', 'Sports', 189.33, 83.36),
(42, 'Home_Item_42', 'Home', 80.95, 42.57),
(43, 'Home_Item_43', 'Home', 183.57, 124.82),
(44, 'Apparel_Item_44', 'Apparel', 13.47, 5.71),
(45, 'Apparel_Item_45', 'Apparel', 61.6, 34.63),
(46, 'Beauty_Item_46', 'Beauty', 186.14, 119.68),
(47, 'Apparel_Item_47', 'Apparel', 166.42, 73.63),
(48, 'Beauty_Item_48', 'Beauty', 101.89, 53.48),
(49, 'Electronics_Item_49', 'Electronics', 179.03, 91.3),
(50, 'Beauty_Item_50', 'Beauty', 152.75, 97.31);

-- Insert 100 records into RetailDB.Bronze.Customers
INSERT INTO RetailDB.Bronze.Customers (CustomerID, Name, LoyaltyTier, Email, JoinDate) VALUES
(1, 'Customer_1', 'Silver', 'cust1@example.com', '2023-10-07'),
(2, 'Customer_2', 'Bronze', 'cust2@example.com', '2024-10-04'),
(3, 'Customer_3', 'Bronze', 'cust3@example.com', '2022-07-06'),
(4, 'Customer_4', 'Gold', 'cust4@example.com', '2020-12-13'),
(5, 'Customer_5', 'Gold', 'cust5@example.com', '2020-05-11'),
(6, 'Customer_6', 'Silver', 'cust6@example.com', '2024-02-27'),
(7, 'Customer_7', 'Bronze', 'cust7@example.com', '2020-06-16'),
(8, 'Customer_8', 'Silver', 'cust8@example.com', '2021-12-10'),
(9, 'Customer_9', 'Silver', 'cust9@example.com', '2021-11-30'),
(10, 'Customer_10', 'Bronze', 'cust10@example.com', '2022-03-31'),
(11, 'Customer_11', 'Bronze', 'cust11@example.com', '2024-01-12'),
(12, 'Customer_12', 'Bronze', 'cust12@example.com', '2022-05-24'),
(13, 'Customer_13', 'Bronze', 'cust13@example.com', '2024-10-14'),
(14, 'Customer_14', 'Bronze', 'cust14@example.com', '2024-01-11'),
(15, 'Customer_15', 'Silver', 'cust15@example.com', '2024-07-22'),
(16, 'Customer_16', 'Bronze', 'cust16@example.com', '2023-10-11'),
(17, 'Customer_17', 'Bronze', 'cust17@example.com', '2024-04-24'),
(18, 'Customer_18', 'Bronze', 'cust18@example.com', '2020-02-09'),
(19, 'Customer_19', 'Bronze', 'cust19@example.com', '2022-04-21'),
(20, 'Customer_20', 'Bronze', 'cust20@example.com', '2022-09-14'),
(21, 'Customer_21', 'Gold', 'cust21@example.com', '2020-10-23'),
(22, 'Customer_22', 'Bronze', 'cust22@example.com', '2023-03-29'),
(23, 'Customer_23', 'Silver', 'cust23@example.com', '2021-12-22'),
(24, 'Customer_24', 'Bronze', 'cust24@example.com', '2023-05-17'),
(25, 'Customer_25', 'Gold', 'cust25@example.com', '2023-07-21'),
(26, 'Customer_26', 'Bronze', 'cust26@example.com', '2020-02-24'),
(27, 'Customer_27', 'Bronze', 'cust27@example.com', '2024-09-23'),
(28, 'Customer_28', 'Bronze', 'cust28@example.com', '2021-10-27'),
(29, 'Customer_29', 'Silver', 'cust29@example.com', '2022-04-28'),
(30, 'Customer_30', 'Bronze', 'cust30@example.com', '2023-12-12'),
(31, 'Customer_31', 'Silver', 'cust31@example.com', '2023-03-31'),
(32, 'Customer_32', 'Silver', 'cust32@example.com', '2022-09-16'),
(33, 'Customer_33', 'Silver', 'cust33@example.com', '2024-11-06'),
(34, 'Customer_34', 'Bronze', 'cust34@example.com', '2023-07-13'),
(35, 'Customer_35', 'Gold', 'cust35@example.com', '2024-02-23'),
(36, 'Customer_36', 'Bronze', 'cust36@example.com', '2020-09-23'),
(37, 'Customer_37', 'Bronze', 'cust37@example.com', '2024-05-16'),
(38, 'Customer_38', 'Gold', 'cust38@example.com', '2024-08-04'),
(39, 'Customer_39', 'Bronze', 'cust39@example.com', '2024-10-23'),
(40, 'Customer_40', 'Gold', 'cust40@example.com', '2020-07-28'),
(41, 'Customer_41', 'Gold', 'cust41@example.com', '2023-08-07'),
(42, 'Customer_42', 'Silver', 'cust42@example.com', '2020-02-11'),
(43, 'Customer_43', 'Bronze', 'cust43@example.com', '2021-12-22'),
(44, 'Customer_44', 'Gold', 'cust44@example.com', '2023-12-18'),
(45, 'Customer_45', 'Silver', 'cust45@example.com', '2023-12-31'),
(46, 'Customer_46', 'Bronze', 'cust46@example.com', '2020-09-13'),
(47, 'Customer_47', 'Gold', 'cust47@example.com', '2022-01-12'),
(48, 'Customer_48', 'Silver', 'cust48@example.com', '2022-04-13'),
(49, 'Customer_49', 'Bronze', 'cust49@example.com', '2023-07-21'),
(50, 'Customer_50', 'Silver', 'cust50@example.com', '2024-06-16'),
(51, 'Customer_51', 'Gold', 'cust51@example.com', '2020-08-03'),
(52, 'Customer_52', 'Gold', 'cust52@example.com', '2021-06-23'),
(53, 'Customer_53', 'Silver', 'cust53@example.com', '2020-04-22'),
(54, 'Customer_54', 'Gold', 'cust54@example.com', '2022-01-27'),
(55, 'Customer_55', 'Silver', 'cust55@example.com', '2022-08-22'),
(56, 'Customer_56', 'Bronze', 'cust56@example.com', '2020-02-14'),
(57, 'Customer_57', 'Bronze', 'cust57@example.com', '2024-02-06'),
(58, 'Customer_58', 'Bronze', 'cust58@example.com', '2020-08-10'),
(59, 'Customer_59', 'Silver', 'cust59@example.com', '2020-02-22'),
(60, 'Customer_60', 'Bronze', 'cust60@example.com', '2021-12-28'),
(61, 'Customer_61', 'Bronze', 'cust61@example.com', '2020-03-15'),
(62, 'Customer_62', 'Bronze', 'cust62@example.com', '2024-05-16'),
(63, 'Customer_63', 'Silver', 'cust63@example.com', '2024-02-29'),
(64, 'Customer_64', 'Bronze', 'cust64@example.com', '2021-09-30'),
(65, 'Customer_65', 'Bronze', 'cust65@example.com', '2024-08-13'),
(66, 'Customer_66', 'Bronze', 'cust66@example.com', '2024-01-21'),
(67, 'Customer_67', 'Bronze', 'cust67@example.com', '2020-02-07'),
(68, 'Customer_68', 'Bronze', 'cust68@example.com', '2024-07-05'),
(69, 'Customer_69', 'Silver', 'cust69@example.com', '2021-10-30'),
(70, 'Customer_70', 'Silver', 'cust70@example.com', '2023-12-16'),
(71, 'Customer_71', 'Platinum', 'cust71@example.com', '2023-12-13'),
(72, 'Customer_72', 'Silver', 'cust72@example.com', '2022-01-10'),
(73, 'Customer_73', 'Bronze', 'cust73@example.com', '2020-11-14'),
(74, 'Customer_74', 'Silver', 'cust74@example.com', '2021-02-23'),
(75, 'Customer_75', 'Bronze', 'cust75@example.com', '2021-10-05'),
(76, 'Customer_76', 'Gold', 'cust76@example.com', '2022-06-09'),
(77, 'Customer_77', 'Silver', 'cust77@example.com', '2020-04-15'),
(78, 'Customer_78', 'Silver', 'cust78@example.com', '2023-05-31'),
(79, 'Customer_79', 'Bronze', 'cust79@example.com', '2024-02-12'),
(80, 'Customer_80', 'Platinum', 'cust80@example.com', '2024-02-05'),
(81, 'Customer_81', 'Bronze', 'cust81@example.com', '2022-05-11'),
(82, 'Customer_82', 'Bronze', 'cust82@example.com', '2022-04-05'),
(83, 'Customer_83', 'Bronze', 'cust83@example.com', '2023-11-03'),
(84, 'Customer_84', 'Bronze', 'cust84@example.com', '2024-01-30'),
(85, 'Customer_85', 'Bronze', 'cust85@example.com', '2021-01-28'),
(86, 'Customer_86', 'Bronze', 'cust86@example.com', '2022-07-13'),
(87, 'Customer_87', 'Silver', 'cust87@example.com', '2020-09-17'),
(88, 'Customer_88', 'Gold', 'cust88@example.com', '2024-09-10'),
(89, 'Customer_89', 'Bronze', 'cust89@example.com', '2023-02-05'),
(90, 'Customer_90', 'Bronze', 'cust90@example.com', '2021-01-16'),
(91, 'Customer_91', 'Bronze', 'cust91@example.com', '2024-11-18'),
(92, 'Customer_92', 'Silver', 'cust92@example.com', '2022-02-12'),
(93, 'Customer_93', 'Silver', 'cust93@example.com', '2020-04-06'),
(94, 'Customer_94', 'Bronze', 'cust94@example.com', '2021-06-07'),
(95, 'Customer_95', 'Silver', 'cust95@example.com', '2024-03-12'),
(96, 'Customer_96', 'Silver', 'cust96@example.com', '2023-09-24'),
(97, 'Customer_97', 'Gold', 'cust97@example.com', '2022-08-08'),
(98, 'Customer_98', 'Silver', 'cust98@example.com', '2021-02-13'),
(99, 'Customer_99', 'Bronze', 'cust99@example.com', '2023-03-04'),
(100, 'Customer_100', 'Bronze', 'cust100@example.com', '2022-09-29');

-- Insert 200 records into RetailDB.Bronze.Transactions
INSERT INTO RetailDB.Bronze.Transactions (TransactionID, CustomerID, StoreID, ProductID, Quantity, TxDate, TotalAmount) VALUES
(1, 35, 4, 26, 2, '2024-11-20', 88.86),
(2, 85, 3, 11, 3, '2024-04-10', 235.82),
(3, 54, 5, 46, 4, '2025-03-23', 480.62),
(4, 21, 3, 4, 5, '2024-06-23', 699.13),
(5, 72, 3, 5, 4, '2025-01-13', 392.93),
(6, 85, 2, 40, 5, '2025-03-14', 280.76),
(7, 48, 4, 35, 2, '2024-02-23', 28.42),
(8, 58, 3, 43, 4, '2025-01-09', 549.73),
(9, 90, 1, 37, 1, '2024-01-29', 160.2),
(10, 81, 2, 16, 5, '2024-09-06', 935.77),
(11, 65, 4, 21, 5, '2024-08-06', 488.63),
(12, 95, 2, 50, 1, '2024-05-26', 191.58),
(13, 59, 3, 39, 2, '2025-03-22', 22.67),
(14, 4, 5, 27, 4, '2024-01-14', 336.65),
(15, 31, 1, 9, 4, '2024-10-01', 357.66),
(16, 31, 5, 42, 3, '2024-07-10', 574.88),
(17, 78, 4, 25, 1, '2024-02-16', 136.25),
(18, 62, 3, 46, 2, '2025-04-07', 291.53),
(19, 29, 1, 37, 3, '2024-03-26', 587.46),
(20, 40, 2, 46, 1, '2024-04-21', 145.68),
(21, 97, 1, 25, 1, '2024-11-17', 25.86),
(22, 49, 4, 30, 4, '2024-03-21', 247.86),
(23, 21, 2, 8, 2, '2024-05-12', 187.38),
(24, 2, 3, 50, 1, '2024-09-03', 130.66),
(25, 46, 1, 2, 1, '2025-02-07', 10.43),
(26, 24, 1, 7, 5, '2024-08-26', 149.79),
(27, 29, 3, 11, 5, '2024-06-14', 468.88),
(28, 62, 5, 30, 2, '2024-05-02', 283.7),
(29, 42, 5, 16, 2, '2024-10-08', 233.59),
(30, 43, 1, 40, 2, '2024-11-16', 151.95),
(31, 70, 2, 36, 4, '2025-04-02', 574.66),
(32, 8, 4, 3, 3, '2024-08-06', 453.79),
(33, 65, 4, 7, 3, '2024-08-02', 204.38),
(34, 37, 3, 48, 3, '2025-02-02', 516.6),
(35, 27, 3, 48, 4, '2025-03-11', 774.53),
(36, 22, 3, 16, 3, '2025-03-06', 94.53),
(37, 8, 5, 38, 2, '2024-04-30', 271.83),
(38, 56, 3, 4, 1, '2024-07-03', 93.0),
(39, 16, 3, 12, 3, '2024-03-10', 246.97),
(40, 30, 4, 49, 5, '2024-06-02', 52.69),
(41, 89, 2, 34, 3, '2024-01-30', 206.2),
(42, 99, 3, 7, 2, '2025-04-07', 270.76),
(43, 97, 5, 27, 4, '2024-06-29', 152.75),
(44, 65, 2, 27, 1, '2024-04-21', 108.0),
(45, 83, 2, 9, 3, '2025-03-26', 242.68),
(46, 72, 2, 37, 4, '2024-11-16', 426.67),
(47, 56, 3, 23, 3, '2024-02-05', 84.77),
(48, 26, 3, 16, 4, '2024-03-20', 109.97),
(49, 69, 3, 8, 2, '2025-02-06', 94.07),
(50, 64, 1, 22, 1, '2024-03-27', 118.81),
(51, 2, 5, 8, 2, '2024-04-09', 210.01),
(52, 99, 5, 16, 2, '2024-06-05', 34.22),
(53, 41, 5, 11, 3, '2024-08-26', 48.28),
(54, 91, 2, 31, 1, '2024-03-07', 83.05),
(55, 80, 5, 39, 3, '2025-01-14', 74.2),
(56, 25, 4, 27, 5, '2024-03-06', 216.22),
(57, 21, 1, 14, 5, '2024-10-17', 137.09),
(58, 23, 4, 31, 1, '2024-12-22', 188.15),
(59, 16, 1, 7, 1, '2024-02-11', 134.45),
(60, 77, 2, 43, 2, '2025-04-02', 280.63),
(61, 96, 3, 13, 1, '2024-02-12', 195.25),
(62, 96, 3, 22, 5, '2025-02-12', 666.32),
(63, 58, 1, 19, 3, '2025-03-25', 98.04),
(64, 60, 2, 5, 3, '2025-03-04', 575.44),
(65, 98, 1, 33, 1, '2024-08-17', 193.37),
(66, 68, 5, 42, 5, '2024-12-31', 336.0),
(67, 89, 5, 9, 1, '2024-09-11', 145.52),
(68, 60, 1, 22, 1, '2024-10-12', 156.34),
(69, 87, 3, 20, 4, '2025-02-14', 50.66),
(70, 35, 2, 13, 4, '2024-06-24', 472.38),
(71, 4, 3, 30, 1, '2024-08-29', 199.57),
(72, 53, 2, 9, 3, '2024-12-27', 500.91),
(73, 88, 3, 23, 3, '2024-04-16', 102.63),
(74, 46, 3, 14, 1, '2025-03-02', 149.07),
(75, 23, 4, 22, 1, '2024-05-15', 195.43),
(76, 74, 1, 7, 2, '2024-04-07', 202.68),
(77, 52, 1, 26, 2, '2024-05-07', 283.9),
(78, 6, 4, 45, 3, '2024-06-24', 280.6),
(79, 67, 3, 27, 3, '2024-02-01', 231.42),
(80, 32, 1, 29, 5, '2024-11-01', 979.15),
(81, 70, 2, 6, 5, '2024-03-27', 350.58),
(82, 3, 2, 40, 5, '2024-06-19', 460.37),
(83, 8, 1, 27, 3, '2025-04-19', 421.75),
(84, 6, 2, 44, 5, '2024-09-03', 369.51),
(85, 61, 3, 8, 2, '2024-01-04', 232.55),
(86, 20, 4, 28, 1, '2024-07-18', 30.74),
(87, 72, 5, 29, 5, '2024-06-28', 760.82),
(88, 13, 5, 5, 3, '2025-03-17', 47.58),
(89, 55, 3, 38, 3, '2024-11-27', 209.07),
(90, 18, 4, 50, 2, '2024-05-17', 184.57),
(91, 71, 3, 39, 4, '2024-06-09', 335.57),
(92, 67, 3, 11, 2, '2024-10-13', 196.34),
(93, 98, 2, 32, 4, '2024-12-25', 648.44),
(94, 67, 5, 48, 4, '2024-02-23', 119.38),
(95, 78, 2, 36, 3, '2024-11-23', 160.74),
(96, 50, 4, 3, 3, '2024-09-12', 408.85),
(97, 78, 4, 48, 4, '2025-01-29', 773.04),
(98, 28, 3, 13, 1, '2024-02-29', 111.9),
(99, 6, 1, 18, 5, '2025-03-15', 437.29),
(100, 62, 3, 25, 1, '2024-12-11', 146.73),
(101, 54, 5, 32, 2, '2025-02-02', 278.97),
(102, 37, 5, 12, 5, '2025-01-22', 320.93),
(103, 54, 3, 16, 2, '2024-07-03', 46.72),
(104, 39, 2, 29, 3, '2024-07-18', 79.17),
(105, 95, 2, 9, 4, '2024-03-31', 653.25),
(106, 22, 5, 8, 3, '2024-11-25', 116.04),
(107, 53, 1, 18, 4, '2024-08-22', 631.63),
(108, 44, 2, 10, 4, '2024-03-03', 679.88),
(109, 45, 2, 49, 2, '2024-08-29', 366.82),
(110, 76, 1, 20, 5, '2024-07-01', 295.01),
(111, 82, 2, 33, 3, '2024-11-17', 234.8),
(112, 26, 5, 18, 4, '2024-06-03', 710.09),
(113, 58, 4, 39, 5, '2025-02-01', 259.54),
(114, 25, 1, 15, 3, '2024-08-05', 222.47),
(115, 36, 3, 18, 5, '2025-03-23', 55.35),
(116, 21, 1, 44, 1, '2024-09-27', 62.88),
(117, 13, 1, 8, 4, '2024-08-11', 346.03),
(118, 82, 3, 16, 3, '2025-01-30', 497.1),
(119, 4, 4, 1, 4, '2024-08-24', 739.46),
(120, 4, 4, 10, 2, '2024-05-18', 186.9),
(121, 54, 3, 43, 1, '2024-10-17', 135.9),
(122, 63, 3, 43, 3, '2025-01-27', 419.49),
(123, 40, 1, 32, 4, '2025-01-31', 168.81),
(124, 41, 1, 25, 3, '2024-05-29', 336.91),
(125, 64, 5, 11, 4, '2024-04-15', 135.2),
(126, 60, 3, 43, 2, '2024-01-10', 363.21),
(127, 73, 2, 48, 5, '2024-01-07', 810.35),
(128, 49, 1, 31, 4, '2024-10-22', 587.69),
(129, 56, 2, 24, 4, '2025-04-14', 379.84),
(130, 39, 5, 50, 5, '2024-10-06', 452.4),
(131, 93, 2, 32, 1, '2024-08-17', 192.72),
(132, 54, 3, 29, 2, '2024-09-13', 277.51),
(133, 17, 2, 5, 5, '2025-03-20', 198.8),
(134, 24, 2, 33, 5, '2024-01-07', 993.29),
(135, 29, 2, 45, 3, '2025-03-09', 69.62),
(136, 99, 5, 38, 2, '2024-07-20', 233.77),
(137, 18, 2, 26, 1, '2024-04-12', 143.42),
(138, 3, 4, 2, 5, '2024-04-20', 945.41),
(139, 48, 4, 8, 4, '2025-03-01', 191.71),
(140, 2, 2, 1, 2, '2025-01-23', 159.94),
(141, 18, 5, 37, 3, '2024-11-28', 551.03),
(142, 98, 2, 28, 2, '2025-03-07', 245.15),
(143, 92, 2, 38, 3, '2024-06-05', 114.13),
(144, 71, 1, 41, 5, '2024-12-10', 63.0),
(145, 24, 5, 47, 2, '2024-07-20', 205.74),
(146, 18, 3, 39, 4, '2025-03-27', 468.75),
(147, 92, 4, 39, 2, '2025-02-19', 280.97),
(148, 67, 1, 25, 5, '2024-09-13', 254.92),
(149, 28, 3, 2, 5, '2024-07-27', 515.22),
(150, 43, 3, 30, 4, '2024-02-29', 385.72),
(151, 45, 3, 33, 2, '2025-03-03', 171.57),
(152, 22, 4, 44, 1, '2024-06-12', 20.89),
(153, 3, 3, 48, 4, '2025-01-10', 40.14),
(154, 20, 5, 24, 4, '2024-02-13', 760.64),
(155, 34, 1, 3, 4, '2025-03-03', 271.67),
(156, 25, 2, 34, 5, '2025-03-18', 885.94),
(157, 9, 1, 18, 2, '2024-02-25', 53.58),
(158, 17, 3, 22, 5, '2024-11-21', 205.11),
(159, 83, 3, 10, 5, '2025-03-17', 824.86),
(160, 59, 1, 32, 5, '2025-01-04', 775.27),
(161, 6, 5, 2, 5, '2025-04-15', 460.6),
(162, 57, 1, 39, 4, '2024-12-25', 681.44),
(163, 75, 4, 38, 5, '2024-04-09', 175.23),
(164, 65, 3, 15, 4, '2024-09-20', 119.74),
(165, 56, 2, 43, 1, '2024-12-11', 78.96),
(166, 73, 5, 7, 1, '2024-02-06', 119.32),
(167, 30, 3, 2, 5, '2024-07-19', 343.81),
(168, 89, 1, 25, 2, '2024-06-12', 90.92),
(169, 51, 3, 33, 1, '2024-12-05', 92.56),
(170, 85, 1, 44, 3, '2024-02-22', 538.74),
(171, 82, 5, 45, 3, '2025-03-27', 186.99),
(172, 85, 3, 42, 3, '2024-06-02', 115.97),
(173, 87, 5, 22, 2, '2024-02-15', 135.37),
(174, 31, 1, 1, 5, '2024-10-12', 200.31),
(175, 34, 2, 45, 3, '2024-10-13', 256.47),
(176, 69, 4, 2, 2, '2025-04-09', 198.87),
(177, 93, 1, 16, 3, '2024-08-06', 161.06),
(178, 98, 5, 40, 5, '2025-02-15', 415.94),
(179, 90, 2, 10, 1, '2024-05-25', 57.02),
(180, 48, 4, 21, 5, '2024-03-26', 346.07),
(181, 93, 4, 30, 3, '2024-10-20', 174.7),
(182, 6, 4, 3, 1, '2024-06-10', 88.45),
(183, 91, 3, 35, 1, '2024-12-28', 98.06),
(184, 11, 2, 6, 1, '2024-09-29', 18.37),
(185, 43, 5, 23, 3, '2024-02-17', 275.7),
(186, 28, 3, 12, 5, '2024-04-08', 187.7),
(187, 42, 5, 35, 4, '2024-04-13', 605.06),
(188, 3, 1, 41, 1, '2025-02-15', 155.56),
(189, 18, 1, 16, 2, '2024-04-24', 34.51),
(190, 93, 3, 7, 4, '2024-06-21', 595.49),
(191, 93, 4, 16, 3, '2024-05-05', 60.34),
(192, 77, 5, 34, 2, '2024-03-10', 286.21),
(193, 86, 3, 25, 4, '2024-06-03', 696.86),
(194, 6, 1, 50, 5, '2024-11-12', 798.68),
(195, 62, 4, 44, 2, '2024-07-25', 116.23),
(196, 37, 2, 39, 3, '2024-05-10', 181.03),
(197, 37, 3, 30, 4, '2024-04-07', 122.63),
(198, 31, 4, 40, 3, '2024-12-12', 344.39),
(199, 10, 3, 47, 2, '2024-04-12', 242.17),
(200, 83, 3, 12, 3, '2024-06-24', 497.52);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Customer Behavior & Enrichment
-------------------------------------------------------------------------------

-- 2.1 View: Enriched_Transactions
-- Joins Transactions with Product and Store details for full context.
CREATE OR REPLACE VIEW RetailDB.Silver.Enriched_Transactions AS
SELECT
    t.TransactionID,
    c.Name AS CustomerName,
    c.LoyaltyTier,
    s.Name AS StoreName,
    p.Name AS ProductName,
    p.Category,
    t.Quantity,
    t.TotalAmount,
    (t.TotalAmount - (p.Cost * t.Quantity)) AS GrossMargin,
    t.TxDate
FROM RetailDB.Bronze.Transactions t
JOIN RetailDB.Bronze.Customers c ON t.CustomerID = c.CustomerID
JOIN RetailDB.Bronze.Stores s ON t.StoreID = s.StoreID
JOIN RetailDB.Bronze.Products p ON t.ProductID = p.ProductID;

-- 2.2 View: Customer_RFM
-- Calculates Recency, Frequency, and Monetary value for each customer.
CREATE OR REPLACE VIEW RetailDB.Silver.Customer_RFM AS
SELECT
    CustomerID,
    MAX(Name) AS CustomerName,
    MAX(LoyaltyTier) AS Tier,
    MAX(TxDate) AS LastPurchaseDate,
    DATEDIFF(day, MAX(TxDate), CAST('2025-05-01' AS DATE)) AS Recency, -- Assumed current date
    COUNT(TransactionID) AS Frequency,
    SUM(TotalAmount) AS MonetaryValue
FROM RetailDB.Silver.Enriched_Transactions
GROUP BY CustomerID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategic Insights
-------------------------------------------------------------------------------

-- 3.1 View: Store_Performance_YTD
-- Aggregates revenue and margin by Store.
CREATE OR REPLACE VIEW RetailDB.Gold.Store_Performance_YTD AS
SELECT
    StoreName,
    Category,
    SUM(TotalAmount) AS TotalRevenue,
    SUM(GrossMargin) AS TotalMargin,
    COUNT(DISTINCT TransactionID) AS TransactionCount
FROM RetailDB.Silver.Enriched_Transactions
WHERE YEAR(TxDate) = 2025 
GROUP BY StoreName, Category;

-- 3.2 View: Customer_Segments
-- Segments customers into 'High Value', 'At Risk', etc. based on RFM.
CREATE OR REPLACE VIEW RetailDB.Gold.Customer_Segments AS
SELECT
    CustomerName,
    Tier,
    Recency,
    MonetaryValue,
    CASE
        WHEN Recency < 30 AND MonetaryValue > 500 THEN 'High Value Active'
        WHEN Recency > 90 AND MonetaryValue > 500 THEN 'At Risk High Value'
        WHEN Recency < 30 THEN 'Active Low Spend'
        ELSE 'Churned'
    END AS Segment
FROM RetailDB.Silver.Customer_RFM;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Store Dashboard):
"Using RetailDB.Gold.Store_Performance_YTD, show me a stacked bar chart of TotalRevenue by StoreName, colored by Category."

PROMPT 2 (Churn Analysis):
"How many customers are in the 'At Risk High Value' segment in RetailDB.Gold.Customer_Segments? List their names and tiers."

PROMPT 3 (Loyalty Impact):
"From RetailDB.Silver.Customer_RFM, what is the average MonetaryValue for each LoyaltyTier?"
*/
