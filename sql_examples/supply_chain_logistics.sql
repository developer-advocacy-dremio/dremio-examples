/*
 * Supply Chain Logistics Demo
 * 
 * Scenario:
 * A logistics company tracks inventory across zones and shipments from global suppliers.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize inventory turnover and reliability.
 * 
 * Note: Assumes a catalog named 'SupplyChainDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SupplyChainDB;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Bronze;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Silver;
CREATE FOLDER IF NOT EXISTS SupplyChainDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------

-- 1.1 Create Suppliers
CREATE TABLE IF NOT EXISTS SupplyChainDB.Bronze.Suppliers (
    SupplierID INT,
    Name VARCHAR,
    Country VARCHAR,
    ReliabilityScore FLOAT,
    ContractDate DATE
);

-- 1.2 Create Products
CREATE TABLE IF NOT EXISTS SupplyChainDB.Bronze.Products (
    ProductID INT,
    SupplierID INT,
    Name VARCHAR,
    Category VARCHAR,
    UnitCost DOUBLE
);

-- 1.3 Create Inventory
CREATE TABLE IF NOT EXISTS SupplyChainDB.Bronze.Inventory (
    InventoryID INT,
    ProductID INT,
    Zone VARCHAR,
    QuantityOnHand INT,
    ReorderLevel INT,
    LastCountDate DATE
);

-- 1.4 Create Shipments
CREATE TABLE IF NOT EXISTS SupplyChainDB.Bronze.Shipments (
    ShipmentID INT,
    SupplierID INT,
    ProductID INT,
    Quantity INT,
    ShipmentDate DATE,
    ExpectedArrival DATE,
    Status VARCHAR
);

-- 1.5 Populate Bronze Tables
-- Insert 50 records into SupplyChainDB.Bronze.Suppliers
INSERT INTO SupplyChainDB.Bronze.Suppliers (SupplierID, Name, Country, ReliabilityScore, ContractDate) VALUES
(1, 'Acme Supply 1', 'Mexico', 84.4, '2022-02-23'),
(2, 'TechComponents Inc 2', 'Mexico', 86.2, '2022-04-05'),
(3, 'TechComponents Inc 3', 'Germany', 71.4, '2021-07-07'),
(4, 'RawMaterials Ltd 4', 'China', 78.1, '2022-07-28'),
(5, 'TechComponents Inc 5', 'Mexico', 96.5, '2021-01-05'),
(6, 'GlobalParts Co 6', 'Vietnam', 85.2, '2020-02-01'),
(7, 'TechComponents Inc 7', 'Germany', 85.6, '2018-09-21'),
(8, 'GlobalParts Co 8', 'Mexico', 74.2, '2024-08-03'),
(9, 'RawMaterials Ltd 9', 'USA', 81.1, '2021-05-29'),
(10, 'Acme Supply 10', 'Germany', 78.0, '2021-01-14'),
(11, 'FastShip Logistics 11', 'Japan', 85.3, '2019-10-05'),
(12, 'FastShip Logistics 12', 'Vietnam', 78.8, '2021-10-03'),
(13, 'GlobalParts Co 13', 'Germany', 92.5, '2018-12-24'),
(14, 'RawMaterials Ltd 14', 'Mexico', 96.1, '2018-08-21'),
(15, 'RawMaterials Ltd 15', 'Vietnam', 97.0, '2018-09-17'),
(16, 'Acme Supply 16', 'Vietnam', 88.2, '2024-02-27'),
(17, 'FastShip Logistics 17', 'Germany', 85.1, '2021-11-20'),
(18, 'Acme Supply 18', 'China', 90.3, '2021-08-29'),
(19, 'FastShip Logistics 19', 'Germany', 73.6, '2019-08-14'),
(20, 'Acme Supply 20', 'Germany', 99.8, '2020-01-29'),
(21, 'RawMaterials Ltd 21', 'Germany', 75.8, '2021-04-16'),
(22, 'TechComponents Inc 22', 'China', 95.1, '2022-10-13'),
(23, 'Acme Supply 23', 'Japan', 75.1, '2020-04-05'),
(24, 'Acme Supply 24', 'Germany', 85.7, '2021-03-13'),
(25, 'FastShip Logistics 25', 'China', 77.5, '2019-01-19'),
(26, 'Acme Supply 26', 'Mexico', 79.7, '2023-08-06'),
(27, 'RawMaterials Ltd 27', 'USA', 78.1, '2020-04-04'),
(28, 'Acme Supply 28', 'Vietnam', 77.7, '2021-08-02'),
(29, 'Acme Supply 29', 'China', 70.4, '2021-12-09'),
(30, 'GlobalParts Co 30', 'Mexico', 82.9, '2022-03-30'),
(31, 'TechComponents Inc 31', 'Germany', 80.9, '2022-05-13'),
(32, 'Acme Supply 32', 'Mexico', 99.1, '2018-06-10'),
(33, 'RawMaterials Ltd 33', 'China', 99.0, '2023-03-22'),
(34, 'GlobalParts Co 34', 'Germany', 89.3, '2022-12-29'),
(35, 'GlobalParts Co 35', 'USA', 77.8, '2018-09-27'),
(36, 'FastShip Logistics 36', 'USA', 99.3, '2019-07-28'),
(37, 'RawMaterials Ltd 37', 'Vietnam', 87.7, '2021-10-31'),
(38, 'GlobalParts Co 38', 'Vietnam', 89.4, '2023-05-28'),
(39, 'FastShip Logistics 39', 'USA', 78.7, '2018-11-03'),
(40, 'RawMaterials Ltd 40', 'Vietnam', 83.8, '2022-11-15'),
(41, 'GlobalParts Co 41', 'China', 96.0, '2019-04-03'),
(42, 'FastShip Logistics 42', 'Mexico', 73.3, '2020-03-18'),
(43, 'FastShip Logistics 43', 'China', 76.4, '2023-07-03'),
(44, 'RawMaterials Ltd 44', 'Japan', 92.6, '2020-12-01'),
(45, 'RawMaterials Ltd 45', 'Germany', 78.5, '2024-09-07'),
(46, 'FastShip Logistics 46', 'Germany', 97.7, '2021-02-26'),
(47, 'TechComponents Inc 47', 'Vietnam', 75.3, '2024-07-07'),
(48, 'Acme Supply 48', 'USA', 91.2, '2018-03-23'),
(49, 'RawMaterials Ltd 49', 'Vietnam', 98.3, '2019-07-12'),
(50, 'FastShip Logistics 50', 'Mexico', 78.2, '2018-04-30');

-- Insert 100 records into SupplyChainDB.Bronze.Products
INSERT INTO SupplyChainDB.Bronze.Products (ProductID, SupplierID, Name, Category, UnitCost) VALUES
(1, 15, 'Product_1', 'Automotive', 476.21),
(2, 10, 'Product_2', 'Industrial', 287.38),
(3, 4, 'Product_3', 'Electronics', 38.58),
(4, 37, 'Product_4', 'Chemicals', 42.2),
(5, 13, 'Product_5', 'Chemicals', 478.29),
(6, 11, 'Product_6', 'Electronics', 491.03),
(7, 3, 'Product_7', 'Industrial', 263.38),
(8, 50, 'Product_8', 'Electronics', 389.85),
(9, 50, 'Product_9', 'Home Goods', 25.99),
(10, 43, 'Product_10', 'Automotive', 414.67),
(11, 17, 'Product_11', 'Industrial', 477.91),
(12, 3, 'Product_12', 'Automotive', 251.38),
(13, 15, 'Product_13', 'Electronics', 316.39),
(14, 47, 'Product_14', 'Home Goods', 222.8),
(15, 7, 'Product_15', 'Electronics', 65.92),
(16, 35, 'Product_16', 'Industrial', 259.2),
(17, 12, 'Product_17', 'Chemicals', 435.34),
(18, 23, 'Product_18', 'Electronics', 432.08),
(19, 47, 'Product_19', 'Electronics', 497.46),
(20, 35, 'Product_20', 'Industrial', 112.54),
(21, 20, 'Product_21', 'Chemicals', 353.23),
(22, 4, 'Product_22', 'Automotive', 74.85),
(23, 48, 'Product_23', 'Home Goods', 373.08),
(24, 9, 'Product_24', 'Chemicals', 122.44),
(25, 7, 'Product_25', 'Industrial', 372.76),
(26, 3, 'Product_26', 'Electronics', 499.52),
(27, 39, 'Product_27', 'Automotive', 389.4),
(28, 10, 'Product_28', 'Automotive', 385.17),
(29, 38, 'Product_29', 'Industrial', 389.44),
(30, 47, 'Product_30', 'Home Goods', 135.55),
(31, 20, 'Product_31', 'Home Goods', 425.4),
(32, 5, 'Product_32', 'Electronics', 230.85),
(33, 42, 'Product_33', 'Electronics', 337.84),
(34, 40, 'Product_34', 'Automotive', 35.02),
(35, 43, 'Product_35', 'Industrial', 234.42),
(36, 21, 'Product_36', 'Electronics', 427.56),
(37, 12, 'Product_37', 'Automotive', 221.38),
(38, 20, 'Product_38', 'Automotive', 62.89),
(39, 17, 'Product_39', 'Electronics', 160.71),
(40, 1, 'Product_40', 'Electronics', 32.24),
(41, 10, 'Product_41', 'Home Goods', 383.8),
(42, 29, 'Product_42', 'Home Goods', 131.36),
(43, 45, 'Product_43', 'Electronics', 154.97),
(44, 9, 'Product_44', 'Electronics', 21.74),
(45, 42, 'Product_45', 'Automotive', 61.39),
(46, 25, 'Product_46', 'Automotive', 154.25),
(47, 15, 'Product_47', 'Industrial', 351.65),
(48, 8, 'Product_48', 'Chemicals', 413.8),
(49, 31, 'Product_49', 'Electronics', 421.41),
(50, 15, 'Product_50', 'Electronics', 439.27),
(51, 28, 'Product_51', 'Electronics', 360.35),
(52, 47, 'Product_52', 'Electronics', 366.94),
(53, 36, 'Product_53', 'Electronics', 259.26),
(54, 28, 'Product_54', 'Chemicals', 69.26),
(55, 34, 'Product_55', 'Industrial', 325.58),
(56, 31, 'Product_56', 'Automotive', 239.31),
(57, 35, 'Product_57', 'Home Goods', 236.02),
(58, 48, 'Product_58', 'Automotive', 116.2),
(59, 23, 'Product_59', 'Chemicals', 305.95),
(60, 15, 'Product_60', 'Chemicals', 133.08),
(61, 46, 'Product_61', 'Chemicals', 88.62),
(62, 35, 'Product_62', 'Industrial', 162.75),
(63, 20, 'Product_63', 'Chemicals', 299.52),
(64, 15, 'Product_64', 'Industrial', 266.36),
(65, 32, 'Product_65', 'Automotive', 256.9),
(66, 3, 'Product_66', 'Home Goods', 54.11),
(67, 11, 'Product_67', 'Electronics', 443.63),
(68, 45, 'Product_68', 'Industrial', 253.99),
(69, 46, 'Product_69', 'Industrial', 58.19),
(70, 5, 'Product_70', 'Automotive', 64.77),
(71, 19, 'Product_71', 'Industrial', 318.61),
(72, 31, 'Product_72', 'Chemicals', 140.64),
(73, 23, 'Product_73', 'Automotive', 122.32),
(74, 31, 'Product_74', 'Chemicals', 212.32),
(75, 17, 'Product_75', 'Chemicals', 350.85),
(76, 19, 'Product_76', 'Industrial', 370.19),
(77, 29, 'Product_77', 'Electronics', 91.49),
(78, 42, 'Product_78', 'Home Goods', 182.56),
(79, 27, 'Product_79', 'Chemicals', 99.95),
(80, 36, 'Product_80', 'Industrial', 100.65),
(81, 37, 'Product_81', 'Chemicals', 432.01),
(82, 21, 'Product_82', 'Home Goods', 377.88),
(83, 31, 'Product_83', 'Industrial', 335.47),
(84, 25, 'Product_84', 'Electronics', 399.78),
(85, 24, 'Product_85', 'Industrial', 439.48),
(86, 47, 'Product_86', 'Industrial', 101.06),
(87, 16, 'Product_87', 'Automotive', 351.91),
(88, 6, 'Product_88', 'Electronics', 300.1),
(89, 37, 'Product_89', 'Automotive', 217.9),
(90, 15, 'Product_90', 'Home Goods', 272.96),
(91, 17, 'Product_91', 'Electronics', 204.67),
(92, 30, 'Product_92', 'Industrial', 185.99),
(93, 24, 'Product_93', 'Home Goods', 17.45),
(94, 7, 'Product_94', 'Home Goods', 54.91),
(95, 47, 'Product_95', 'Chemicals', 133.64),
(96, 22, 'Product_96', 'Industrial', 331.14),
(97, 16, 'Product_97', 'Home Goods', 23.59),
(98, 7, 'Product_98', 'Electronics', 272.48),
(99, 44, 'Product_99', 'Home Goods', 89.47),
(100, 25, 'Product_100', 'Industrial', 217.43);

-- Insert 100 records into SupplyChainDB.Bronze.Inventory
INSERT INTO SupplyChainDB.Bronze.Inventory (InventoryID, ProductID, Zone, QuantityOnHand, ReorderLevel, LastCountDate) VALUES
(1, 1, 'Zone D', 906, 165, '2025-03-29'),
(2, 2, 'Zone B', 386, 101, '2025-03-21'),
(3, 3, 'Zone C', 830, 79, '2025-01-06'),
(4, 4, 'Zone D', 843, 137, '2025-02-08'),
(5, 5, 'Zone B', 848, 127, '2025-01-18'),
(6, 6, 'Zone B', 266, 52, '2025-03-02'),
(7, 7, 'Zone B', 702, 66, '2025-04-25'),
(8, 8, 'Zone D', 449, 157, '2025-04-19'),
(9, 9, 'Zone B', 901, 147, '2025-01-18'),
(10, 10, 'Zone A', 663, 168, '2025-02-04'),
(11, 11, 'Zone D', 365, 95, '2025-04-18'),
(12, 12, 'Zone C', 494, 163, '2025-03-12'),
(13, 13, 'Zone C', 91, 67, '2025-01-18'),
(14, 14, 'Zone B', 173, 178, '2025-03-10'),
(15, 15, 'Zone C', 149, 179, '2025-01-08'),
(16, 16, 'Zone A', 990, 190, '2025-01-02'),
(17, 17, 'Zone A', 304, 166, '2025-02-14'),
(18, 18, 'Zone D', 367, 67, '2025-04-19'),
(19, 19, 'Zone A', 21, 126, '2025-02-15'),
(20, 20, 'Zone A', 145, 50, '2025-02-23'),
(21, 21, 'Zone B', 245, 184, '2025-01-29'),
(22, 22, 'Zone A', 294, 71, '2025-01-25'),
(23, 23, 'Zone A', 324, 143, '2025-04-19'),
(24, 24, 'Zone D', 326, 57, '2025-03-25'),
(25, 25, 'Zone B', 616, 107, '2025-01-15'),
(26, 26, 'Zone B', 976, 165, '2025-01-29'),
(27, 27, 'Zone D', 498, 96, '2025-03-14'),
(28, 28, 'Zone D', 220, 96, '2025-04-17'),
(29, 29, 'Zone D', 520, 149, '2025-03-18'),
(30, 30, 'Zone C', 470, 87, '2025-03-12'),
(31, 31, 'Zone C', 790, 55, '2025-01-31'),
(32, 32, 'Zone B', 930, 153, '2025-04-12'),
(33, 33, 'Zone A', 973, 171, '2025-01-13'),
(34, 34, 'Zone C', 734, 147, '2025-04-07'),
(35, 35, 'Zone A', 324, 88, '2025-01-01'),
(36, 36, 'Zone D', 977, 152, '2025-04-22'),
(37, 37, 'Zone D', 876, 189, '2025-02-15'),
(38, 38, 'Zone D', 279, 137, '2025-04-27'),
(39, 39, 'Zone D', 580, 119, '2025-04-08'),
(40, 40, 'Zone C', 867, 108, '2025-03-09'),
(41, 41, 'Zone A', 142, 106, '2025-04-22'),
(42, 42, 'Zone D', 479, 182, '2025-01-23'),
(43, 43, 'Zone A', 349, 72, '2025-02-17'),
(44, 44, 'Zone C', 137, 149, '2025-04-10'),
(45, 45, 'Zone D', 545, 96, '2025-04-06'),
(46, 46, 'Zone D', 994, 113, '2025-03-19'),
(47, 47, 'Zone C', 355, 55, '2025-03-30'),
(48, 48, 'Zone C', 480, 107, '2025-01-04'),
(49, 49, 'Zone C', 34, 186, '2025-02-25'),
(50, 50, 'Zone A', 994, 144, '2025-02-27'),
(51, 51, 'Zone D', 897, 81, '2025-03-04'),
(52, 52, 'Zone B', 109, 133, '2025-04-04'),
(53, 53, 'Zone D', 530, 184, '2025-01-15'),
(54, 54, 'Zone B', 612, 96, '2025-01-18'),
(55, 55, 'Zone C', 999, 50, '2025-04-04'),
(56, 56, 'Zone C', 321, 51, '2025-03-30'),
(57, 57, 'Zone D', 324, 133, '2025-01-30'),
(58, 58, 'Zone C', 676, 89, '2025-03-31'),
(59, 59, 'Zone A', 688, 85, '2025-04-11'),
(60, 60, 'Zone B', 694, 65, '2025-04-04'),
(61, 61, 'Zone A', 754, 57, '2025-01-15'),
(62, 62, 'Zone A', 944, 161, '2025-02-23'),
(63, 63, 'Zone D', 581, 136, '2025-01-04'),
(64, 64, 'Zone A', 56, 195, '2025-02-23'),
(65, 65, 'Zone D', 833, 176, '2025-01-06'),
(66, 66, 'Zone C', 422, 150, '2025-03-19'),
(67, 67, 'Zone A', 721, 177, '2025-03-31'),
(68, 68, 'Zone C', 761, 57, '2025-01-31'),
(69, 69, 'Zone B', 456, 150, '2025-04-23'),
(70, 70, 'Zone B', 781, 137, '2025-03-15'),
(71, 71, 'Zone A', 993, 124, '2025-01-25'),
(72, 72, 'Zone C', 87, 99, '2025-02-10'),
(73, 73, 'Zone D', 570, 173, '2025-03-04'),
(74, 74, 'Zone A', 264, 150, '2025-03-28'),
(75, 75, 'Zone B', 194, 67, '2025-03-23'),
(76, 76, 'Zone A', 238, 123, '2025-02-07'),
(77, 77, 'Zone A', 117, 66, '2025-03-06'),
(78, 78, 'Zone A', 450, 81, '2025-03-01'),
(79, 79, 'Zone C', 939, 156, '2025-03-14'),
(80, 80, 'Zone B', 895, 171, '2025-04-10'),
(81, 81, 'Zone B', 372, 142, '2025-01-06'),
(82, 82, 'Zone B', 284, 152, '2025-01-30'),
(83, 83, 'Zone A', 134, 130, '2025-04-05'),
(84, 84, 'Zone D', 436, 85, '2025-02-17'),
(85, 85, 'Zone C', 367, 138, '2025-04-02'),
(86, 86, 'Zone A', 47, 106, '2025-03-02'),
(87, 87, 'Zone A', 946, 183, '2025-03-13'),
(88, 88, 'Zone A', 792, 54, '2025-02-18'),
(89, 89, 'Zone C', 397, 108, '2025-04-26'),
(90, 90, 'Zone C', 291, 112, '2025-03-03'),
(91, 91, 'Zone A', 257, 168, '2025-02-11'),
(92, 92, 'Zone A', 47, 135, '2025-04-18'),
(93, 93, 'Zone D', 710, 157, '2025-03-15'),
(94, 94, 'Zone D', 972, 179, '2025-03-10'),
(95, 95, 'Zone A', 53, 148, '2025-01-16'),
(96, 96, 'Zone D', 54, 195, '2025-01-11'),
(97, 97, 'Zone A', 145, 147, '2025-04-23'),
(98, 98, 'Zone D', 888, 98, '2025-02-07'),
(99, 99, 'Zone B', 490, 183, '2025-04-19'),
(100, 100, 'Zone C', 548, 126, '2025-01-29');

-- Insert 100 records into SupplyChainDB.Bronze.Shipments
INSERT INTO SupplyChainDB.Bronze.Shipments (ShipmentID, SupplierID, ProductID, Quantity, ShipmentDate, ExpectedArrival, Status) VALUES
(1, 5, 30, 365, '2025-02-24', '2025-03-21', 'Delayed'),
(2, 22, 53, 4236, '2025-01-04', '2025-01-14', 'Delivered'),
(3, 40, 84, 3266, '2025-01-10', '2025-01-29', 'In Transit'),
(4, 11, 93, 3420, '2025-04-10', '2025-05-17', 'Delivered'),
(5, 9, 28, 2611, '2025-01-09', '2025-02-14', 'Delivered'),
(6, 25, 77, 2010, '2025-03-30', '2025-04-13', 'Delivered'),
(7, 13, 98, 1222, '2025-03-08', '2025-04-13', 'Pending'),
(8, 35, 6, 3791, '2025-04-17', '2025-05-31', 'In Transit'),
(9, 30, 30, 971, '2025-02-26', '2025-03-27', 'Delivered'),
(10, 33, 58, 1498, '2025-02-06', '2025-02-19', 'In Transit'),
(11, 35, 51, 4362, '2025-02-11', '2025-03-04', 'Delivered'),
(12, 29, 96, 4778, '2025-04-12', '2025-04-25', 'Delayed'),
(13, 23, 69, 4152, '2025-03-25', '2025-05-08', 'In Transit'),
(14, 1, 49, 2390, '2025-02-02', '2025-03-14', 'Delivered'),
(15, 29, 24, 1741, '2025-03-30', '2025-04-19', 'Delivered'),
(16, 22, 15, 3909, '2025-01-29', '2025-02-23', 'Delivered'),
(17, 18, 42, 1589, '2025-04-23', '2025-05-07', 'Delivered'),
(18, 25, 68, 2448, '2025-03-16', '2025-03-29', 'Delivered'),
(19, 17, 67, 330, '2025-04-18', '2025-05-27', 'Delivered'),
(20, 16, 8, 213, '2025-02-01', '2025-03-15', 'In Transit'),
(21, 27, 53, 1068, '2025-01-18', '2025-01-24', 'Delivered'),
(22, 46, 25, 2449, '2025-03-23', '2025-04-01', 'Delivered'),
(23, 39, 63, 4294, '2025-03-10', '2025-03-29', 'In Transit'),
(24, 6, 10, 3947, '2025-01-15', '2025-02-04', 'Delayed'),
(25, 6, 45, 2269, '2025-04-10', '2025-05-20', 'In Transit'),
(26, 44, 3, 1453, '2025-04-12', '2025-05-22', 'In Transit'),
(27, 16, 50, 1370, '2025-03-17', '2025-03-31', 'Delivered'),
(28, 26, 61, 3339, '2025-04-01', '2025-04-19', 'Delivered'),
(29, 25, 95, 3984, '2025-04-22', '2025-05-27', 'In Transit'),
(30, 37, 84, 2768, '2025-02-20', '2025-02-25', 'In Transit'),
(31, 5, 22, 2025, '2025-01-17', '2025-02-14', 'In Transit'),
(32, 37, 42, 370, '2025-01-05', '2025-02-15', 'In Transit'),
(33, 11, 29, 1517, '2025-02-26', '2025-03-26', 'Delivered'),
(34, 43, 59, 4704, '2025-03-27', '2025-05-05', 'Pending'),
(35, 45, 1, 1953, '2025-02-07', '2025-03-14', 'Delayed'),
(36, 50, 97, 847, '2025-03-30', '2025-05-08', 'In Transit'),
(37, 10, 68, 4141, '2025-03-05', '2025-03-21', 'In Transit'),
(38, 38, 91, 1055, '2025-02-18', '2025-03-20', 'In Transit'),
(39, 30, 63, 3771, '2025-01-15', '2025-01-31', 'In Transit'),
(40, 17, 19, 2813, '2025-03-18', '2025-04-03', 'Delivered'),
(41, 41, 56, 3111, '2025-03-04', '2025-03-10', 'Delivered'),
(42, 36, 24, 776, '2025-03-07', '2025-04-18', 'In Transit'),
(43, 38, 5, 4767, '2025-01-07', '2025-01-12', 'In Transit'),
(44, 8, 6, 4521, '2025-02-24', '2025-04-09', 'Delivered'),
(45, 7, 70, 1861, '2025-02-24', '2025-03-14', 'In Transit'),
(46, 31, 15, 2880, '2025-03-08', '2025-03-24', 'Delivered'),
(47, 8, 60, 3445, '2025-04-07', '2025-04-25', 'In Transit'),
(48, 33, 82, 3179, '2025-04-22', '2025-04-30', 'In Transit'),
(49, 10, 40, 2776, '2025-02-21', '2025-02-27', 'Delivered'),
(50, 38, 11, 4044, '2025-01-11', '2025-02-07', 'Delivered'),
(51, 38, 2, 4003, '2025-01-19', '2025-02-04', 'Delivered'),
(52, 22, 16, 2205, '2025-02-17', '2025-03-30', 'Delivered'),
(53, 46, 24, 3339, '2025-04-11', '2025-04-20', 'Delayed'),
(54, 41, 1, 1994, '2025-04-07', '2025-04-21', 'Delayed'),
(55, 1, 82, 2524, '2025-01-10', '2025-02-14', 'Delivered'),
(56, 27, 97, 861, '2025-04-04', '2025-04-10', 'Delayed'),
(57, 6, 73, 4298, '2025-01-06', '2025-01-21', 'Delivered'),
(58, 20, 49, 4725, '2025-04-16', '2025-05-15', 'Delivered'),
(59, 25, 74, 2765, '2025-04-08', '2025-05-02', 'In Transit'),
(60, 28, 18, 2455, '2025-03-07', '2025-04-02', 'Delivered'),
(61, 31, 28, 244, '2025-04-21', '2025-05-19', 'Pending'),
(62, 39, 93, 2041, '2025-01-23', '2025-02-06', 'Delivered'),
(63, 28, 17, 3862, '2025-02-14', '2025-02-20', 'In Transit'),
(64, 38, 66, 2110, '2025-02-04', '2025-02-12', 'In Transit'),
(65, 16, 71, 4620, '2025-04-11', '2025-05-19', 'Delivered'),
(66, 21, 60, 3390, '2025-02-23', '2025-03-13', 'Delayed'),
(67, 18, 34, 4384, '2025-02-17', '2025-03-10', 'In Transit'),
(68, 14, 4, 4439, '2025-02-07', '2025-03-13', 'Delivered'),
(69, 28, 97, 459, '2025-03-26', '2025-04-06', 'In Transit'),
(70, 14, 51, 4682, '2025-01-27', '2025-03-02', 'In Transit'),
(71, 25, 82, 2841, '2025-01-28', '2025-02-13', 'In Transit'),
(72, 28, 31, 677, '2025-02-22', '2025-04-04', 'Delayed'),
(73, 18, 59, 4468, '2025-04-12', '2025-05-10', 'In Transit'),
(74, 35, 24, 650, '2025-03-24', '2025-04-16', 'Delayed'),
(75, 26, 49, 2266, '2025-04-20', '2025-05-05', 'In Transit'),
(76, 47, 50, 852, '2025-04-23', '2025-05-18', 'Delivered'),
(77, 44, 67, 397, '2025-03-31', '2025-04-25', 'Pending'),
(78, 45, 23, 4544, '2025-02-15', '2025-03-21', 'In Transit'),
(79, 2, 7, 1261, '2025-04-27', '2025-05-05', 'Delivered'),
(80, 34, 21, 4931, '2025-03-16', '2025-03-29', 'Delivered'),
(81, 46, 52, 3558, '2025-02-21', '2025-03-22', 'In Transit'),
(82, 9, 100, 2311, '2025-01-09', '2025-01-18', 'Delivered'),
(83, 8, 61, 4843, '2025-04-27', '2025-05-13', 'Delayed'),
(84, 31, 78, 3251, '2025-04-15', '2025-05-23', 'In Transit'),
(85, 23, 36, 2959, '2025-01-13', '2025-01-27', 'In Transit'),
(86, 8, 55, 2099, '2025-04-16', '2025-04-23', 'Delivered'),
(87, 11, 94, 2619, '2025-03-29', '2025-05-01', 'In Transit'),
(88, 24, 10, 4244, '2025-03-19', '2025-03-27', 'Delayed'),
(89, 38, 17, 1026, '2025-02-17', '2025-02-22', 'In Transit'),
(90, 2, 32, 508, '2025-02-09', '2025-03-01', 'Delivered'),
(91, 32, 27, 3846, '2025-01-01', '2025-01-08', 'Delivered'),
(92, 32, 74, 1689, '2025-03-05', '2025-03-13', 'In Transit'),
(93, 8, 43, 3628, '2025-02-17', '2025-03-05', 'In Transit'),
(94, 2, 73, 1510, '2025-02-01', '2025-02-24', 'Delivered'),
(95, 30, 48, 1370, '2025-04-12', '2025-05-10', 'Delivered'),
(96, 26, 39, 2268, '2025-03-18', '2025-04-06', 'Delivered'),
(97, 14, 53, 1236, '2025-01-17', '2025-02-04', 'Delivered'),
(98, 18, 92, 3885, '2025-04-23', '2025-06-04', 'Delivered'),
(99, 34, 59, 362, '2025-02-10', '2025-03-23', 'Delayed'),
(100, 11, 53, 4422, '2025-02-01', '2025-03-12', 'Delivered');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Enrichment & Cleanup
-------------------------------------------------------------------------------

-- 2.1 View: Current_Stock
-- Joins Inventory with Product details.
CREATE OR REPLACE VIEW SupplyChainDB.Silver.Current_Stock AS
SELECT
    i.InventoryID,
    p.Name AS ProductName,
    p.Category,
    i.Zone,
    i.QuantityOnHand,
    i.ReorderLevel,
    (i.QuantityOnHand * p.UnitCost) AS InventoryValue,
    CASE WHEN i.QuantityOnHand < i.ReorderLevel THEN 'REORDER_NOW' ELSE 'OK' END AS StockStatus
FROM SupplyChainDB.Bronze.Inventory i
JOIN SupplyChainDB.Bronze.Products p ON i.ProductID = p.ProductID;

-- 2.2 View: Shipment_Delays
-- Calculates delay in days for delivered items or potential delay for in-transit.
CREATE OR REPLACE VIEW SupplyChainDB.Silver.Shipment_Delays AS
SELECT
    s.ShipmentID,
    sup.Name AS SupplierName,
    p.Name AS ProductName,
    s.Quantity,
    s.Status,
    s.ExpectedArrival,
    s.ShipmentDate,
    DATEDIFF(day, s.ShipmentDate, s.ExpectedArrival) AS EstimatedTransitDays
FROM SupplyChainDB.Bronze.Shipments s
JOIN SupplyChainDB.Bronze.Suppliers sup ON s.SupplierID = sup.SupplierID
JOIN SupplyChainDB.Bronze.Products p ON s.ProductID = p.ProductID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Strategic Metrics
-------------------------------------------------------------------------------

-- 3.1 View: Inventory_Turnover_Rate
-- Aggregates inventory value by Category.
CREATE OR REPLACE VIEW SupplyChainDB.Gold.Stock_Value_By_Category AS
SELECT
    Category,
    COUNT(DISTINCT ProductName) AS ProductCount,
    SUM(QuantityOnHand) AS TotalUnits,
    SUM(InventoryValue) AS TotalValue,
    COUNT(CASE WHEN StockStatus = 'REORDER_NOW' THEN 1 END) AS LowStockItems
FROM SupplyChainDB.Silver.Current_Stock
GROUP BY Category;

-- 3.2 View: Supplier_Reliability_Score
-- Analyzes shipment volume and supplier baseline reliability.
CREATE OR REPLACE VIEW SupplyChainDB.Gold.Supplier_Reliability AS
SELECT
    SupplierName,
    COUNT(ShipmentID) AS TotalShipments,
    SUM(Quantity) AS TotalVolumeShipped
FROM SupplyChainDB.Silver.Shipment_Delays
GROUP BY SupplierName;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Stock Analysis):
"Using SupplyChainDB.Gold.Stock_Value_By_Category, show me a bar chart of TotalValue by Category."

PROMPT 2 (Reorder Action):
"List all products from SupplyChainDB.Silver.Current_Stock where StockStatus is 'REORDER_NOW', sorted by Zone."

PROMPT 3 (Supplier Volume):
"From SupplyChainDB.Gold.Supplier_Reliability, who are my top 5 suppliers by TotalVolumeShipped?"
*/
