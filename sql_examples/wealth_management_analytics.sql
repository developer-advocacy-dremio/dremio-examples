/*
 * Wealth Management Analytics Demo
 * 
 * Scenario:
 * A wealth management firm wants to track client assets, advisor performance, and retention risk.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: optimizing portfolio allocations and identifying high-risk clients.
 * 
 * Note: Assumes a catalog named 'WealthDB' exists. 
 *       If using a different source, find/replace 'WealthDB' with your source name.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS WealthDB.Bronze;
CREATE FOLDER IF NOT EXISTS WealthDB.Silver;
CREATE FOLDER IF NOT EXISTS WealthDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data Ingestion
-------------------------------------------------------------------------------

-- 1.1 Create Clients Table
CREATE TABLE IF NOT EXISTS WealthDB.Bronze.Clients (
    ClientID INT,
    Name VARCHAR,
    Region VARCHAR,
    RiskProfile VARCHAR, -- 'Conservative', 'Moderate', 'Aggressive'
    AdvisorID INT,
    OnboardingDate DATE
);

-- 1.2 Create Assets Table
CREATE TABLE IF NOT EXISTS WealthDB.Bronze.Assets (
    AssetID INT,
    ClientID INT,
    AssetClass VARCHAR, -- 'Equities', 'Fixed Income', 'Cash', etc.
    MarketValue DOUBLE,
    CostBasis DOUBLE,
    LastUpdated DATE
);

-- 1.3 Populate Bronze Tables (Generated Data)
-- Insert 100 records into WealthDB.Bronze.Clients
INSERT INTO WealthDB.Bronze.Clients (ClientID, Name, Region, RiskProfile, AdvisorID, OnboardingDate) VALUES
(1, 'Client_1', 'APAC', 'Conservative', 105, '2023-11-04'),
(2, 'Client_2', 'EMEA', 'Conservative', 101, '2023-01-21'),
(3, 'Client_3', 'LATAM', 'Moderate', 105, '2024-01-04'),
(4, 'Client_4', 'EMEA', 'Moderate', 103, '2020-08-23'),
(5, 'Client_5', 'LATAM', 'Conservative', 105, '2021-01-20'),
(6, 'Client_6', 'APAC', 'Conservative', 104, '2022-12-01'),
(7, 'Client_7', 'North America', 'Conservative', 103, '2020-12-16'),
(8, 'Client_8', 'North America', 'Moderate', 105, '2020-03-20'),
(9, 'Client_9', 'EMEA', 'Moderate', 103, '2024-05-25'),
(10, 'Client_10', 'LATAM', 'Aggressive', 103, '2022-01-17'),
(11, 'Client_11', 'EMEA', 'Aggressive', 104, '2021-10-02'),
(12, 'Client_12', 'North America', 'Aggressive', 105, '2022-10-11'),
(13, 'Client_13', 'LATAM', 'Moderate', 103, '2023-05-02'),
(14, 'Client_14', 'North America', 'Conservative', 101, '2024-01-02'),
(15, 'Client_15', 'APAC', 'Conservative', 105, '2021-10-16'),
(16, 'Client_16', 'APAC', 'Aggressive', 101, '2020-10-06'),
(17, 'Client_17', 'LATAM', 'Aggressive', 105, '2021-06-05'),
(18, 'Client_18', 'APAC', 'Conservative', 104, '2021-10-05'),
(19, 'Client_19', 'APAC', 'Conservative', 103, '2020-07-06'),
(20, 'Client_20', 'APAC', 'Moderate', 103, '2022-04-30'),
(21, 'Client_21', 'LATAM', 'Conservative', 102, '2022-07-08'),
(22, 'Client_22', 'North America', 'Aggressive', 105, '2024-07-28'),
(23, 'Client_23', 'EMEA', 'Conservative', 103, '2021-02-18'),
(24, 'Client_24', 'APAC', 'Moderate', 104, '2021-07-28'),
(25, 'Client_25', 'North America', 'Conservative', 102, '2020-09-26'),
(26, 'Client_26', 'LATAM', 'Moderate', 104, '2022-12-10'),
(27, 'Client_27', 'EMEA', 'Conservative', 105, '2023-08-17'),
(28, 'Client_28', 'LATAM', 'Moderate', 104, '2023-05-12'),
(29, 'Client_29', 'EMEA', 'Moderate', 101, '2023-03-15'),
(30, 'Client_30', 'LATAM', 'Moderate', 102, '2024-01-26'),
(31, 'Client_31', 'LATAM', 'Aggressive', 103, '2023-11-16'),
(32, 'Client_32', 'LATAM', 'Aggressive', 105, '2023-12-14'),
(33, 'Client_33', 'LATAM', 'Conservative', 104, '2021-10-09'),
(34, 'Client_34', 'EMEA', 'Aggressive', 105, '2024-04-05'),
(35, 'Client_35', 'EMEA', 'Aggressive', 101, '2024-04-19'),
(36, 'Client_36', 'North America', 'Aggressive', 103, '2020-07-21'),
(37, 'Client_37', 'North America', 'Aggressive', 104, '2022-03-23'),
(38, 'Client_38', 'EMEA', 'Aggressive', 101, '2021-06-28'),
(39, 'Client_39', 'EMEA', 'Conservative', 105, '2020-05-07'),
(40, 'Client_40', 'North America', 'Moderate', 105, '2020-01-31'),
(41, 'Client_41', 'EMEA', 'Moderate', 105, '2022-07-15'),
(42, 'Client_42', 'North America', 'Moderate', 105, '2024-02-22'),
(43, 'Client_43', 'EMEA', 'Conservative', 103, '2023-10-28'),
(44, 'Client_44', 'North America', 'Conservative', 103, '2022-12-03'),
(45, 'Client_45', 'LATAM', 'Moderate', 102, '2022-08-04'),
(46, 'Client_46', 'North America', 'Conservative', 105, '2021-04-27'),
(47, 'Client_47', 'EMEA', 'Aggressive', 102, '2021-05-20'),
(48, 'Client_48', 'North America', 'Aggressive', 101, '2021-04-16'),
(49, 'Client_49', 'EMEA', 'Aggressive', 104, '2020-05-06'),
(50, 'Client_50', 'LATAM', 'Moderate', 101, '2020-11-10'),
(51, 'Client_51', 'North America', 'Conservative', 101, '2023-05-17'),
(52, 'Client_52', 'EMEA', 'Moderate', 102, '2020-07-09'),
(53, 'Client_53', 'LATAM', 'Conservative', 104, '2020-01-21'),
(54, 'Client_54', 'EMEA', 'Conservative', 103, '2023-12-30'),
(55, 'Client_55', 'LATAM', 'Conservative', 105, '2021-06-30'),
(56, 'Client_56', 'EMEA', 'Moderate', 104, '2022-02-15'),
(57, 'Client_57', 'EMEA', 'Moderate', 101, '2023-01-15'),
(58, 'Client_58', 'EMEA', 'Moderate', 104, '2021-10-14'),
(59, 'Client_59', 'APAC', 'Moderate', 101, '2024-01-24'),
(60, 'Client_60', 'APAC', 'Conservative', 101, '2024-12-09'),
(61, 'Client_61', 'EMEA', 'Aggressive', 102, '2022-10-22'),
(62, 'Client_62', 'EMEA', 'Moderate', 105, '2020-07-15'),
(63, 'Client_63', 'LATAM', 'Aggressive', 103, '2020-03-07'),
(64, 'Client_64', 'APAC', 'Conservative', 105, '2020-11-04'),
(65, 'Client_65', 'North America', 'Conservative', 105, '2020-07-04'),
(66, 'Client_66', 'APAC', 'Moderate', 102, '2024-01-05'),
(67, 'Client_67', 'North America', 'Aggressive', 101, '2021-08-11'),
(68, 'Client_68', 'EMEA', 'Moderate', 103, '2022-09-09'),
(69, 'Client_69', 'LATAM', 'Conservative', 101, '2021-05-19'),
(70, 'Client_70', 'LATAM', 'Aggressive', 105, '2020-02-22'),
(71, 'Client_71', 'LATAM', 'Moderate', 105, '2020-07-09'),
(72, 'Client_72', 'LATAM', 'Moderate', 104, '2020-08-05'),
(73, 'Client_73', 'LATAM', 'Conservative', 103, '2022-01-26'),
(74, 'Client_74', 'APAC', 'Aggressive', 101, '2023-04-25'),
(75, 'Client_75', 'North America', 'Moderate', 104, '2022-06-20'),
(76, 'Client_76', 'EMEA', 'Aggressive', 102, '2021-09-23'),
(77, 'Client_77', 'LATAM', 'Conservative', 102, '2024-11-19'),
(78, 'Client_78', 'APAC', 'Aggressive', 105, '2020-05-20'),
(79, 'Client_79', 'North America', 'Moderate', 105, '2024-10-23'),
(80, 'Client_80', 'EMEA', 'Conservative', 103, '2020-01-03'),
(81, 'Client_81', 'LATAM', 'Conservative', 101, '2024-07-20'),
(82, 'Client_82', 'North America', 'Aggressive', 103, '2021-09-18'),
(83, 'Client_83', 'North America', 'Conservative', 102, '2020-04-28'),
(84, 'Client_84', 'LATAM', 'Conservative', 104, '2024-12-16'),
(85, 'Client_85', 'LATAM', 'Aggressive', 105, '2023-10-27'),
(86, 'Client_86', 'LATAM', 'Moderate', 102, '2024-10-23'),
(87, 'Client_87', 'LATAM', 'Conservative', 104, '2022-03-03'),
(88, 'Client_88', 'APAC', 'Conservative', 101, '2021-09-06'),
(89, 'Client_89', 'LATAM', 'Conservative', 104, '2021-10-10'),
(90, 'Client_90', 'North America', 'Moderate', 105, '2020-03-16'),
(91, 'Client_91', 'EMEA', 'Moderate', 105, '2023-12-23'),
(92, 'Client_92', 'EMEA', 'Moderate', 103, '2022-04-07'),
(93, 'Client_93', 'EMEA', 'Moderate', 103, '2021-10-22'),
(94, 'Client_94', 'North America', 'Moderate', 102, '2023-05-13'),
(95, 'Client_95', 'EMEA', 'Moderate', 102, '2020-06-20'),
(96, 'Client_96', 'LATAM', 'Aggressive', 102, '2020-04-17'),
(97, 'Client_97', 'North America', 'Conservative', 104, '2024-06-29'),
(98, 'Client_98', 'APAC', 'Conservative', 105, '2021-10-17'),
(99, 'Client_99', 'LATAM', 'Conservative', 103, '2022-08-14'),
(100, 'Client_100', 'LATAM', 'Aggressive', 101, '2024-08-27');

-- Insert ~300 records into WealthDB.Bronze.Assets
INSERT INTO WealthDB.Bronze.Assets (AssetID, ClientID, AssetClass, MarketValue, CostBasis, LastUpdated) VALUES
(1, 1, 'Alternatives', 907240.15, 710793.77, '2025-04-15'),
(2, 1, 'Cash', 296147.0, 296147.0, '2025-04-15'),
(3, 1, 'Cash', 948032.53, 948032.53, '2025-04-15'),
(4, 1, 'Equities', 309510.24, 254426.63, '2025-04-15'),
(5, 2, 'Cash', 850472.28, 850472.28, '2025-04-15'),
(6, 2, 'Real Estate', 130865.22, 94507.99, '2025-04-15'),
(7, 2, 'Equities', 459533.95, 407917.04, '2025-04-15'),
(8, 2, 'Real Estate', 311464.17, 298795.59, '2025-04-15'),
(9, 3, 'Fixed Income', 332983.39, 269400.38, '2025-04-15'),
(10, 3, 'Alternatives', 305567.55, 283131.23, '2025-04-15'),
(11, 3, 'Alternatives', 1231449.01, 959965.68, '2025-04-15'),
(12, 3, 'Equities', 1134672.14, 818386.28, '2025-04-15'),
(13, 3, 'Cash', 516835.59, 516835.59, '2025-04-15'),
(14, 4, 'Equities', 1246007.59, 920861.61, '2025-04-15'),
(15, 4, 'Real Estate', 449231.6, 367217.72, '2025-04-15'),
(16, 4, 'Cash', 932906.16, 932906.16, '2025-04-15'),
(17, 4, 'Alternatives', 572538.67, 491280.58, '2025-04-15'),
(18, 4, 'Alternatives', 238947.89, 233751.96, '2025-04-15'),
(19, 5, 'Alternatives', 1177908.83, 856452.36, '2025-04-15'),
(20, 6, 'Cash', 883816.71, 883816.71, '2025-04-15'),
(21, 7, 'Real Estate', 1045895.72, 909807.36, '2025-04-15'),
(22, 8, 'Equities', 1191107.45, 858470.21, '2025-04-15'),
(23, 8, 'Fixed Income', 1094209.25, 910366.49, '2025-04-15'),
(24, 8, 'Real Estate', 267099.46, 265763.86, '2025-04-15'),
(25, 9, 'Equities', 1240311.51, 995982.8, '2025-04-15'),
(26, 9, 'Alternatives', 1032733.96, 742155.57, '2025-04-15'),
(27, 9, 'Equities', 660661.68, 615135.05, '2025-04-15'),
(28, 10, 'Equities', 250926.46, 206257.94, '2025-04-15'),
(29, 10, 'Fixed Income', 537993.46, 511291.48, '2025-04-15'),
(30, 10, 'Alternatives', 179104.9, 173145.98, '2025-04-15'),
(31, 10, 'Real Estate', 136014.67, 141024.26, '2025-04-15'),
(32, 11, 'Equities', 140140.71, 128625.86, '2025-04-15'),
(33, 12, 'Fixed Income', 479686.43, 348778.3, '2025-04-15'),
(34, 12, 'Fixed Income', 530690.71, 637542.59, '2025-04-15'),
(35, 12, 'Alternatives', 49805.89, 37643.57, '2025-04-15'),
(36, 12, 'Cash', 627988.79, 627988.79, '2025-04-15'),
(37, 13, 'Real Estate', 1170945.22, 958848.33, '2025-04-15'),
(38, 13, 'Cash', 41164.88, 41164.88, '2025-04-15'),
(39, 13, 'Alternatives', 412435.71, 467959.25, '2025-04-15'),
(40, 14, 'Equities', 149022.21, 107605.32, '2025-04-15'),
(41, 14, 'Alternatives', 686485.47, 687638.69, '2025-04-15'),
(42, 14, 'Cash', 586702.77, 586702.77, '2025-04-15'),
(43, 14, 'Fixed Income', 246630.04, 215060.6, '2025-04-15'),
(44, 14, 'Real Estate', 508562.99, 410493.99, '2025-04-15'),
(45, 15, 'Alternatives', 466482.7, 397563.8, '2025-04-15'),
(46, 15, 'Real Estate', 403661.73, 348596.44, '2025-04-15'),
(47, 15, 'Cash', 324682.35, 324682.35, '2025-04-15'),
(48, 15, 'Equities', 454862.23, 409166.51, '2025-04-15'),
(49, 16, 'Alternatives', 118519.79, 96535.24, '2025-04-15'),
(50, 17, 'Fixed Income', 985606.64, 786162.32, '2025-04-15'),
(51, 17, 'Equities', 363827.61, 289553.19, '2025-04-15'),
(52, 17, 'Alternatives', 328768.28, 336011.15, '2025-04-15'),
(53, 17, 'Equities', 833469.85, 870756.84, '2025-04-15'),
(54, 17, 'Cash', 929346.54, 929346.54, '2025-04-15'),
(55, 18, 'Equities', 139958.57, 141347.54, '2025-04-15'),
(56, 18, 'Real Estate', 695407.85, 550132.95, '2025-04-15'),
(57, 18, 'Equities', 1264263.59, 912256.7, '2025-04-15'),
(58, 18, 'Cash', 334525.76, 334525.76, '2025-04-15'),
(59, 19, 'Equities', 630135.94, 751255.23, '2025-04-15'),
(60, 20, 'Alternatives', 35556.74, 26885.84, '2025-04-15'),
(61, 20, 'Cash', 868019.41, 868019.41, '2025-04-15'),
(62, 20, 'Real Estate', 886816.46, 982857.93, '2025-04-15'),
(63, 20, 'Fixed Income', 158508.08, 138905.19, '2025-04-15'),
(64, 20, 'Cash', 537121.58, 537121.58, '2025-04-15'),
(65, 21, 'Fixed Income', 175214.03, 171473.0, '2025-04-15'),
(66, 21, 'Fixed Income', 856660.21, 686846.19, '2025-04-15'),
(67, 21, 'Cash', 864998.76, 864998.76, '2025-04-15'),
(68, 21, 'Cash', 673232.58, 673232.58, '2025-04-15'),
(69, 21, 'Fixed Income', 204842.66, 180285.25, '2025-04-15'),
(70, 22, 'Real Estate', 1196740.16, 989316.65, '2025-04-15'),
(71, 22, 'Cash', 367515.26, 367515.26, '2025-04-15'),
(72, 22, 'Alternatives', 81021.91, 85022.81, '2025-04-15'),
(73, 23, 'Cash', 102765.73, 102765.73, '2025-04-15'),
(74, 23, 'Real Estate', 20626.79, 24560.84, '2025-04-15'),
(75, 23, 'Real Estate', 510459.53, 384144.34, '2025-04-15'),
(76, 24, 'Cash', 137553.04, 137553.04, '2025-04-15'),
(77, 25, 'Cash', 702391.5, 702391.5, '2025-04-15'),
(78, 25, 'Real Estate', 1165865.57, 989976.87, '2025-04-15'),
(79, 25, 'Equities', 1008399.75, 852538.45, '2025-04-15'),
(80, 25, 'Equities', 865365.98, 626611.09, '2025-04-15'),
(81, 25, 'Cash', 641651.83, 641651.83, '2025-04-15'),
(82, 26, 'Equities', 83747.39, 100092.35, '2025-04-15'),
(83, 26, 'Alternatives', 286320.55, 271397.0, '2025-04-15'),
(84, 27, 'Equities', 323040.92, 263832.84, '2025-04-15'),
(85, 27, 'Fixed Income', 759581.17, 768985.49, '2025-04-15'),
(86, 28, 'Real Estate', 895565.24, 980758.27, '2025-04-15'),
(87, 28, 'Equities', 792552.99, 907624.96, '2025-04-15'),
(88, 29, 'Alternatives', 189472.78, 174492.99, '2025-04-15'),
(89, 29, 'Equities', 283948.37, 334711.4, '2025-04-15'),
(90, 30, 'Cash', 672828.79, 672828.79, '2025-04-15'),
(91, 30, 'Alternatives', 236672.9, 240114.39, '2025-04-15'),
(92, 30, 'Real Estate', 884064.48, 687679.29, '2025-04-15'),
(93, 30, 'Real Estate', 738999.8, 791765.58, '2025-04-15'),
(94, 30, 'Fixed Income', 172388.18, 127266.88, '2025-04-15'),
(95, 31, 'Fixed Income', 468020.25, 441186.03, '2025-04-15'),
(96, 31, 'Cash', 541303.25, 541303.25, '2025-04-15'),
(97, 31, 'Real Estate', 665381.24, 771919.32, '2025-04-15'),
(98, 31, 'Cash', 266864.22, 266864.22, '2025-04-15'),
(99, 31, 'Equities', 772568.05, 573516.69, '2025-04-15'),
(100, 32, 'Cash', 811027.39, 811027.39, '2025-04-15'),
(101, 32, 'Fixed Income', 579166.97, 556454.97, '2025-04-15'),
(102, 33, 'Cash', 221860.6, 221860.6, '2025-04-15'),
(103, 33, 'Real Estate', 358521.11, 364231.75, '2025-04-15'),
(104, 33, 'Fixed Income', 843046.37, 957693.36, '2025-04-15'),
(105, 33, 'Cash', 246810.01, 246810.01, '2025-04-15'),
(106, 34, 'Real Estate', 1158568.8, 930583.76, '2025-04-15'),
(107, 34, 'Equities', 311725.39, 299276.01, '2025-04-15'),
(108, 34, 'Cash', 262443.05, 262443.05, '2025-04-15'),
(109, 34, 'Alternatives', 761036.32, 902258.43, '2025-04-15'),
(110, 34, 'Fixed Income', 294103.06, 330040.04, '2025-04-15'),
(111, 35, 'Fixed Income', 655525.41, 795197.28, '2025-04-15'),
(112, 35, 'Equities', 1014949.91, 831073.58, '2025-04-15'),
(113, 35, 'Cash', 760510.14, 760510.14, '2025-04-15'),
(114, 36, 'Alternatives', 752441.81, 576331.91, '2025-04-15'),
(115, 37, 'Fixed Income', 733317.64, 566658.24, '2025-04-15'),
(116, 37, 'Equities', 1163248.08, 856310.96, '2025-04-15'),
(117, 37, 'Real Estate', 56678.8, 61830.46, '2025-04-15'),
(118, 37, 'Alternatives', 516896.02, 502202.93, '2025-04-15'),
(119, 37, 'Cash', 368793.36, 368793.36, '2025-04-15'),
(120, 38, 'Fixed Income', 591970.62, 696336.0, '2025-04-15'),
(121, 39, 'Equities', 93785.08, 76236.35, '2025-04-15'),
(122, 39, 'Real Estate', 662162.41, 507258.25, '2025-04-15'),
(123, 39, 'Real Estate', 486901.95, 558684.76, '2025-04-15'),
(124, 40, 'Cash', 600486.53, 600486.53, '2025-04-15'),
(125, 41, 'Real Estate', 24521.18, 26361.9, '2025-04-15'),
(126, 41, 'Alternatives', 1198314.56, 882498.23, '2025-04-15'),
(127, 41, 'Real Estate', 864449.58, 690562.7, '2025-04-15'),
(128, 41, 'Equities', 604507.47, 633407.76, '2025-04-15'),
(129, 42, 'Equities', 962428.04, 832283.45, '2025-04-15'),
(130, 42, 'Equities', 310629.04, 385224.68, '2025-04-15'),
(131, 42, 'Alternatives', 381233.45, 432164.8, '2025-04-15'),
(132, 42, 'Equities', 51428.42, 48606.22, '2025-04-15'),
(133, 43, 'Alternatives', 935600.73, 800230.07, '2025-04-15'),
(134, 43, 'Equities', 733554.53, 824029.5, '2025-04-15'),
(135, 44, 'Real Estate', 1021666.02, 760687.73, '2025-04-15'),
(136, 44, 'Equities', 898293.67, 838296.51, '2025-04-15'),
(137, 45, 'Alternatives', 194972.49, 176922.96, '2025-04-15'),
(138, 45, 'Alternatives', 372523.67, 445000.6, '2025-04-15'),
(139, 45, 'Cash', 368965.14, 368965.14, '2025-04-15'),
(140, 46, 'Cash', 166032.44, 166032.44, '2025-04-15'),
(141, 46, 'Equities', 831636.0, 865876.63, '2025-04-15'),
(142, 46, 'Real Estate', 861287.84, 622483.85, '2025-04-15'),
(143, 47, 'Equities', 308836.62, 314446.45, '2025-04-15'),
(144, 47, 'Cash', 85256.13, 85256.13, '2025-04-15'),
(145, 47, 'Cash', 936816.57, 936816.57, '2025-04-15'),
(146, 47, 'Equities', 468962.93, 524802.68, '2025-04-15'),
(147, 48, 'Fixed Income', 124389.34, 154804.88, '2025-04-15'),
(148, 49, 'Cash', 931066.33, 931066.33, '2025-04-15'),
(149, 49, 'Real Estate', 732204.94, 537023.53, '2025-04-15'),
(150, 50, 'Alternatives', 291166.21, 273083.43, '2025-04-15'),
(151, 50, 'Equities', 817755.49, 733550.37, '2025-04-15'),
(152, 50, 'Real Estate', 238652.74, 243870.83, '2025-04-15'),
(153, 51, 'Equities', 382603.13, 465115.86, '2025-04-15'),
(154, 52, 'Real Estate', 689345.13, 653083.03, '2025-04-15'),
(155, 52, 'Fixed Income', 822195.54, 939257.95, '2025-04-15'),
(156, 52, 'Equities', 992201.85, 729653.26, '2025-04-15'),
(157, 53, 'Real Estate', 616906.81, 536694.81, '2025-04-15'),
(158, 53, 'Alternatives', 1226100.15, 924471.3, '2025-04-15'),
(159, 53, 'Cash', 351081.86, 351081.86, '2025-04-15'),
(160, 53, 'Fixed Income', 319677.08, 238359.29, '2025-04-15'),
(161, 53, 'Alternatives', 610748.2, 500868.39, '2025-04-15'),
(162, 54, 'Cash', 654111.09, 654111.09, '2025-04-15'),
(163, 54, 'Fixed Income', 826572.42, 956562.22, '2025-04-15'),
(164, 54, 'Equities', 338811.87, 362952.34, '2025-04-15'),
(165, 54, 'Equities', 746900.11, 658432.37, '2025-04-15'),
(166, 54, 'Alternatives', 49080.62, 55993.94, '2025-04-15'),
(167, 55, 'Alternatives', 208464.86, 182733.31, '2025-04-15'),
(168, 55, 'Alternatives', 825929.5, 977465.99, '2025-04-15'),
(169, 55, 'Cash', 122806.26, 122806.26, '2025-04-15'),
(170, 55, 'Fixed Income', 747166.07, 536772.81, '2025-04-15'),
(171, 56, 'Cash', 596076.2, 596076.2, '2025-04-15'),
(172, 56, 'Real Estate', 653573.54, 516665.67, '2025-04-15'),
(173, 57, 'Equities', 1109609.04, 918404.52, '2025-04-15'),
(174, 57, 'Real Estate', 665081.34, 812884.72, '2025-04-15'),
(175, 58, 'Real Estate', 617023.83, 564131.72, '2025-04-15'),
(176, 58, 'Alternatives', 67450.44, 62194.25, '2025-04-15'),
(177, 58, 'Alternatives', 802802.35, 713208.73, '2025-04-15'),
(178, 59, 'Real Estate', 275286.49, 260118.83, '2025-04-15'),
(179, 59, 'Cash', 779417.32, 779417.32, '2025-04-15'),
(180, 60, 'Cash', 352465.34, 352465.34, '2025-04-15'),
(181, 60, 'Equities', 775456.63, 792032.74, '2025-04-15'),
(182, 60, 'Real Estate', 441132.33, 447546.89, '2025-04-15'),
(183, 61, 'Alternatives', 654926.03, 610069.99, '2025-04-15'),
(184, 62, 'Cash', 151935.7, 151935.7, '2025-04-15'),
(185, 62, 'Cash', 466038.91, 466038.91, '2025-04-15'),
(186, 62, 'Alternatives', 685861.48, 753854.27, '2025-04-15'),
(187, 63, 'Real Estate', 64170.65, 70283.08, '2025-04-15'),
(188, 63, 'Alternatives', 238978.35, 242490.44, '2025-04-15'),
(189, 64, 'Cash', 834617.26, 834617.26, '2025-04-15'),
(190, 64, 'Fixed Income', 882043.94, 801951.8, '2025-04-15'),
(191, 65, 'Real Estate', 782891.85, 626822.41, '2025-04-15'),
(192, 65, 'Cash', 122173.7, 122173.7, '2025-04-15'),
(193, 65, 'Fixed Income', 1044012.03, 967069.75, '2025-04-15'),
(194, 65, 'Fixed Income', 902338.86, 653195.29, '2025-04-15'),
(195, 66, 'Alternatives', 469894.51, 403431.54, '2025-04-15'),
(196, 66, 'Equities', 274571.41, 219962.3, '2025-04-15'),
(197, 66, 'Fixed Income', 318335.87, 302537.77, '2025-04-15'),
(198, 67, 'Equities', 871240.24, 696904.03, '2025-04-15'),
(199, 68, 'Real Estate', 17364.19, 13918.25, '2025-04-15'),
(200, 69, 'Cash', 862851.67, 862851.67, '2025-04-15'),
(201, 69, 'Real Estate', 178623.04, 141115.8, '2025-04-15'),
(202, 69, 'Real Estate', 520124.23, 414073.27, '2025-04-15'),
(203, 70, 'Real Estate', 926373.05, 962198.88, '2025-04-15'),
(204, 70, 'Cash', 487372.4, 487372.4, '2025-04-15'),
(205, 70, 'Alternatives', 1030167.35, 817167.02, '2025-04-15'),
(206, 70, 'Equities', 683434.82, 703548.87, '2025-04-15'),
(207, 71, 'Alternatives', 527194.13, 476543.64, '2025-04-15'),
(208, 71, 'Equities', 1062617.29, 889796.23, '2025-04-15'),
(209, 72, 'Equities', 1174984.21, 961164.29, '2025-04-15'),
(210, 72, 'Cash', 982027.85, 982027.85, '2025-04-15'),
(211, 73, 'Alternatives', 1187254.97, 905860.93, '2025-04-15'),
(212, 73, 'Real Estate', 498039.14, 403645.81, '2025-04-15'),
(213, 74, 'Equities', 490985.78, 352094.7, '2025-04-15'),
(214, 74, 'Fixed Income', 487204.07, 363404.25, '2025-04-15'),
(215, 75, 'Real Estate', 591584.08, 518566.88, '2025-04-15'),
(216, 75, 'Real Estate', 532610.92, 539145.81, '2025-04-15'),
(217, 75, 'Cash', 884120.1, 884120.1, '2025-04-15'),
(218, 75, 'Alternatives', 450416.15, 477514.81, '2025-04-15'),
(219, 75, 'Real Estate', 111864.89, 136325.75, '2025-04-15'),
(220, 76, 'Equities', 472157.61, 445328.49, '2025-04-15'),
(221, 76, 'Equities', 926394.38, 922680.64, '2025-04-15'),
(222, 76, 'Fixed Income', 419699.93, 511763.55, '2025-04-15'),
(223, 76, 'Fixed Income', 761622.69, 793715.56, '2025-04-15'),
(224, 77, 'Equities', 545424.18, 560919.25, '2025-04-15'),
(225, 77, 'Cash', 861485.16, 861485.16, '2025-04-15'),
(226, 77, 'Fixed Income', 710416.62, 772218.17, '2025-04-15'),
(227, 77, 'Equities', 362058.76, 301564.61, '2025-04-15'),
(228, 77, 'Real Estate', 1117848.08, 950855.92, '2025-04-15'),
(229, 78, 'Cash', 683196.55, 683196.55, '2025-04-15'),
(230, 78, 'Real Estate', 554837.78, 531516.85, '2025-04-15'),
(231, 79, 'Fixed Income', 21480.64, 18619.76, '2025-04-15'),
(232, 79, 'Fixed Income', 1093706.05, 890983.88, '2025-04-15'),
(233, 79, 'Equities', 48295.27, 43219.68, '2025-04-15'),
(234, 80, 'Fixed Income', 149885.93, 147109.5, '2025-04-15'),
(235, 80, 'Alternatives', 913576.33, 925886.76, '2025-04-15'),
(236, 80, 'Cash', 811694.39, 811694.39, '2025-04-15'),
(237, 80, 'Cash', 917456.14, 917456.14, '2025-04-15'),
(238, 80, 'Real Estate', 272745.58, 206421.12, '2025-04-15'),
(239, 81, 'Alternatives', 928710.78, 824950.41, '2025-04-15'),
(240, 81, 'Equities', 408068.06, 353952.9, '2025-04-15'),
(241, 81, 'Cash', 54938.57, 54938.57, '2025-04-15'),
(242, 81, 'Equities', 1140033.81, 906030.48, '2025-04-15'),
(243, 82, 'Real Estate', 602084.52, 555123.71, '2025-04-15'),
(244, 82, 'Fixed Income', 595736.16, 614245.19, '2025-04-15'),
(245, 83, 'Fixed Income', 332129.14, 367501.61, '2025-04-15'),
(246, 83, 'Real Estate', 648779.95, 792374.9, '2025-04-15'),
(247, 83, 'Equities', 449227.24, 485127.36, '2025-04-15'),
(248, 83, 'Real Estate', 804116.96, 837016.85, '2025-04-15'),
(249, 84, 'Cash', 227976.37, 227976.37, '2025-04-15'),
(250, 84, 'Cash', 942749.74, 942749.74, '2025-04-15'),
(251, 84, 'Equities', 866042.28, 652357.43, '2025-04-15'),
(252, 85, 'Cash', 118128.24, 118128.24, '2025-04-15'),
(253, 85, 'Alternatives', 800176.5, 962254.22, '2025-04-15'),
(254, 86, 'Real Estate', 164899.32, 129810.86, '2025-04-15'),
(255, 86, 'Alternatives', 310438.84, 319109.8, '2025-04-15'),
(256, 86, 'Alternatives', 65981.35, 76252.44, '2025-04-15'),
(257, 87, 'Cash', 788185.71, 788185.71, '2025-04-15'),
(258, 88, 'Cash', 240360.67, 240360.67, '2025-04-15'),
(259, 89, 'Real Estate', 1182267.79, 853760.23, '2025-04-15'),
(260, 89, 'Cash', 667291.9, 667291.9, '2025-04-15'),
(261, 89, 'Real Estate', 176599.53, 189385.8, '2025-04-15'),
(262, 89, 'Alternatives', 143726.01, 106325.7, '2025-04-15'),
(263, 90, 'Equities', 422509.8, 346020.67, '2025-04-15'),
(264, 90, 'Fixed Income', 959420.36, 942442.79, '2025-04-15'),
(265, 90, 'Real Estate', 554058.85, 511378.84, '2025-04-15'),
(266, 91, 'Cash', 407720.68, 407720.68, '2025-04-15'),
(267, 91, 'Fixed Income', 242510.15, 261324.0, '2025-04-15'),
(268, 91, 'Real Estate', 509208.81, 382384.89, '2025-04-15'),
(269, 91, 'Equities', 693634.85, 553008.04, '2025-04-15'),
(270, 91, 'Fixed Income', 345923.74, 282873.49, '2025-04-15'),
(271, 92, 'Alternatives', 1108016.26, 907544.84, '2025-04-15'),
(272, 92, 'Fixed Income', 198829.8, 156813.38, '2025-04-15'),
(273, 92, 'Alternatives', 919653.61, 908416.48, '2025-04-15'),
(274, 93, 'Fixed Income', 226347.46, 205561.91, '2025-04-15'),
(275, 94, 'Fixed Income', 543728.61, 515598.38, '2025-04-15'),
(276, 94, 'Cash', 67069.27, 67069.27, '2025-04-15'),
(277, 94, 'Cash', 732753.95, 732753.95, '2025-04-15'),
(278, 94, 'Equities', 708851.55, 528783.48, '2025-04-15'),
(279, 95, 'Real Estate', 672646.32, 676638.62, '2025-04-15'),
(280, 95, 'Real Estate', 113478.96, 133461.19, '2025-04-15'),
(281, 96, 'Real Estate', 68006.64, 61455.62, '2025-04-15'),
(282, 96, 'Real Estate', 517595.84, 570175.6, '2025-04-15'),
(283, 96, 'Alternatives', 118347.84, 124626.25, '2025-04-15'),
(284, 96, 'Fixed Income', 769295.26, 832654.27, '2025-04-15'),
(285, 97, 'Alternatives', 260444.82, 305535.03, '2025-04-15'),
(286, 97, 'Equities', 368151.99, 419872.39, '2025-04-15'),
(287, 97, 'Real Estate', 691392.32, 548769.84, '2025-04-15'),
(288, 98, 'Fixed Income', 1037090.07, 992084.32, '2025-04-15'),
(289, 98, 'Cash', 71888.58, 71888.58, '2025-04-15'),
(290, 98, 'Real Estate', 747648.83, 822721.78, '2025-04-15'),
(291, 98, 'Alternatives', 466013.03, 334094.59, '2025-04-15'),
(292, 99, 'Equities', 134321.82, 118277.49, '2025-04-15'),
(293, 99, 'Fixed Income', 58029.2, 62680.44, '2025-04-15'),
(294, 100, 'Alternatives', 376882.17, 469213.27, '2025-04-15'),
(295, 100, 'Real Estate', 548401.73, 568638.23, '2025-04-15'),
(296, 100, 'Fixed Income', 358609.52, 385628.24, '2025-04-15'),
(297, 100, 'Equities', 657995.58, 726953.37, '2025-04-15'),
(298, 100, 'Equities', 451347.47, 420504.56, '2025-04-15');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Data Cleaning & Client 360
-------------------------------------------------------------------------------

-- 2.1 View: Client_360
-- Aggregates total assets and performance at the client level.
CREATE OR REPLACE VIEW WealthDB.Silver.Client_360 AS
SELECT
    c.ClientID,
    c.Name,
    c.Region,
    c.RiskProfile,
    c.AdvisorID,
    COUNT(a.AssetID) AS HoldingsCount,
    SUM(a.MarketValue) AS TotalAUM,
    SUM(a.CostBasis) AS TotalInvested,
    (SUM(a.MarketValue) - SUM(a.CostBasis)) AS UnrealizedGainLoss,
    CASE 
        WHEN SUM(a.CostBasis) > 0 THEN 
            ((SUM(a.MarketValue) - SUM(a.CostBasis)) / SUM(a.CostBasis)) * 100 
        ELSE 0 
    END AS ReturnPct
FROM WealthDB.Bronze.Clients c
LEFT JOIN WealthDB.Bronze.Assets a ON c.ClientID = a.ClientID
GROUP BY c.ClientID, c.Name, c.Region, c.RiskProfile, c.AdvisorID;

-- 2.2 View: Portfolio_Allocations
-- Calculates percentage allocation by asset class for each client.
CREATE OR REPLACE VIEW WealthDB.Silver.Portfolio_Allocations AS
SELECT
    a.ClientID,
    c.Name,
    a.AssetClass,
    SUM(a.MarketValue) AS ClassValue,
    RATIO_TO_REPORT(SUM(a.MarketValue)) OVER (PARTITION BY a.ClientID) * 100 AS AllocationPct
FROM WealthDB.Bronze.Assets a
JOIN WealthDB.Bronze.Clients c ON a.ClientID = c.ClientID
GROUP BY a.ClientID, c.Name, a.AssetClass;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Advisor Insights & Risk
-------------------------------------------------------------------------------

-- 3.1 View: Advisor_Performance
-- Ranks advisors by AUM and High Net Worth (HNW) client count.
CREATE OR REPLACE VIEW WealthDB.Gold.Advisor_Performance AS
SELECT
    AdvisorID,
    COUNT(DISTINCT ClientID) AS TotalClients,
    SUM(TotalAUM) AS ManagedAUM,
    COUNT(CASE WHEN TotalAUM > 1000000 THEN 1 END) AS HNW_Client_Count
FROM WealthDB.Silver.Client_360
GROUP BY AdvisorID;

-- 3.2 View: Churn_Risk_Candidates
-- Identifies conservative clients with negative returns (Mismatch of experience vs expectation).
CREATE OR REPLACE VIEW WealthDB.Gold.Churn_Risk_Candidates AS
SELECT
    ClientID,
    Name,
    RiskProfile,
    ReturnPct,
    TotalAUM
FROM WealthDB.Silver.Client_360
WHERE RiskProfile = 'Conservative' 
  AND ReturnPct < 0
ORDER BY TotalAUM DESC;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS (Visualization)
-------------------------------------------------------------------------------

/*
PROMPT 1 (Advisor Dashboard):
"Using WealthDB.Gold.Advisor_Performance, create a bar chart of ManagedAUM by AdvisorID, sorted descending."

PROMPT 2 (Risk Analysis):
"Count the number of clients in WealthDB.Gold.Churn_Risk_Candidates and show me the list sorted by lowest ReturnPct."

PROMPT 3 (Portfolio Check):
"From WealthDB.Silver.Portfolio_Allocations, show me the AssetClass breakdown for Client 'Client_5' as a pie chart."

PROMPT 4 (Regional Growth):
"Using WealthDB.Silver.Client_360, show me the average ReturnPct by Region."
*/
