/*
 * Hybrid Supply Chain Control Tower Demo
 * 
 * Scenario:
 * A global logistics company needs a "Control Tower" view to optimize supply chain operations.
 * They need to unify data from three distinct, simulated physical sources:
 * 1. Snowflake (Data Warehouse): High-volume historical sales and demand forecasts.
 * 2. AWS S3 (Data Lake): Semi-structured shipping logs and IoT sensor streams.
 * 3. Postgres (Operational DB): Real-time inventory levels and product master data.
 * 
 * Architecture: Medallion (Source -> Bronze -> Silver -> Gold)
 * 
 * Dremio Feature Highlight:
 * This demo simulates the "Data Virtualization" capability of Dremio by creating
 * separate folders for each "Source" technology, then joining them seamlessly
 * without data movement.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure (Simulating Sources)
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS HybridSupply;
CREATE FOLDER IF NOT EXISTS HybridSupply.Sources;

-- Simulating Physical Source Connections
CREATE FOLDER IF NOT EXISTS HybridSupply.Sources.Snowflake;       -- e.g., "EDW_Sales"
CREATE FOLDER IF NOT EXISTS HybridSupply.Sources.AWS_S3;          -- e.g., "DataLake_Logs"
CREATE FOLDER IF NOT EXISTS HybridSupply.Sources.Postgres_Prod;   -- e.g., "Ops_Inventory"

-- Logical Layers
CREATE FOLDER IF NOT EXISTS HybridSupply.Bronze;
CREATE FOLDER IF NOT EXISTS HybridSupply.Silver;
CREATE FOLDER IF NOT EXISTS HybridSupply.Gold;

-------------------------------------------------------------------------------
-- 1. SOURCES: Physical Tables & Seed Data
-------------------------------------------------------------------------------

-- ============================================================================
-- SOURCE 1: SNOWFLAKE (Historical Sales & Forecasts)
-- ============================================================================

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.Snowflake.Historical_Sales (
    OrderID VARCHAR,
    SKU VARCHAR,
    Region VARCHAR,
    SaleDate DATE,
    Quantity INT,
    Revenue DOUBLE
);

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.Snowflake.Regional_Demand_Forecast (
    Region VARCHAR,
    ForecastMonth DATE,
    Category VARCHAR,
    PredictedVolume INT,
    ConfidenceInterval DOUBLE
);

-- Seed Snowflake.Historical_Sales
INSERT INTO HybridSupply.Sources.Snowflake.Historical_Sales (OrderID, SKU, Region, SaleDate, Quantity, Revenue) VALUES
('ORD-2023-001', 'SKU-A100', 'NA', '2023-01-01', 100, 5000.0),
('ORD-2023-002', 'SKU-B200', 'EU', '2023-01-02', 50, 1200.0),
('ORD-2023-003', 'SKU-C300', 'APAC', '2023-01-05', 200, 8000.0),
('ORD-2023-004', 'SKU-A100', 'NA', '2023-02-01', 120, 6000.0),
('ORD-2023-005', 'SKU-B200', 'EU', '2023-02-15', 60, 1440.0),
('ORD-2023-006', 'SKU-C300', 'APAC', '2023-03-01', 180, 7200.0),
('ORD-2023-007', 'SKU-D400', 'LATAM', '2023-01-10', 30, 900.0),
('ORD-2023-008', 'SKU-E500', 'NA', '2023-04-01', 500, 25000.0),
('ORD-2023-009', 'SKU-A100', 'EU', '2023-05-01', 80, 4000.0),
('ORD-2023-010', 'SKU-B200', 'APAC', '2023-06-01', 70, 1680.0),
('ORD-2023-011', 'SKU-A100', 'NA', '2024-01-01', 110, 5500.0),
('ORD-2023-012', 'SKU-B200', 'EU', '2024-01-02', 55, 1320.0),
('ORD-2023-013', 'SKU-C300', 'APAC', '2024-01-05', 210, 8400.0),
('ORD-2023-014', 'SKU-D400', 'LATAM', '2024-01-10', 35, 1050.0),
('ORD-2023-015', 'SKU-E500', 'NA', '2024-01-15', 520, 26000.0),
('ORD-2023-016', 'SKU-F600', 'EU', '2024-02-01', 10, 5000.0), -- High value
('ORD-2023-017', 'SKU-G700', 'APAC', '2024-02-05', 1000, 5000.0), -- Low value
('ORD-2023-018', 'SKU-A100', 'NA', '2024-03-01', 90, 4500.0),
('ORD-2023-019', 'SKU-B200', 'EU', '2024-03-05', 65, 1560.0),
('ORD-2023-020', 'SKU-C300', 'APAC', '2024-03-10', 190, 7600.0),
('ORD-2023-021', 'SKU-D400', 'LATAM', '2024-03-15', 40, 1200.0),
('ORD-2023-022', 'SKU-E500', 'NA', '2024-03-20', 510, 25500.0),
('ORD-2023-023', 'SKU-F600', 'EU', '2024-04-01', 12, 6000.0),
('ORD-2023-024', 'SKU-G700', 'APAC', '2024-04-05', 900, 4500.0),
('ORD-2023-025', 'SKU-A100', 'NA', '2024-04-10', 105, 5250.0),
('ORD-2023-026', 'SKU-B200', 'EU', '2024-04-15', 58, 1392.0),
('ORD-2023-027', 'SKU-C300', 'APAC', '2024-04-20', 205, 8200.0),
('ORD-2023-028', 'SKU-D400', 'LATAM', '2024-04-25', 38, 1140.0),
('ORD-2023-029', 'SKU-E500', 'NA', '2024-04-30', 530, 26500.0),
('ORD-2023-030', 'SKU-A100', 'NA', '2024-05-01', 100, 5000.0),
('ORD-2023-031', 'SKU-A100', 'EU', '2024-05-05', 50, 2500.0),
('ORD-2023-032', 'SKU-A100', 'APAC', '2024-05-10', 150, 7500.0),
('ORD-2023-033', 'SKU-B200', 'NA', '2024-05-15', 40, 960.0),
('ORD-2023-034', 'SKU-B200', 'EU', '2024-05-20', 60, 1440.0),
('ORD-2023-035', 'SKU-B200', 'APAC', '2024-05-25', 80, 1920.0),
('ORD-2023-036', 'SKU-F600', 'NA', '2024-06-01', 5, 2500.0),
('ORD-2023-037', 'SKU-F600', 'EU', '2024-06-05', 8, 4000.0),
('ORD-2023-038', 'SKU-F600', 'APAC', '2024-06-10', 15, 7500.0),
('ORD-2023-039', 'SKU-G700', 'NA', '2024-06-15', 200, 1000.0),
('ORD-2023-040', 'SKU-G700', 'EU', '2024-06-20', 300, 1500.0),
('ORD-2023-041', 'SKU-G700', 'APAC', '2024-06-25', 500, 2500.0),
('ORD-2023-042', 'SKU-C300', 'NA', '2024-07-01', 100, 4000.0),
('ORD-2023-043', 'SKU-C300', 'EU', '2024-07-05', 100, 4000.0),
('ORD-2023-044', 'SKU-C300', 'APAC', '2024-07-10', 100, 4000.0),
('ORD-2023-045', 'SKU-H800', 'NA', '2024-07-15', 10, 1000.0), -- New product
('ORD-2023-046', 'SKU-H800', 'EU', '2024-07-20', 10, 1000.0),
('ORD-2023-047', 'SKU-H800', 'APAC', '2024-07-25', 10, 1000.0),
('ORD-2023-048', 'SKU-I900', 'NA', '2024-08-01', 1, 10000.0), -- Super expensive
('ORD-2023-049', 'SKU-I900', 'EU', '2024-08-05', 1, 10000.0),
('ORD-2023-050', 'SKU-I900', 'APAC', '2024-08-10', 1, 10000.0);

-- Seed Snowflake.Regional_Demand_Forecast
INSERT INTO HybridSupply.Sources.Snowflake.Regional_Demand_Forecast (Region, ForecastMonth, Category, PredictedVolume, ConfidenceInterval) VALUES
('NA', '2025-06-01', 'Electronics', 5000, 0.95),
('EU', '2025-06-01', 'Electronics', 3000, 0.90),
('APAC', '2025-06-01', 'Electronics', 8000, 0.85),
('NA', '2025-06-01', 'Furniture', 200, 0.80),
('EU', '2025-06-01', 'Furniture', 150, 0.85),
('APAC', '2025-06-01', 'Furniture', 300, 0.75),
('NA', '2025-07-01', 'Electronics', 5500, 0.92),
('EU', '2025-07-01', 'Electronics', 3200, 0.88),
('APAC', '2025-07-01', 'Electronics', 8500, 0.82),
('LATAM', '2025-06-01', 'Electronics', 1000, 0.70), -- Low confidence
('LATAM', '2025-07-01', 'Electronics', 1200, 0.65),
('NA', '2025-06-01', 'Clothing', 10000, 0.98),
('EU', '2025-06-01', 'Clothing', 8000, 0.95),
('APAC', '2025-06-01', 'Clothing', 12000, 0.90),
('NA', '2025-06-01', 'Toys', 500, 0.60), -- Volatile
('EU', '2025-06-01', 'Toys', 400, 0.65),
('APAC', '2025-06-01', 'Toys', 600, 0.55),
('NA', '2025-08-01', 'Electronics', 6000, 0.90),
('EU', '2025-08-01', 'Electronics', 3500, 0.85),
('APAC', '2025-08-01', 'Electronics', 9000, 0.80),
('NA', '2025-06-01', 'Auto Parts', 2000, 0.99), -- Stable
('EU', '2025-06-01', 'Auto Parts', 1800, 0.99),
('APAC', '2025-06-01', 'Auto Parts', 2500, 0.99),
('LATAM', '2025-06-01', 'Auto Parts', 500, 0.95),
('NA', '2025-09-01', 'Electronics', 7000, 0.80), -- Far future
('EU', '2025-09-01', 'Electronics', 4000, 0.75),
('APAC', '2025-09-01', 'Electronics', 10000, 0.70),
('NA', '2025-07-01', 'Clothing', 11000, 0.97),
('EU', '2025-07-01', 'Clothing', 9000, 0.94),
('APAC', '2025-07-01', 'Clothing', 13000, 0.89),
('NA', '2025-08-01', 'Clothing', 12000, 0.96),
('EU', '2025-08-01', 'Clothing', 10000, 0.93),
('APAC', '2025-08-01', 'Clothing', 14000, 0.88),
('NA', '2025-07-01', 'Furniture', 220, 0.79),
('EU', '2025-07-01', 'Furniture', 160, 0.84),
('APAC', '2025-07-01', 'Furniture', 320, 0.74),
('NA', '2025-08-01', 'Furniture', 240, 0.78),
('EU', '2025-08-01', 'Furniture', 170, 0.83),
('APAC', '2025-08-01', 'Furniture', 340, 0.73),
('NA', '2025-09-01', 'Furniture', 260, 0.77),
('EU', '2025-09-01', 'Furniture', 180, 0.82),
('APAC', '2025-09-01', 'Furniture', 360, 0.72),
('NA', '2025-10-01', 'Electronics', 8000, 0.70),
('EU', '2025-10-01', 'Electronics', 4500, 0.65),
('APAC', '2025-10-01', 'Electronics', 11000, 0.60),
('LATAM', '2025-08-01', 'Electronics', 1500, 0.60),
('LATAM', '2025-09-01', 'Electronics', 1800, 0.55),
('LATAM', '2025-10-01', 'Electronics', 2000, 0.50),
('NA', '2025-11-01', 'Electronics', 9000, 0.60), -- Holiday peak?
('EU', '2025-11-01', 'Electronics', 5000, 0.55),
('APAC', '2025-11-01', 'Electronics', 12000, 0.50);

-- ============================================================================
-- SOURCE 2: AWS S3 (Logistics Logs & IoT)
-- ============================================================================

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.AWS_S3.Shipping_Logs (
    ShipmentID VARCHAR,
    OrderID VARCHAR,
    Carrier VARCHAR,
    Status VARCHAR, -- 'In Transit', 'Delivered', 'Delayed'
    CurrentLocation VARCHAR,
    LastUpdate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.AWS_S3.IoT_Sensor_Data (
    SensorLogID INT,
    ShipmentID VARCHAR,
    ContainerTempC DOUBLE,
    ShockGLevel DOUBLE, -- G-force impact
    BatteryPct INT,
    Timestamp TIMESTAMP
);

-- Seed S3 Data
INSERT INTO HybridSupply.Sources.AWS_S3.Shipping_Logs (ShipmentID, OrderID, Carrier, Status, CurrentLocation, LastUpdate) VALUES
('SHP-001', 'ORD-2023-011', 'FedEx', 'Delivered', 'New York, NY', '2024-01-05 10:00:00'),
('SHP-002', 'ORD-2023-012', 'DHL', 'Delivered', 'London, UK', '2024-01-06 14:00:00'),
('SHP-003', 'ORD-2023-013', 'Maersk', 'Delivered', 'Singapore', '2024-01-10 09:00:00'),
('SHP-004', 'ORD-2023-014', 'UPS', 'Delivered', 'Sao Paulo, BR', '2024-01-15 11:00:00'),
('SHP-005', 'ORD-2023-015', 'FedEx', 'In Transit', 'Chicago, IL', '2024-01-20 08:00:00'), -- Active
('SHP-006', 'ORD-2023-016', 'DHL', 'In Transit', 'Paris, FR', '2024-02-05 10:00:00'),
('SHP-007', 'ORD-2023-017', 'Maersk', 'In Transit', 'Tokyo, JP', '2024-02-10 12:00:00'),
('SHP-008', 'ORD-2023-018', 'FedEx', 'Delayed', 'Memphis, TN', '2024-03-05 16:00:00'), -- Issue
('SHP-009', 'ORD-2023-019', 'DHL', 'Delayed', 'Berlin, DE', '2024-03-10 09:00:00'),
('SHP-010', 'ORD-2023-020', 'Maersk', 'In Transit', 'Sydney, AU', '2024-03-15 14:00:00'),
('SHP-011', 'ORD-2023-021', 'UPS', 'In Transit', 'Mexico City', '2024-03-20 10:00:00'),
('SHP-012', 'ORD-2023-022', 'FedEx', 'Delivered', 'LA, CA', '2024-03-25 11:00:00'),
('SHP-013', 'ORD-2023-023', 'DHL', 'In Transit', 'Rome, IT', '2024-04-05 08:00:00'),
('SHP-014', 'ORD-2023-024', 'Maersk', 'In Transit', 'Shanghai, CN', '2024-04-10 12:00:00'),
('SHP-015', 'ORD-2023-025', 'FedEx', 'Delivered', 'Boston, MA', '2024-04-15 10:00:00'),
('SHP-016', 'ORD-2023-026', 'DHL', 'Delivered', 'Madrid, ES', '2024-04-20 14:00:00'),
('SHP-017', 'ORD-2023-027', 'Maersk', 'In Transit', 'Hong Kong', '2024-04-25 09:00:00'),
('SHP-018', 'ORD-2023-028', 'UPS', 'Delayed', 'Bogota, CO', '2024-04-30 16:00:00'),
('SHP-019', 'ORD-2023-029', 'FedEx', 'In Transit', 'Seattle, WA', '2024-05-05 08:00:00'),
('SHP-020', 'ORD-2023-030', 'FedEx', 'In Transit', 'Miami, FL', '2024-05-10 10:00:00'),
('SHP-021', 'ORD-2023-031', 'DHL', 'Delivered', 'Amsterdam, NL', '2024-05-15 12:00:00'),
('SHP-022', 'ORD-2023-032', 'Maersk', 'In Transit', 'Mumbai, IN', '2024-05-20 14:00:00'),
('SHP-023', 'ORD-2023-033', 'FedEx', 'Delivered', 'Dallas, TX', '2024-05-25 10:00:00'),
('SHP-024', 'ORD-2023-034', 'DHL', 'Delivered', 'Vienna, AT', '2024-05-30 14:00:00'),
('SHP-025', 'ORD-2023-035', 'Maersk', 'In Transit', 'Seoul, KR', '2024-06-05 09:00:00'),
('SHP-026', 'ORD-2023-036', 'FedEx', 'Delayed', 'Denver, CO', '2024-06-10 16:00:00'),
('SHP-027', 'ORD-2023-037', 'DHL', 'In Transit', 'Warsaw, PL', '2024-06-15 10:00:00'),
('SHP-028', 'ORD-2023-038', 'Maersk', 'In Transit', 'Bangkok, TH', '2024-06-20 12:00:00'),
('SHP-029', 'ORD-2023-039', 'FedEx', 'Delivered', 'Atlanta, GA', '2024-06-25 10:00:00'),
('SHP-030', 'ORD-2023-040', 'DHL', 'Delivered', 'Lisbon, PT', '2024-06-30 14:00:00'),
('SHP-031', 'ORD-2023-041', 'Maersk', 'In Transit', 'Jakarta, ID', '2024-07-05 09:00:00'),
('SHP-032', 'ORD-2023-042', 'FedEx', 'In Transit', 'Phoenix, AZ', '2024-07-10 10:00:00'),
('SHP-033', 'ORD-2023-043', 'DHL', 'In Transit', 'Brussels, BE', '2024-07-15 12:00:00'),
('SHP-034', 'ORD-2023-044', 'Maersk', 'Delayed', 'Manila, PH', '2024-07-20 16:00:00'),
('SHP-035', 'ORD-2023-045', 'FedEx', 'Delivered', 'Houston, TX', '2024-07-25 10:00:00'),
('SHP-036', 'ORD-2023-046', 'DHL', 'Delivered', 'Dublin, IE', '2024-07-30 14:00:00'),
('SHP-037', 'ORD-2023-047', 'Maersk', 'In Transit', 'Hanoi, VN', '2024-08-05 09:00:00'),
('SHP-038', 'ORD-2023-048', 'FedEx', 'In Transit', 'Detroit, MI', '2024-08-10 10:00:00'),
('SHP-039', 'ORD-2023-049', 'DHL', 'In Transit', 'Zurich, CH', '2024-08-15 12:00:00'),
('SHP-040', 'ORD-2023-050', 'Maersk', 'Delayed', 'Kuala Lumpur', '2024-08-20 16:00:00'),
('SHP-041', 'ORD-2023-051', 'FedEx', 'Delivered', 'SF, CA', '2024-08-25 10:00:00'), -- No matching order in seed (orphaned)
('SHP-042', 'ORD-2023-052', 'DHL', 'Delivered', 'Munich, DE', '2024-08-30 14:00:00'),
('SHP-043', 'ORD-2023-053', 'Maersk', 'In Transit', 'Osaka, JP', '2024-09-05 09:00:00'),
('SHP-044', 'ORD-2023-054', 'UPS', 'In Transit', 'Lima, PE', '2024-09-10 10:00:00'),
('SHP-045', 'ORD-2023-055', 'FedEx', 'In Transit', 'Minneapolis', '2024-09-15 12:00:00'),
('SHP-046', 'ORD-2023-056', 'DHL', 'Delayed', 'Prague, CZ', '2024-09-20 16:00:00'),
('SHP-047', 'ORD-2023-057', 'Maersk', 'In Transit', 'Taipei, TW', '2024-09-25 09:00:00'),
('SHP-048', 'ORD-2023-058', 'UPS', 'Delivered', 'Santiago, CL', '2024-09-30 10:00:00'),
('SHP-049', 'ORD-2023-059', 'FedEx', 'Delivered', 'Portland, OR', '2024-10-05 14:00:00'),
('SHP-050', 'ORD-2023-060', 'DHL', 'In Transit', 'Stockholm, SE', '2024-10-10 10:00:00');

INSERT INTO HybridSupply.Sources.AWS_S3.IoT_Sensor_Data (SensorLogID, ShipmentID, ContainerTempC, ShockGLevel, BatteryPct, Timestamp) VALUES
(1, 'SHP-005', 4.5, 0.1, 95, '2024-01-20 08:00:00'), -- Normal
(2, 'SHP-005', 4.6, 0.2, 94, '2024-01-20 09:00:00'),
(3, 'SHP-006', 20.0, 0.5, 80, '2024-02-05 10:00:00'),
(4, 'SHP-006', 21.0, 5.0, 79, '2024-02-05 10:30:00'), -- Dropped!
(5, 'SHP-006', 20.5, 0.1, 78, '2024-02-05 11:00:00'),
(6, 'SHP-007', -5.0, 0.0, 99, '2024-02-10 12:00:00'), -- Frozen goods
(7, 'SHP-008', 35.0, 0.1, 50, '2024-03-05 16:00:00'), -- Heat warning
(8, 'SHP-010', 5.0, 0.2, 90, '2024-03-15 14:00:00'),
(9, 'SHP-013', 15.0, 0.1, 85, '2024-04-05 08:00:00'),
(10, 'SHP-014', 18.0, 4.0, 40, '2024-04-10 12:00:00'), -- Rough handling
(11, 'SHP-017', 25.0, 0.0, 95, '2024-04-25 09:00:00'),
(12, 'SHP-018', 22.0, 0.1, 94, '2024-04-30 16:00:00'),
(13, 'SHP-019', 10.0, 0.1, 93, '2024-05-05 08:00:00'),
(14, 'SHP-020', 28.0, 0.1, 92, '2024-05-10 10:00:00'),
(15, 'SHP-022', 30.0, 0.2, 91, '2024-05-20 14:00:00'),
(16, 'SHP-025', 12.0, 0.0, 90, '2024-06-05 09:00:00'),
(17, 'SHP-026', 15.0, 6.0, 89, '2024-06-10 16:00:00'), -- Severe Drop
(18, 'SHP-027', 18.0, 0.1, 88, '2024-06-15 10:00:00'),
(19, 'SHP-028', 33.0, 0.1, 87, '2024-06-20 12:00:00'),
(20, 'SHP-031', 29.0, 0.1, 86, '2024-07-05 09:00:00'),
(21, 'SHP-032', 40.0, 0.1, 85, '2024-07-10 10:00:00'), -- Extreme Heat
(22, 'SHP-033', 18.0, 0.1, 84, '2024-07-15 12:00:00'),
(23, 'SHP-034', 27.0, 0.5, 83, '2024-07-20 16:00:00'),
(24, 'SHP-037', 26.0, 0.1, 82, '2024-08-05 09:00:00'),
(25, 'SHP-038', 19.0, 0.1, 81, '2024-08-10 10:00:00'),
(26, 'SHP-039', 16.0, 0.1, 80, '2024-08-15 12:00:00'),
(27, 'SHP-040', 31.0, 0.2, 20, '2024-08-20 16:00:00'), -- Low battery
(28, 'SHP-043', 20.0, 0.1, 78, '2024-09-05 09:00:00'),
(29, 'SHP-044', 23.0, 0.1, 77, '2024-09-10 10:00:00'),
(30, 'SHP-045', 15.0, 0.1, 76, '2024-09-15 12:00:00'),
(31, 'SHP-046', 17.0, 0.1, 75, '2024-09-20 16:00:00'),
(32, 'SHP-047', 28.0, 0.1, 74, '2024-09-25 09:00:00'),
(33, 'SHP-050', 5.0, 0.1, 73, '2024-10-10 10:00:00'),
(34, 'SHP-005', 4.8, 0.1, 93, '2024-01-20 10:00:00'),
(35, 'SHP-005', 5.0, 0.1, 92, '2024-01-20 11:00:00'),
(36, 'SHP-006', 20.8, 0.1, 77, '2024-02-05 12:00:00'),
(37, 'SHP-006', 21.0, 0.1, 76, '2024-02-05 13:00:00'),
(38, 'SHP-008', 36.0, 0.1, 40, '2024-03-05 17:00:00'),
(39, 'SHP-022', 31.0, 0.2, 90, '2024-05-20 15:00:00'),
(40, 'SHP-032', 42.0, 0.1, 84, '2024-07-10 11:00:00'),
(41, 'SHP-040', 32.0, 0.2, 19, '2024-08-20 17:00:00'),
(42, 'SHP-040', 33.0, 0.2, 18, '2024-08-20 18:00:00'),
(43, 'SHP-026', 15.5, 0.0, 88, '2024-06-10 17:00:00'),
(44, 'SHP-014', 18.5, 0.0, 39, '2024-04-10 13:00:00'),
(45, 'SHP-005', 4.9, 0.1, 91, '2024-01-20 12:00:00'),
(46, 'SHP-006', 20.9, 0.1, 75, '2024-02-05 14:00:00'),
(47, 'SHP-032', 43.0, 0.1, 83, '2024-07-10 12:00:00'),
(48, 'SHP-040', 34.0, 0.2, 10, '2024-08-20 19:00:00'), -- Critical battery
(49, 'SHP-026', 16.0, 0.0, 87, '2024-06-10 18:00:00'),
(50, 'SHP-027', 18.5, 0.1, 87, '2024-06-15 11:00:00');

-- ============================================================================
-- SOURCE 3: POSTGRES (Inventory & Product Master)
-- ============================================================================

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.Postgres_Prod.Product_Master (
    SKU VARCHAR,
    ProductName VARCHAR,
    Category VARCHAR,
    WeightKG DOUBLE,
    SupplierID VARCHAR
);

CREATE TABLE IF NOT EXISTS HybridSupply.Sources.Postgres_Prod.Warehouse_Stock (
    WarehouseID VARCHAR,
    SKU VARCHAR,
    Region VARCHAR,
    CurrentStock INT,
    SafetyStockLevel INT
);

-- Seed Postgres Data
INSERT INTO HybridSupply.Sources.Postgres_Prod.Product_Master (SKU, ProductName, Category, WeightKG, SupplierID) VALUES
('SKU-A100', 'Smart Speaker', 'Electronics', 0.5, 'SUP-01'),
('SKU-B200', 'Wireless Buds', 'Electronics', 0.1, 'SUP-02'),
('SKU-C300', '4K Monitor', 'Electronics', 5.0, 'SUP-01'),
('SKU-D400', 'Gaming Chair', 'Furniture', 15.0, 'SUP-03'),
('SKU-E500', 'Office Desk', 'Furniture', 25.0, 'SUP-03'),
('SKU-F600', 'OLED TV', 'Electronics', 12.0, 'SUP-01'),
('SKU-G700', 'Phone Case', 'Global', 0.05, 'SUP-04'), -- Cheap
('SKU-H800', 'Drone', 'Toys', 2.0, 'SUP-05'),
('SKU-I900', 'Server Rack', 'Enterprise', 50.0, 'SUP-06');

INSERT INTO HybridSupply.Sources.Postgres_Prod.Warehouse_Stock (WarehouseID, SKU, Region, CurrentStock, SafetyStockLevel) VALUES
('WH-NA-01', 'SKU-A100', 'NA', 5000, 1000),
('WH-EU-01', 'SKU-A100', 'EU', 200, 500), -- Low stock
('WH-AP-01', 'SKU-A100', 'APAC', 8000, 2000),
('WH-NA-01', 'SKU-B200', 'NA', 1000, 1000),
('WH-EU-01', 'SKU-B200', 'EU', 1500, 1000),
('WH-AP-01', 'SKU-B200', 'APAC', 2000, 1000),
('WH-NA-02', 'SKU-C300', 'NA', 50, 100), -- Critical
('WH-EU-02', 'SKU-C300', 'EU', 300, 100),
('WH-AP-02', 'SKU-C300', 'APAC', 500, 200),
('WH-LATAM-01', 'SKU-D400', 'LATAM', 100, 50),
('WH-NA-01', 'SKU-E500', 'NA', 20, 10),
('WH-EU-01', 'SKU-E500', 'EU', 15, 10),
('WH-AP-01', 'SKU-F600', 'APAC', 5, 2),
('WH-NA-01', 'SKU-G700', 'NA', 10000, 5000),
('WH-EU-01', 'SKU-G700', 'EU', 8000, 5000),
('WH-AP-01', 'SKU-G700', 'APAC', 12000, 5000),
('WH-NA-01', 'SKU-H800', 'NA', 50, 100), -- Forecasted demand is low for now
('WH-EU-01', 'SKU-H800', 'EU', 40, 100),
('WH-AP-01', 'SKU-H800', 'APAC', 30, 100),
('WH-NA-01', 'SKU-I900', 'NA', 2, 1),
('WH-EU-01', 'SKU-I900', 'EU', 2, 1),
('WH-AP-01', 'SKU-I900', 'APAC', 1, 1);

-------------------------------------------------------------------------------
-- 2. BRONZE LAYER (Raw Views)
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_Sales AS SELECT * FROM HybridSupply.Sources.Snowflake.Historical_Sales;
CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_Forecasts AS SELECT * FROM HybridSupply.Sources.Snowflake.Regional_Demand_Forecast;
CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_Shipments AS SELECT * FROM HybridSupply.Sources.AWS_S3.Shipping_Logs;
CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_IoT AS SELECT * FROM HybridSupply.Sources.AWS_S3.IoT_Sensor_Data;
CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_Products AS SELECT * FROM HybridSupply.Sources.Postgres_Prod.Product_Master;
CREATE OR REPLACE VIEW HybridSupply.Bronze.Bronze_Stock AS SELECT * FROM HybridSupply.Sources.Postgres_Prod.Warehouse_Stock;

-------------------------------------------------------------------------------
-- 3. SILVER LAYER (Integrated)
-------------------------------------------------------------------------------

-- 3.1 Unified Shipment Costing
-- Joins S3 Shipping Logs with Postgres Product Dimensions to estimate shipping cost
CREATE OR REPLACE VIEW HybridSupply.Silver.Unified_Shipment_Cost AS
SELECT
    sh.ShipmentID,
    sh.OrderID,
    sh.Carrier,
    sh.Status,
    p.ProductName,
    p.WeightKG,
    p.Category,
    -- Simple logic: Base $10 + $2 per KG + $50 if Delayed
    (10.0 + (2.0 * p.WeightKG) + (CASE WHEN sh.Status = 'Delayed' THEN 50.0 ELSE 0.0 END)) AS EstimatedCost
FROM HybridSupply.Bronze.Bronze_Shipments sh
-- We don't have OrderID in Product table, we link via Sales (Snowflake)
JOIN HybridSupply.Bronze.Bronze_Sales sa ON sh.OrderID = sa.OrderID
JOIN HybridSupply.Bronze.Bronze_Products p ON sa.SKU = p.SKU;

-- 3.2 Inventory Risk Assessment
-- Joins Postgres Stock with Snowflake Forecasts to find shortages
CREATE OR REPLACE VIEW HybridSupply.Silver.Inventory_Risks AS
SELECT
    s.Region,
    s.SKU,
    p.ProductName,
    s.CurrentStock,
    s.SafetyStockLevel,
    f.PredictedVolume AS ForecastNextMonth,
    -- Risk Logic: If (Stock - Forecast) < SafetyStock
    CASE 
        WHEN (s.CurrentStock - f.PredictedVolume) < 0 THEN 'Stockout Imminent'
        WHEN (s.CurrentStock - f.PredictedVolume) < s.SafetyStockLevel THEN 'Low Stock Warning'
        ELSE 'Healthy'
    END AS RiskLevel
FROM HybridSupply.Bronze.Bronze_Stock s
JOIN HybridSupply.Bronze.Bronze_Products p ON s.SKU = p.SKU
JOIN HybridSupply.Bronze.Bronze_Forecasts f ON s.Region = f.Region AND p.Category = f.Category
WHERE f.ForecastMonth = '2025-06-01'; -- Looking at next month

-------------------------------------------------------------------------------
-- 4. GOLD LAYER (Business Intelligence)
-------------------------------------------------------------------------------

-- 4.1 Supply Chain Control Tower
-- Unifies Sales (Snowflake), Shipments (S3), and Stock (Postgres)
CREATE OR REPLACE VIEW HybridSupply.Gold.Supply_Chain_Control_Tower AS
SELECT
    ir.Region,
    ir.RiskLevel,
    COUNT(usc.ShipmentID) AS ActiveShipments,
    SUM(usc.EstimatedCost) AS PendingLogisticsCost,
    ir.ProductName
FROM HybridSupply.Silver.Inventory_Risks ir
LEFT JOIN HybridSupply.Silver.Unified_Shipment_Cost usc ON ir.ProductName = usc.ProductName
GROUP BY ir.Region, ir.RiskLevel, ir.ProductName;

-- 4.2 Carrier IoT Safety Scorecard
-- Joins S3 Shipments with S3 IoT data (Same source, but different 'tables')
CREATE OR REPLACE VIEW HybridSupply.Gold.Carrier_Safety_Scorecard AS
SELECT
    sh.Carrier,
    COUNT(DISTINCT sh.ShipmentID) AS TotalTrips,
    -- Count incidents where shock > 2G or Temp > 30C
    COUNT(DISTINCT CASE WHEN iot.ShockGLevel > 2.0 OR iot.ContainerTempC > 30.0 THEN sh.ShipmentID END) AS IncubentIncidents,
    AVG(iot.BatteryPct) AS AvgSensorHealth
FROM HybridSupply.Bronze.Bronze_Shipments sh
JOIN HybridSupply.Bronze.Bronze_IoT iot ON sh.ShipmentID = iot.ShipmentID
GROUP BY sh.Carrier;

-------------------------------------------------------------------------------
-- 5. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Cross-Source Join):
"List regions with 'Stockout Imminent' risk from HybridSupply.Silver.Inventory_Risks"
(Note: This view implicitly joins Postgres Stock and Snowflake Forecasts)

PROMPT 2 (Unstructured Data):
"Which carrier has the highest incident rate in HybridSupply.Gold.Carrier_Safety_Scorecard?"
(Note: This is derived from IoT sensor logs in S3)

PROMPT 3 (Cost Analysis):
"What is the total PendingLogisticsCost for 'EU' region in HybridSupply.Gold.Supply_Chain_Control_Tower?"
*/
