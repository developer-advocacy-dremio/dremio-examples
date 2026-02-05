/*
 * Dremio "Messy Data" Challenge: Car Maintenance History
 * 
 * Scenario: 
 * Vehicle service logs.
 * 'Odometer' readings should strictly increase over time.
 * 'VIN' (Vehicle ID) entries have typos (O vs 0, I vs 1).
 * 
 * Objective for AI Agent:
 * 1. Normalize VIN: Regex replace ambiguous chars (O->0, I->1).
 * 2. Detect Odometer Rollbacks: Flag records where Current Miles < Previous Miles for same VIN.
 * 3. Calculate Average Miles Driven per Year.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Auto_Shop;
CREATE FOLDER IF NOT EXISTS Auto_Shop.Service_Logs;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Auto_Shop.Service_Logs.WORK_ORDERS (
    WO_ID VARCHAR,
    VIN_RAW VARCHAR,
    SERVICE_DT DATE,
    ODOMETER INT,
    SERVICE_DESC VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Rollbacks, Typos)
-------------------------------------------------------------------------------

-- Normal History
INSERT INTO Auto_Shop.Service_Logs.WORK_ORDERS VALUES
('W-100', '1HGCM82633A', '2020-01-01', 10000, 'Oil Change'),
('W-101', '1HGCM82633A', '2021-01-01', 20000, 'Tires');

-- Rollback (Fraud?)
INSERT INTO Auto_Shop.Service_Logs.WORK_ORDERS VALUES
('W-102', '1HGCM82633A', '2022-01-01', 15000, 'Sale Prep'); -- Dropped 5k miles

-- VIN Typos (Same car, slightly diff string)
INSERT INTO Auto_Shop.Service_Logs.WORK_ORDERS VALUES
('W-103', '1HGCM82633A', '2023-01-01', 25000, 'Brakes'),
('W-104', 'IHGCM82633A', '2023-06-01', 30000, 'Battery'); -- I instead of 1

-- Bulk Fill
INSERT INTO Auto_Shop.Service_Logs.WORK_ORDERS VALUES
('W-200', 'VIN12345678', '2019-01-01', 5000, 'Service A'),
('W-201', 'VIN12345678', '2020-01-01', 10000, 'Service B'),
('W-202', 'VIN12345678', '2021-01-01', 15000, 'Service C'),
('W-203', 'VIN12345678', '2022-01-01', 20000, 'Service D'),
('W-204', 'VIN12345678', '2023-01-01', 500, 'Rollback!'); -- Huge rollback
('W-205', 'ABC000111', '2020-05-01', 50000, 'Oil'),
('W-206', 'ABCOOO111', '2021-05-01', 60000, 'Oil'); -- O vs 0
('W-207', 'XYZ999', '2022-01-01', 1000, 'New'),
('W-208', 'XYZ999', '2022-02-01', 1100, 'Check'),
('W-209', 'XYZ999', '2022-03-01', 1200, 'Check'),
('W-210', 'XYZ999', '2022-04-01', 1150, 'Typo?'), -- Slight rollback
('W-211', 'XYZ999', '2022-05-01', 1300, 'Check'),
('W-212', 'FORD100', '2020-01-01', 100, 'PDI'),
('W-213', 'FORD100', '2020-02-01', 200, 'Oil'),
('W-214', 'FORD100', '2020-03-01', 300, 'Oil'),
('W-215', 'FORD100', '2020-04-01', 400, 'Oil'),
('W-216', 'FORD100', '2020-05-01', 500, 'Oil'),
('W-217', 'FORD100', '2020-06-01', 600, 'Oil'),
('W-218', 'FORD100', '2020-07-01', 700, 'Oil'),
('W-219', 'FORD100', '2020-08-01', 800, 'Oil'),
('W-220', 'FORD100', '2020-09-01', 900, 'Oil'),
('W-221', 'FORD100', '2020-10-01', 1000, 'Oil'),
('W-222', 'FORD100', '2020-11-01', 999, 'Glitch'), -- -1 mile
('W-223', 'CHEVY200', '2021-01-01', 20000, 'Service'),
('W-224', 'CHEVY200', '2021-01-01', 20000, 'Service'), -- Dupe
('W-225', 'NULL', '2021-01-01', 0, 'Null VIN'),
('W-226', NULL, '2021-01-01', 0, 'Real Null VIN'),
('W-227', 'TOY300', '2022-01-01', 50000, 'Oil'),
('W-228', 'TOY300', '2020-01-01', 40000, 'Late Entry'), -- Date is older, but mile is lower. Valid order logic?
('W-229', 'HONDA400', '2023-01-01', 10, 'New'),
('W-230', 'HONDA400', '2023-01-02', 200000, 'Typo'), -- Huge jump in 1 day
('W-231', 'BMW500', '2023-01-01', -100, 'Negative'), -- Negative miles
('W-232', 'TESLA600', '2023-01-01', 5000, 'Tires'),
('W-233', 'TESLA6OO', '2023-06-01', 10000, 'Glass'); -- 600 vs 6OO

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit vehicle service history in Auto_Shop.Service_Logs.WORK_ORDERS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean VIN: Upper case, replace 'O' with '0', 'I' with '1' (common OCR errors).
 *     - Flag 'Rollbacks': Using WINDOW functions (LAG), identify rows where ODOMETER < LAG(ODOMETER) over (PARTITION BY VIN ORDER BY SERVICE_DT).
 *  3. Gold: 
 *     - Report VINs with Rollback Flags.
 *     - Calculate 'Mileage per Year' for clean VINs.
 *  
 *  Show the SQL."
 */
