/*
 * Dremio "Messy Data" Challenge: Unstructured Retail Inventory
 * 
 * Scenario: 
 * An excel dump of inventory from multiple store managers.
 * Product names are inconsistent (typos, case sensitivity).
 * Currency symbols mixed in price columns (€, $, £).
 * 
 * Objective for AI Agent:
 * 1. Standardize Product Names (Fuzzy Match / Case Normalize).
 * 2. Extract numeric Price and normalize to USD (assume static exchange rate).
 * 3. Aggregte Stock Levels by standardized Product Name.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Retail_Dump;
CREATE FOLDER IF NOT EXISTS Retail_Dump.Excel_Sheets;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Retail_Dump.Excel_Sheets.SHEET1_INVENTORY (
    SKU_RAW VARCHAR,
    PROD_NAME VARCHAR,
    QTY INT,
    PRICE_STR VARCHAR -- '100 USD', '€90'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Typos, Mixed Currencies)
-------------------------------------------------------------------------------

-- iPhone Variations
INSERT INTO Retail_Dump.Excel_Sheets.SHEET1_INVENTORY VALUES
('SKU-001', 'iPhone 14', 10, '$999.00'),
('SKU-001', 'iphone 14', 5, '999 USD'),
('SKU-001', 'I-Phone 14', 2, '€950'), -- Euro
('SKU-001', 'IPHONE 14 Pro', 1, '£1100'); -- GBP

-- Samsung Variations
INSERT INTO Retail_Dump.Excel_Sheets.SHEET1_INVENTORY VALUES
('SKU-002', 'Samsung S23', 20, '$800'),
('SKU-002', 'Samsumg S23', 3, '$800'), -- Typo
('SKU-002', 'Galaxy S23', 5, '$800');

-- Laptop Variations
INSERT INTO Retail_Dump.Excel_Sheets.SHEET1_INVENTORY VALUES
('SKU-003', 'MacBook Air M2', 10, '$1200'),
('SKU-003', 'Macbook air m2', 5, '$1200'),
('SKU-003', 'MB  Air M2', 2, '$1200');

-- Bulk Fill
INSERT INTO Retail_Dump.Excel_Sheets.SHEET1_INVENTORY VALUES
('SKU-004', 'Usb Cable', 100, '$10'),
('SKU-004', 'USB cable', 50, '$10'),
('SKU-004', 'usb-c cable', 20, '$12'), -- Different item matches SKU?
('SKU-005', 'Mouse', 10, '$20'),
('SKU-005', 'Mouse ', 10, '$20'), -- Trailing space
('SKU-005', ' Mouse', 10, '$20'), -- Leading space
('SKU-006', 'Keyboard', 5, '30 USD'),
('SKU-006', 'Key board', 2, '30 USD'),
('SKU-007', 'Monitor', 10, '€200'),
('SKU-007', 'Screen', 5, '€200'),
('SKU-008', 'Headset', 15, '£50'),
('SKU-008', 'Head phones', 5, '£50'),
('SKU-009', 'Webcam', 20, '$50'),
('SKU-009', 'Web Cam', 5, '$50'),
('SKU-010', 'Dock', 10, '$150'),
('SKU-010', 'Docking Station', 5, '$150'),
('SKU-011', 'Chair', 10, '$200'),
('SKU-011', 'Office Chair', 5, '$200'),
('SKU-012', 'Desk', 5, '$300'),
('SKU-012', 'Stand Desk', 2, '$500'),
('SKU-013', 'Lamp', 20, '$20'),
('SKU-013', 'Light', 5, '$20'),
('SKU-014', 'Pad', 50, '$5'),
('SKU-014', 'Mousepad', 20, '$5'),
('SKU-015', 'Drive', 10, '$100'),
('SKU-015', 'HDD', 5, '$100'),
('SKU-015', 'SSD', 5, '$150'), -- Mixed types same SKU? Messy!
('SKU-016', 'Printer', 5, '$150'),
('SKU-016', 'Printr', 1, '$150'), -- Typo
('SKU-017', 'Ink', 20, '$50'),
('SKU-017', 'Toner', 10, '$80'),
('SKU-018', 'Paper', 100, '$5'),
('SKU-018', 'A4 Paper', 50, '$5'),
('SKU-019', 'Stapler', 10, '$5'),
('SKU-019', 'Staplerv', 1, '$5'), -- Typo
('SKU-020', 'Pen', 100, '$1'),
('SKU-020', 'Pens', 50, '$1'), -- Plural
('SKU-021', 'Pencil', 100, '$0.5'),
('SKU-021', 'Pencils', 50, '$0.5'),
('SKU-022', 'Notebook', 50, '$2'),
('SKU-022', 'Note book', 20, '$2'),
('SKU-023', 'Folder', 50, '$1'),
('SKU-023', 'File Folder', 20, '$1'),
('SKU-024', 'Marker', 50, '$2'),
('SKU-024', 'Markers', 20, '$2'),
('SKU-025', 'Tape', 50, '$2'),
('SKU-025', 'Scotch Tape', 20, '$2'),
('SKU-026', 'Glue', 20, '$2'),
('SKU-026', 'Glue Stick', 10, '$2');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Refine the inventory data in Retail_Dump.Excel_Sheets.SHEET1_INVENTORY.
 *  
 *  1. Bronze: Raw view.
 *  2. Silver: 
 *     - Clean 'PROD_NAME' by trimming whitespace and normalizing case.
 *     - Parse 'PRICE_STR' to numeric. Detect currency symbols (€, £) and apply a rough conversion rate to USD.
 *  3. Gold: 
 *     - Aggregate Total Inventory Value (Price * Qty) by standardized Product Name.
 *  
 *  Show the SQL for these cleaning steps."
 */
