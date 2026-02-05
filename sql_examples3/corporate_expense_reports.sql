/*
 * Dremio "Messy Data" Challenge: Corporate Expense Reports
 * 
 * Scenario: 
 * Scanned receipt data (OCR output).
 * 'Amount' often has typos ('100..00', '1O.00').
 * 'Currency' mismatch (USD vs EUR mixed in column).
 * Duplicates (Same amount, same date, same vendor).
 * 
 * Objective for AI Agent:
 * 1. Clean Amount: Remove extra dots, replace 'O' with '0'.
 * 2. Normalize Currency: Convert everything to USD (Approx rates).
 * 3. Fraud Check: Detect identical expenses submitted by different 'User_ID's.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Fin_Corp;
CREATE FOLDER IF NOT EXISTS Fin_Corp.Expenses;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Fin_Corp.Expenses.RECEIPTS (
    REPORT_ID VARCHAR,
    USER_ID VARCHAR,
    DATE_VAL DATE,
    VENDOR VARCHAR,
    AMOUNT_RAW VARCHAR, -- '100.00'
    CURRENCY VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (OCR Errors, Dupes)
-------------------------------------------------------------------------------

-- Normal
INSERT INTO Fin_Corp.Expenses.RECEIPTS VALUES
('R-1', 'U-1', '2023-01-01', 'Uber', '25.50', 'USD'),
('R-2', 'U-1', '2023-01-01', 'Starbucks', '5.00', 'USD');

-- OCR Typos
INSERT INTO Fin_Corp.Expenses.RECEIPTS VALUES
('R-3', 'U-2', '2023-01-01', 'Hotel', '1OO.00', 'USD'), -- Letter O
('R-4', 'U-2', '2023-01-01', 'Flight', '500..00', 'USD'); -- Double dot

-- Duplicate Submission (Fraud?)
INSERT INTO Fin_Corp.Expenses.RECEIPTS VALUES
('R-5', 'U-3', '2023-01-01', 'Dinner', '100.00', 'USD'),
('R-6', 'U-4', '2023-01-01', 'Dinner', '100.00', 'USD'); -- Same vendor/amt, diff user

-- Currency Mix
INSERT INTO Fin_Corp.Expenses.RECEIPTS VALUES
('R-7', 'U-5', '2023-01-01', 'Train', '50.00', 'EUR'),
('R-8', 'U-5', '2023-01-01', 'Taxi', '5000', 'JPY');

-- Bulk Fill
INSERT INTO Fin_Corp.Expenses.RECEIPTS VALUES
('R-10', 'U-6', '2023-01-01', 'Lunch', '20.00', 'USD'),
('R-11', 'U-6', '2023-01-01', 'Lunch', '20,00', 'USD'), -- Comma decimal
('R-12', 'U-7', '2023-01-01', 'Software', '$100.00', 'USD'), -- Symbol included
('R-13', 'U-8', '2023-01-01', 'Hardware', '1,000.00', 'USD'),
('R-14', 'U-9', '2023-01-01', 'Unknown', NULL, 'USD'),
('R-15', 'U-10', '2023-01-01', 'Taxi', '10.l5', 'USD'), -- l for 1
('R-16', 'U-11', '2023-01-01', 'Uber', '25.50', 'USD'),
('R-16', 'U-11', '2023-01-01', 'Uber', '25.50', 'USD'), -- Exact Dupe
('R-17', 'U-12', '2023-01-01', 'Amazon', '50.00', 'GBP'),
('R-18', 'U-13', '2023-01-01', 'AWS', '125.50', 'USD'),
('R-19', 'U-14', '2023-01-01', 'Google', '10.00', 'USD'),
('R-20', 'U-15', '2023-01-01', 'Apple', '10.00', 'USD'),
('R-21', 'U-16', '2023-01-01', 'Meta', '10.00', 'USD'),
('R-22', 'U-17', '2023-01-02', 'Navi', '10.00', 'USD'),
('R-23', 'U-18', '2023-01-03', 'Zoom', '150.00', 'USD'),
('R-24', 'U-19', '2023-01-01', 'Slack', '8.00', 'USD'),
('R-25', 'U-20', '2023-01-01', 'Teams', '0.00', 'USD'), -- Free?
('R-26', 'U-21', '2023-01-01', 'Jira', '-10.00', 'USD'), -- Negative
('R-27', 'U-22', '2023-01-01', 'Confluence', '1000000.00', 'USD'), -- Outlier
('R-28', 'U-23', '2023-01-01', 'Vendor', '10.0', 'USD'),
('R-29', 'U-24', '2023-01-01', 'Vendor', '.50', 'USD'), -- No leading zero
('R-30', 'U-25', '2023-01-01', 'Vendor', '10.', 'USD'), -- Trailing dot
('R-31', 'U-26', '2023-01-01', 'Vendor', '1 000', 'USD'), -- Space
('R-32', 'U-27', '2023-01-01', 'Vendor', '10-00', 'USD'), -- Dash
('R-33', 'U-28', '2023-01-01', 'Vendor', '10_00', 'USD'),
('R-34', 'U-29', '2023-01-01', 'Vendor', 'Ten', 'USD'), -- Text
('R-35', 'U-30', '2023-01-01', 'Vendor', '100.0O', 'USD');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Audit expenses in Fin_Corp.Expenses.RECEIPTS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Clean Amount: RegEx replace non-numeric characters (allow one decimal). Cast to Double.
 *     - Convert Currency: IF EUR then Amount * 1.1, IF JPY then Amount * 0.007.
 *  3. Gold: 
 *     - Suspicious List: Duplicate (Vendor, Date, Amount) tuples.
 *  
 *  Show the SQL."
 */
