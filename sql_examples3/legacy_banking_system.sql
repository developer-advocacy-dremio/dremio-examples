/*
 * Dremio "Messy Data" Challenge: Legacy Banking System
 * 
 * Scenario: 
 * You have ingested a dump from a COBOL mainframe system.
 * The table and column names are opaque codes (e.g., T_1001, C2).
 * There are duplicates and deleted flags that need handling.
 * 
 * Objective for AI Agent:
 * 1. Identify which table is Customers and which is Transactions based on data patterns.
 * 2. Rename columns to meaningful names (e.g., C2 -> Account_ID).
 * 3. Filter out records where 'X_FLG' = 'D' (Deleted).
 * 4. Deduplicate the transaction log.
 * 5. Create a Gold view summing transaction amounts by Customer.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Old_Bank_System;
CREATE FOLDER IF NOT EXISTS Old_Bank_System.Raw_Dump;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (Opaque Schema)
-------------------------------------------------------------------------------

-- Likely Customers Table
CREATE TABLE IF NOT EXISTS Old_Bank_System.Raw_Dump.T_REF_01 (
    R_ID VARCHAR,
    C_NM VARCHAR,
    C_DT DATE,
    X_FLG VARCHAR -- 'A' = Active, 'D' = Deleted
);

-- Likely Transactions Table
CREATE TABLE IF NOT EXISTS Old_Bank_System.Raw_Dump.T_DAT_99 (
    T_REF VARCHAR, -- UUID?
    R_LINK VARCHAR, -- FK to R_ID?
    AMT_V DOUBLE,
    T_TS TIMESTAMP,
    MEMO_TXT VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Messy: Duplicates, Opaqueness)
-------------------------------------------------------------------------------

-- Customers
INSERT INTO Old_Bank_System.Raw_Dump.T_REF_01 VALUES
('C001', 'Acme Corp', '2000-01-01', 'A'),
('C002', 'Globex', '1999-05-20', 'A'),
('C003', 'Soylent', '2020-03-10', 'D'), -- Deleted
('C004', 'Initech', '2015-08-15', 'A'),
('C005', 'Umbrella', '2010-10-31', 'A');
-- ... duplicates (mainframe quirks)
INSERT INTO Old_Bank_System.Raw_Dump.T_REF_01 VALUES
('C001', 'Acme Corp', '2000-01-01', 'A'); -- Duplicate

-- Transactions
INSERT INTO Old_Bank_System.Raw_Dump.T_DAT_99 VALUES
('TX-100', 'C001', 5000.00, '2023-01-01 10:00:00', 'Inv 101'),
('TX-101', 'C001', -200.00, '2023-01-02 11:00:00', 'Fee'),
('TX-102', 'C002', 10000.00, '2023-01-03 12:00:00', 'Service'),
('TX-103', 'C003', 50.00, '2023-01-04 14:00:00', 'Closing'), -- Deleted cust trans
('TX-104', 'C009', 999.00, '2023-01-05 09:00:00', 'Orphan Record'); -- Orphan FK

-- Generate bulk
INSERT INTO Old_Bank_System.Raw_Dump.T_DAT_99 VALUES
('TX-201', 'C001', 100.0, '2023-02-01 10:00:00', 'Dep'),
('TX-202', 'C001', 100.0, '2023-02-01 10:01:00', 'Dep'),
('TX-203', 'C001', 100.0, '2023-02-01 10:02:00', 'Dep'),
('TX-204', 'C002', 200.0, '2023-02-01 10:03:00', 'Dep'),
('TX-205', 'C002', 200.0, '2023-02-01 10:04:00', 'Dep'),
('TX-206', 'C004', 500.0, '2023-02-01 10:05:00', 'Dep'),
('TX-206', 'C004', 500.0, '2023-02-01 10:05:00', 'Dep'), -- Exact Duplicate row
('TX-207', 'C004', -50.0, '2023-02-01 10:06:00', 'Wth'),
('TX-208', 'C005', 1000.0, '2023-02-01 10:07:00', 'Dep'),
('TX-209', 'C005', -100.0, '2023-02-01 10:08:00', 'Fee'),
('TX-210', 'C001', 12.50, '2023-02-02 08:00:00', 'Int'),
('TX-211', 'C001', 12.50, '2023-02-03 08:00:00', 'Int'),
('TX-212', 'C001', 12.50, '2023-02-04 08:00:00', 'Int'),
('TX-213', 'C002', 50.00, '2023-02-02 08:00:00', 'Int'),
('TX-214', 'C002', 50.00, '2023-02-03 08:00:00', 'Int'),
('TX-215', 'C002', 50.00, '2023-02-04 08:00:00', 'Int'),
('TX-216', 'C001', -5.00, '2023-02-05 09:00:00', 'Fee'),
('TX-217', 'C001', -5.00, '2023-02-06 09:00:00', 'Fee'),
('TX-218', 'C001', -5.00, '2023-02-07 09:00:00', 'Fee'),
('TX-219', 'C004', 10000.0, '2023-03-01 10:00:00', 'Wire'),
('TX-220', 'C004', -25.0, '2023-03-01 10:00:00', 'WireFee'),
('TX-221', 'C005', 5000.0, '2023-03-01 10:00:00', 'Wire'),
('TX-222', 'C005', 5000.0, '2023-03-01 10:00:00', 'Wire'), -- Double charge?
('TX-223', 'C005', -25.0, '2023-03-01 10:00:00', 'WireFee'),
('TX-224', 'C001', NULL, '2023-03-02 12:00:00', 'Error'), -- Null Amount
('TX-225', 'C001', 0.00, '2023-03-02 12:01:00', 'Null Check'),
('TX-226', 'C002', 75.00, '2023-03-03 14:00:00', 'POS'),
('TX-227', 'C002', 25.00, '2023-03-03 15:00:00', 'POS'),
('TX-228', 'C002', 15.00, '2023-03-03 16:00:00', 'POS'),
('TX-229', 'C002', 10.00, '2023-03-03 17:00:00', 'POS'),
('TX-230', 'C002', 5.00, '2023-03-03 18:00:00', 'POS'),
('TX-231', 'C004', 100.0, '2023-03-04 09:00:00', 'ATM'),
('TX-232', 'C004', 100.0, '2023-03-04 09:00:00', 'ATM'), -- Dupe?
('TX-233', 'C004', -3.00, '2023-03-04 09:00:00', 'ATM Fee'),
('TX-234', 'C005', 200.0, '2023-03-05 10:00:00', 'ATM'),
('TX-235', 'C005', -3.00, '2023-03-05 10:00:00', 'ATM Fee'),
('TX-236', 'C001', 11.11, '2023-04-01 12:00:00', 'Test'),
('TX-237', 'C001', 22.22, '2023-04-01 12:00:00', 'Test'),
('TX-238', 'C001', 33.33, '2023-04-01 12:00:00', 'Test'),
('TX-239', 'C001', 44.44, '2023-04-01 12:00:00', 'Test'),
('TX-240', 'C001', 55.55, '2023-04-01 12:00:00', 'Test'),
('TX-241', 'C001', 66.66, '2023-04-01 12:00:00', 'Test'),
('TX-242', 'C001', 77.77, '2023-04-01 12:00:00', 'Test'),
('TX-243', 'C001', 88.88, '2023-04-01 12:00:00', 'Test'),
('TX-244', 'C001', 99.99, '2023-04-01 12:00:00', 'Test'),
('TX-245', 'C002', 1000.0, '2023-04-02 12:00:00', 'Transfer'),
('TX-246', 'C002', -1000.0, '2023-04-02 12:00:00', 'Transfer Out'),
('TX-247', 'C002', 500.0, '2023-04-02 12:00:00', 'Transfer In'),
('TX-248', 'C005', 9000.0, '2023-04-03 09:00:00', 'Bonus'),
('TX-249', 'C005', 100.0, '2023-04-03 10:00:00', 'Petty Cash'),
('TX-250', 'C005', -100.0, '2023-04-03 11:00:00', 'Petty Cash Ret');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "I have raw data in Old_Bank_System.Raw_Dump.
 *  Please generate a Medallion Architecture (Bronze/Silver/Gold) using SQL.
 *  
 *  1. Bronze: Create views wrapping the raw tables T_REF_01 and T_DAT_99.
 *  2. Silver: 
 *     - Rename opaque columns to meaningful names (e.g., C_NM -> Customer_Name, AMT_V -> Amount).
 *     - Filter out Customers where X_FLG is 'D' (Deleted).
 *     - Deduplicate Transactions based on T_REF or content.
 *  3. Gold: 
 *     - Aggregate total transaction volume and count by Customer Name.
 *  
 *  Show me the SQL for these layers."
 */
