/*
 * Dremio "Messy Data" Challenge: Tax Authority Fraud
 * 
 * Scenario: 
 * Tax return processing.
 * 3 Tables: FILERS (Entity), RETURNS (Forms), AUDITS (Flags).
 * 
 * Objective for AI Agent:
 * 1. Join Tables: RETURNS -> FILERS and AUDITS.
 * 2. Risk Score: High Income + Low Tax = Risk.
 * 3. Detect Fraud: Same 'Bank_Account_Hash' for different Filers.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Govt_Tax;
CREATE FOLDER IF NOT EXISTS Govt_Tax.IRS;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Govt_Tax.IRS.FILERS (
    SSN_HASH VARCHAR,
    NAME VARCHAR,
    ADDRESS VARCHAR
);

CREATE TABLE IF NOT EXISTS Govt_Tax.IRS.RETURNS (
    RETURN_ID VARCHAR,
    SSN_HASH VARCHAR,
    TAX_YEAR INT,
    INCOME DOUBLE,
    TAX_PAID DOUBLE,
    BANK_ACCT_HASH VARCHAR
);

CREATE TABLE IF NOT EXISTS Govt_Tax.IRS.AUDITS (
    AUDIT_ID VARCHAR,
    RETURN_ID VARCHAR,
    STATUS VARCHAR -- 'Open', 'Closed', 'Fraud'
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- FILERS (20 Rows)
INSERT INTO Govt_Tax.IRS.FILERS VALUES
('Hash-1', 'Alice', '123 Main'), ('Hash-2', 'Bob', '456 Oak'),
('Hash-3', 'Charlie', '789 Pine'), ('Hash-4', 'Dave', '101 Maple'),
('Hash-5', 'Eve', '123 Main'), -- Dupe Address w/ Alice
('Hash-6', 'Frank', '202 Elm'), ('Hash-7', 'Grace', '303 Ash'),
('Hash-8', 'Hank', '404 Birch'), ('Hash-9', 'Ivy', '505 Cedar'),
('Hash-10', 'Jack', '606 Dogwood'), ('Hash-11', 'Kevin', '707 Elm'),
('Hash-12', 'Leo', '808 Fir'), ('Hash-13', 'Mike', '909 Gum'),
('Hash-14', 'Nick', '1010 Holly'), ('Hash-15', 'Oscar', '1111 Ivy'),
('Hash-16', 'Paul', '1212 Juniper'), ('Hash-17', 'Quinn', '1313 Kelp'),
('Hash-18', 'Ray', '1414 Lily'), ('Hash-19', 'Sam', '1515 Maple'),
('Hash-20', 'Tom', '1616 Nut');

-- RETURNS (50 Rows)
INSERT INTO Govt_Tax.IRS.RETURNS VALUES
('R-1', 'Hash-1', 2022, 100000.0, 20000.0, 'Acct-A'),
('R-2', 'Hash-2', 2022, 50000.0, 10000.0, 'Acct-B'),
('R-3', 'Hash-3', 2022, 1000000.0, 10.0, 'Acct-C'), -- Fraud Risk
('R-4', 'Hash-4', 2022, 20000.0, 0.0, 'Acct-D'),
('R-5', 'Hash-5', 2022, 20000.0, 0.0, 'Acct-A'), -- Same Acct as Alice (Risk)
('R-6', 'Hash-6', 2022, 80000.0, 16000.0, 'Acct-E'),
('R-7', 'Hash-7', 2022, 90000.0, 18000.0, 'Acct-F'),
('R-8', 'Hash-8', 2022, 100000.0, 20000.0, 'Acct-G'),
('R-9', 'Hash-9', 2022, 110000.0, 22000.0, 'Acct-H'),
('R-10', 'Hash-10', 2022, 120000.0, 24000.0, 'Acct-I'),
('R-11', 'Hash-11', 2022, 130000.0, 26000.0, 'Acct-J'),
('R-12', 'Hash-12', 2022, 140000.0, 28000.0, 'Acct-K'),
('R-13', 'Hash-13', 2022, 150000.0, 30000.0, 'Acct-L'),
('R-14', 'Hash-14', 2022, 160000.0, 32000.0, 'Acct-M'),
('R-15', 'Hash-15', 2022, 170000.0, 34000.0, 'Acct-N'),
('R-16', 'Hash-16', 2022, 180000.0, 36000.0, 'Acct-O'),
('R-17', 'Hash-17', 2022, 190000.0, 38000.0, 'Acct-P'),
('R-18', 'Hash-18', 2022, 200000.0, 40000.0, 'Acct-Q'),
('R-19', 'Hash-19', 2022, 210000.0, 42000.0, 'Acct-R'),
('R-20', 'Hash-20', 2022, 220000.0, 44000.0, 'Acct-S'),
('R-21', 'Hash-1', 2023, 110000.0, 22000.0, 'Acct-A'),
('R-22', 'Hash-2', 2023, 55000.0, 11000.0, 'Acct-B'),
('R-23', 'Hash-3', 2023, 100000.0, 20.0, 'Acct-C'),
('R-24', 'Hash-4', 2023, 22000.0, 1000.0, 'Acct-D'),
('R-25', 'Hash-5', 2023, 22000.0, 1000.0, 'Acct-A'),
('R-26', 'Hash-6', 2023, 85000.0, 17000.0, 'Acct-E'),
('R-27', 'Hash-7', 2023, 95000.0, 19000.0, 'Acct-F'),
('R-28', 'Hash-8', 2023, 105000.0, 21000.0, 'Acct-G'),
('R-29', 'Hash-9', 2023, 115000.0, 23000.0, 'Acct-H'),
('R-30', 'Hash-10', 2023, 125000.0, 25000.0, 'Acct-I'),
('R-31', 'Hash-11', 2023, 135000.0, 27000.0, 'Acct-J'),
('R-32', 'Hash-12', 2023, 145000.0, 29000.0, 'Acct-K'),
('R-33', 'Hash-13', 2023, 155000.0, 31000.0, 'Acct-L'),
('R-34', 'Hash-14', 2023, 165000.0, 33000.0, 'Acct-M'),
('R-35', 'Hash-15', 2023, 175000.0, 35000.0, 'Acct-N'),
('R-36', 'Hash-16', 2023, 185000.0, 37000.0, 'Acct-O'),
('R-37', 'Hash-17', 2023, 195000.0, 39000.0, 'Acct-P'),
('R-38', 'Hash-18', 2023, 205000.0, 41000.0, 'Acct-Q'),
('R-39', 'Hash-19', 2023, 215000.0, 43000.0, 'Acct-R'),
('R-40', 'Hash-20', 2023, 225000.0, 45000.0, 'Acct-S'),
('R-41', 'Hash-1', 2021, 90000.0, 18000.0, 'Acct-A'),
('R-42', 'Hash-2', 2021, 45000.0, 9000.0, 'Acct-B'),
('R-43', 'Hash-3', 2021, 900000.0, 5.0, 'Acct-C'),
('R-44', 'Hash-4', 2021, 18000.0, 0.0, 'Acct-D'),
('R-45', 'Hash-5', 2021, 18000.0, 0.0, 'Acct-A'),
('R-46', 'Hash-6', 2021, 75000.0, 15000.0, 'Acct-E'),
('R-47', 'Hash-7', 2021, 85000.0, 17000.0, 'Acct-F'),
('R-48', 'Hash-8', 2021, 95000.0, 19000.0, 'Acct-G'),
('R-49', 'Hash-9', 2021, 105000.0, 21000.0, 'Acct-H'),
('R-50', 'Hash-10', 2021, 115000.0, 23000.0, 'Acct-I');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Detect tax fraud in Govt_Tax.IRS.
 *  
 *  1. Bronze: Raw View of RETURNS, FILERS, AUDITS.
 *  2. Silver: 
 *     - Join: RETURNS -> FILERS.
 *     - Calc Tax Rate: Tax_Paid / Income.
 *  3. Gold: 
 *     - Suspicious Activity: 
 *       a) Income > 1M AND Tax_Rate < 0.01.
 *       b) Returns sharing same Bank_Account_Hash but different SSN_Hash.
 *  
 *  Show the SQL."
 */
