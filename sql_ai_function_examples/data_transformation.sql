/*
 * AI Data Transformation Demo
 * 
 * Scenario:
 * Cleaning and standardizing messy data using GenAI.
 * 
 * Requirements:
 * - Dremio environment with Generative AI features enabled.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Messy Data
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS RetailDB.AI_Lab.Messy_Contacts (
    ID INT,
    Raw_Address VARCHAR,
    Raw_Phone VARCHAR
);

INSERT INTO RetailDB.AI_Lab.Messy_Contacts VALUES
(1, '123 main st, springfield, il', '(555) 123-4567'),
(2, '456 Elm Avenue, Apt 4B, New York NY 10001', '555.987.6543'),
(3, 'PO Box 789, austin texas', '5551112222');

-------------------------------------------------------------------------------
-- 1. ADDRESS NORMALIZATION
-- Standardize addresses to a proper format.
-------------------------------------------------------------------------------

SELECT
    ID,
    Raw_Address,
    AI_GENERATE_TEXT(
        'Format the following address into a standardized single-line format (Street, City, State ZIP). Address: ' || Raw_Address
    ) AS Standardized_Address
FROM RetailDB.AI_Lab.Messy_Contacts;

-------------------------------------------------------------------------------
-- 2. PHONE NUMBER FORMATTING
-- Convert all phone numbers to (XXX) XXX-XXXX.
-------------------------------------------------------------------------------

SELECT
    ID,
    Raw_Phone,
    AI_GENERATE_TEXT(
        'Reformat this phone number to (XXX) XXX-XXXX format. Return only the number. Input: ' || Raw_Phone
    ) AS Formatted_Phone
FROM RetailDB.AI_Lab.Messy_Contacts;

-------------------------------------------------------------------------------
-- 3. DATA EXTRACTION
-- Extract City and State into separate columns (JSON simulation).
-------------------------------------------------------------------------------

-- Note: AI responses are text. We can ask for JSON and then parse it if needed.
SELECT
    ID,
    Raw_Address,
    AI_GENERATE_TEXT(
        'Extract the City and State from this address and return it as valid JSON {"City": "...", "State": "..."}. Input: ' || Raw_Address
    ) AS Address_JSON
FROM RetailDB.AI_Lab.Messy_Contacts;
