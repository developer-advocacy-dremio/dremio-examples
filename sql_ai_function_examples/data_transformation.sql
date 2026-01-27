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

-- PREREQUISITE: Ensure a space named 'RetailDB' exists.
CREATE FOLDER IF NOT EXISTS RetailDB.AI_Lab;

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
    AI_GENERATE(
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
    AI_GENERATE(
        'Reformat this phone number to (XXX) XXX-XXXX format. Return only the number. Input: ' || Raw_Phone
    ) AS Formatted_Phone
FROM RetailDB.AI_Lab.Messy_Contacts;

-------------------------------------------------------------------------------
-- 3. DATA EXTRACTION
-- Extract City and State into separate columns using WITH SCHEMA.
-------------------------------------------------------------------------------

SELECT
    ID,
    Raw_Address,
    t.minc.City,
    t.minc.State
FROM (
    SELECT
        ID,
        Raw_Address,
        AI_GENERATE(
            'Extract the City and State from this address.',
            Raw_Address
        ) WITH SCHEMA (City VARCHAR, State VARCHAR) AS minc
    FROM RetailDB.AI_Lab.Messy_Contacts
) t;

