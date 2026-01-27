-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: String Functions
-- 
-- This script demonstrates the usage of various string manipulation functions.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure a space named 'FunctionExamplesDB' exists, or run within your Home Space.
CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

CREATE TABLE IF NOT EXISTS FunctionExamplesDB.StringData (
    ID INT,
    OriginalText VARCHAR,
    Code VARCHAR,
    Email VARCHAR
);

INSERT INTO FunctionExamplesDB.StringData (ID, OriginalText, Code, Email) VALUES
(1, '  Dremio Data Lakehouse  ', 'DRM-2023-A', 'admin@dremio.com'),
(2, 'Apache Iceberg', 'ICE-2024-B', 'developer@apache.org'),
(3, 'Data Engineering', 'ENG-2025-C', 'engineer@data.co.uk'),
(4, 'mixed CASE text', 'ABC-1234-D', 'user.name@domain.net');

-------------------------------------------------------------------------------
-- 2. EXAMPLES: String Functions
-------------------------------------------------------------------------------

-- 2.1 Casing and Formatting (UPPER, LOWER, INITCAP)
SELECT 
    OriginalText,
    UPPER(OriginalText) AS All_Caps,
    LOWER(OriginalText) AS All_Lower,
    INITCAP(OriginalText) AS Title_Case
FROM FunctionExamplesDB.StringData;

-- 2.2 Trimming and Padding (TRIM, LTRIM, RTRIM, LPAD, RPAD)
-- TRIM: Removes leading/trailing spaces.
-- LPAD/RPAD: Pads a string to a certain length.
SELECT 
    OriginalText,
    LENGTH(OriginalText) AS Raw_Len,
    TRIM(OriginalText) AS Cleaned_Text,
    LENGTH(TRIM(OriginalText)) AS Cleaned_Len,
    LPAD(Code, 15, '*') AS Left_Padded,
    RPAD(Code, 15, '-') AS Right_Padded
FROM FunctionExamplesDB.StringData;

-- 2.3 Substrings and Extraction (SUBSTRING, LEFT, RIGHT, STRPOS)
-- SUBSTRING(str, start, length) or SUBSTR
-- STRPOS(string, substring): Returns the position of the substring.
-- LEFT / RIGHT: Returns n characters from left or right.
SELECT 
    Code,
    SUBSTR(Code, 1, 3) AS Prefix,
    LEFT(Code, 3) AS Left_3,
    RIGHT(Code, 1) AS Suffix,
    STRPOS(Email, '@') AS At_Symbol_Pos,
    SUBSTRING(Email, 1, STRPOS(Email, '@') - 1) AS Username_Extracted
FROM FunctionExamplesDB.StringData;

-- 2.4 Replacement and Reversal (REPLACE, REVERSE)
SELECT 
    OriginalText,
    REPLACE(OriginalText, 'Dremio', 'The') AS Replaced_Text,
    REVERSE(Code) AS Reversed_Code
FROM FunctionExamplesDB.StringData;

-- 2.5 Concatenation (CONCAT, ||, CONCAT_WS)
-- CONCAT: Joins two or more strings.
-- || : Standard SQL concatenation operator.
-- CONCAT_WS: Concatenate With Separator.
SELECT 
    Code,
    Email,
    CONCAT('User: ', Email) AS Concat_Func,
    'User: ' || Email AS Concat_Op,
    CONCAT_WS(' | ', Code, Email, TRIM(OriginalText)) AS Joined_With_Separator
FROM FunctionExamplesDB.StringData;

-- 2.6 Regular Expressions (REGEXP_LIKE, REGEXP_REPLACE)
-- REGEXP_LIKE: Returns true if the pattern matches.
-- REGEXP_REPLACE: Replaces substring matching a regex pattern.
SELECT 
    Email,
    REGEXP_LIKE(Email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') AS Is_Valid_Email_Format,
    REGEXP_REPLACE(Code, '[0-9]', '#') AS Masked_Code_Digits
FROM FunctionExamplesDB.StringData;

-- 2.7 Length (LENGTH, CHAR_LENGTH)
SELECT
    OriginalText,
    LENGTH(OriginalText) AS Byte_Length, -- Often same as char length in basic ASCII
    CHAR_LENGTH(OriginalText) AS Character_Count
FROM FunctionExamplesDB.StringData;
