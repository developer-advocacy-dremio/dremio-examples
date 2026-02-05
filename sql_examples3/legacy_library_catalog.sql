/*
 * Dremio "Messy Data" Challenge: Legacy Library Catalog
 * 
 * Scenario: 
 * Library book list with mixed ISBN formats (10-digit vs 13-digit).
 * 'Publication_Year' has Roman numerals (MCMXCIX) and typos.
 * 'Call_Number' sort order is text-based, breaking Dewey Decimal logic.
 * 
 * Objective for AI Agent:
 * 1. Normalize ISBN: distinct 13-digit if possible (ISBN-10 can convert to 13).
 * 2. Parse Roman Numeral Years to Integers.
 * 3. Normalize Call Numbers for correct sorting.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS City_Library;
CREATE FOLDER IF NOT EXISTS City_Library.Catalog;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS City_Library.Catalog.BOOKS (
    BOOK_ID VARCHAR,
    TITLE VARCHAR,
    ISBN_RAW VARCHAR,
    PUB_YEAR VARCHAR, -- '1999', 'MM', 'c. 1990'
    CALL_NUM VARCHAR
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (ISBNs, Roman Years)
-------------------------------------------------------------------------------

-- Standard
INSERT INTO City_Library.Catalog.BOOKS VALUES
('B-001', 'SQL Guide', '978-3-16-148410-0', '2020', '005.133'),
('B-002', 'History of Time', '0-553-10953-7', '1988', '520');

-- Roman Numerals / Circa
INSERT INTO City_Library.Catalog.BOOKS VALUES
('B-003', 'Old Classics', 'N/A', 'MCMXCIX', '800'), -- 1999
('B-004', 'Ancient Art', 'N/A', 'c. 1850', '700');

-- 10 vs 13 ISBN
INSERT INTO City_Library.Catalog.BOOKS VALUES
('B-005', 'Dunc', '0441172717', '1965', '813.54'), -- ISBN 10
('B-006', 'Hobbit', '9780547928227', '2012', '823.912'); -- ISBN 13

-- Bulk Fill
INSERT INTO City_Library.Catalog.BOOKS VALUES
('B-010', 'Refactoring', '0-201-48567-2', '1999', '005.1'),
('B-011', 'Clean Code', '978-0132350884', '2008', '005.1'),
('B-012', 'Design Patterns', '0-201-63361-2', '1994', '005.12'),
('B-013', 'Mythical Man Month', '0-201-83595-9', '1975', '005.1'),
('B-014', 'Pragmatic Programmer', '0-201-61622-X', '1999', '005.1'),
('B-015', 'Intro to Algo', '978-0262033848', 'MMIX', '005.1'), -- 2009
('B-016', 'K&R C', '0-13-110362-8', '1978', '005.13'),
('B-017', 'Compilers', '0-321-48681-1', '2006', '005.45'),
('B-018', 'OS Concepts', '978-1118063330', '2012', '005.43'),
('B-019', 'Database Systems', '978-0136086208', '2010', '005.74'),
('B-020', 'AI Modern Approach', '978-0136042594', 'MMX', '006.3'), -- 2010
('B-021', 'Deep Learning', '978-0262035613', '2016', '006.31'),
('B-022', 'Data Science', '978-1449358655', '2013', '005.7'),
('B-023', 'Machine Learning', '978-0070428072', '1997', '006.31'),
('B-024', 'Pattern Rec', '0-387-31073-8', '2006', '006.4'),
('B-025', 'Computer Vision', '978-0136085204', 'MMXI', '006.37'), -- 2011
('B-026', 'NLP', '0-13-187321-0', '2008', '006.35'),
('B-027', 'Robotics', '978-0262201629', '2005', '629.892'),
('B-028', 'Crypto', '0-13-795556-X', '1995', '005.82'),
('B-029', 'Security', '978-0132390774', '2011', '005.8'),
('B-030', 'Networks', '0-13-212695-8', '2010', '004.6'),
('B-031', 'TCP/IP', '0-201-63346-9', '1994', '004.62'),
('B-032', 'Distributed Sys', '0-13-214301-1', '2011', '004.36'),
('B-033', 'Cloud Computing', '978-0123748547', 'MMX', '004.678'),
('B-034', 'Big Data', '978-1118618026', '2014', '005.7'),
('B-035', 'NoSQL', '978-0321826626', '2012', '005.756'),
('B-036', 'Graph DB', '978-1449356262', '2015', '005.758'),
('B-037', 'Blockchain', '978-1491954274', '2017', '005.74'),
('B-038', 'Quantum Comp', '978-1107008545', 'MMXI', '004.1'),
('B-039', 'Bioinformatics', '978-0262015066', '2011', '570.285'),
('B-040', 'Genetics', '0-534-49260-9', '2005', '576.5'),
('B-041', 'Bad ISO', '12345', '1900', '000'), -- Invalid ISBN
('B-042', 'Null Year', '999-999', NULL, '000'),
('B-043', 'Future Year', '111-111', '3000', '000'),
('B-044', 'Text Year', '222-222', 'Ninety Nine', '000'),
('B-045', 'Sort Test 1', 'X', '2000', '10.1'),
('B-046', 'Sort Test 2', 'Y', '2000', '10.2'),
('B-047', 'Sort Test 3', 'Z', '2000', '10.10'), -- Should sort after 10.2, but string sort puts it before
('B-048', 'Sort Test 4', 'A', '2000', '2'),
('B-049', 'Sort Test 5', 'B', '2000', '20'),
('B-050', 'Sort Test 6', 'C', '2000', '100');

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Organize the library catalog in City_Library.Catalog.BOOKS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Standardize ISBN: Strip dashes/spaces. Identify ISBN-10 vs 13.
 *     - Fix 'PUB_YEAR':
 *       - Convert Roman Numerals (M=1000, D=500, C=100, etc) to Integer.
 *       - Remove 'c.' or 'circa' prefixes.
 *     - Pad 'CALL_NUM' for correct sorting (e.g. 2 -> 002.00).
 *  3. Gold: 
 *     - Count Books published per Decade.
 *  
 *  Generate the SQL."
 */
