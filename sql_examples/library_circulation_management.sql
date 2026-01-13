/*
 * Library Circulation Management Demo
 * 
 * Scenario:
 * Tracking book checkouts, renewals, and overdue fines.
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * Goal: Optimize detailed collection purchasing and shelf space.
 */

-------------------------------------------------------------------------------
-- 0. SETUP
-------------------------------------------------------------------------------
CREATE FOLDER IF NOT EXISTS LibraryDB;
CREATE FOLDER IF NOT EXISTS LibraryDB.Bronze;
CREATE FOLDER IF NOT EXISTS LibraryDB.Silver;
CREATE FOLDER IF NOT EXISTS LibraryDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS LibraryDB.Bronze.Books (
    ISBN VARCHAR,
    Title VARCHAR,
    Author VARCHAR,
    Genre VARCHAR,
    AcquiredDate DATE
);

CREATE TABLE IF NOT EXISTS LibraryDB.Bronze.Checkouts (
    CheckoutID INT,
    ISBN VARCHAR,
    PatronID INT,
    CheckoutDate DATE,
    DueDate DATE,
    ReturnDate DATE
);

INSERT INTO LibraryDB.Bronze.Books VALUES
('978-3-16-148410-0', 'SQL for Dummies', 'A. Author', 'Education', '2020-01-01'),
('978-0-7432-7356-5', 'The Great Gatsby', 'F. Scott Fitzgerald', 'Fiction', '2015-05-20');

INSERT INTO LibraryDB.Bronze.Checkouts VALUES
(1, '978-3-16-148410-0', 101, '2025-01-01', '2025-01-15', '2025-01-14'), -- On Time
(2, '978-0-7432-7356-5', 102, '2025-01-01', '2025-01-15', NULL); -- Overdue

-------------------------------------------------------------------------------
-- 2. SILVER LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LibraryDB.Silver.CirculationStats AS
SELECT 
    c.CheckoutID,
    b.Title,
    b.Genre,
    c.CheckoutDate,
    c.ReturnDate,
    CASE 
        WHEN c.ReturnDate IS NULL AND CURRENT_DATE > c.DueDate THEN 'Overdue'
        WHEN c.ReturnDate > c.DueDate THEN 'Returned Late'
        ELSE 'On Time' 
    END AS Status
FROM LibraryDB.Bronze.Checkouts c
JOIN LibraryDB.Bronze.Books b ON c.ISBN = b.ISBN;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW LibraryDB.Gold.GenrePopularity AS
SELECT 
    Genre,
    COUNT(CheckoutID) AS TotalCheckouts,
    SUM(CASE WHEN Status = 'Overdue' THEN 1 ELSE 0 END) AS CurrentOverdueCount
FROM LibraryDB.Silver.CirculationStats
GROUP BY Genre;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------
/*
PROMPT:
"Which Genre has the most TotalCheckouts in LibraryDB.Gold.GenrePopularity?"
*/
