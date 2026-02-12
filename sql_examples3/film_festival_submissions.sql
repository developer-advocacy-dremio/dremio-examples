/*
 * Dremio "Messy Data" Challenge: Film Festival Submissions
 * 
 * Scenario: 
 * Regional film festival managing submissions across multiple years and platforms.
 * 3 Tables: FILMMAKERS, FILMS, SCREENINGS.
 * 
 * Objective for AI Agent:
 * 1. Runtime Parsing: Normalize '92 min' / '1h32m' / '01:32:00' / '5520s' to minutes.
 * 2. Genre Explosion: Split 'Drama/Comedy' and 'Sci-Fi, Thriller' into individual genres.
 * 3. Duplicate Detection: Same film submitted under variant titles across years.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Lone_Star_Film_Fest;
CREATE FOLDER IF NOT EXISTS Lone_Star_Film_Fest.Submissions;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Lone_Star_Film_Fest.Submissions.FILMMAKERS (
    MAKER_ID VARCHAR,
    MAKER_NAME VARCHAR,
    EMAIL VARCHAR,
    COUNTRY VARCHAR
);

CREATE TABLE IF NOT EXISTS Lone_Star_Film_Fest.Submissions.FILMS (
    FILM_ID VARCHAR,
    MAKER_ID VARCHAR,
    TITLE VARCHAR,
    RUNTIME_RAW VARCHAR, -- '92 min', '1h32m', '01:32:00', '5520s'
    GENRE_RAW VARCHAR, -- 'Drama/Comedy', 'Sci-Fi, Thriller', 'drama'
    SUBMIT_YEAR INT,
    LANG VARCHAR, -- 'English', 'en', 'ENG', 'Spanish', 'es'
    BUDGET_RAW VARCHAR -- '$50K', '50000', '$2.5M', 'No Budget', NULL
);

CREATE TABLE IF NOT EXISTS Lone_Star_Film_Fest.Submissions.SCREENINGS (
    SCREEN_ID VARCHAR,
    FILM_ID VARCHAR,
    VENUE VARCHAR,
    SCREEN_DT DATE,
    SCREEN_TIME VARCHAR, -- '7:30 PM', '19:30', '7:30pm'
    ATTENDANCE INT,
    RATING_AVG DOUBLE -- Audience rating
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- FILMMAKERS (10 rows)
INSERT INTO Lone_Star_Film_Fest.Submissions.FILMMAKERS VALUES
('FM-001', 'Alex Rivera', 'arivera@filmmail.com', 'USA'),
('FM-002', 'Sophie Chen', 'sophie.chen@gmail.com', 'Canada'),
('FM-003', 'Raj Kapoor', 'rajk@bollywood.in', 'India'),
('FM-004', 'Elena Voss', 'elena.voss@email.de', 'Germany'),
('FM-005', 'Taro Yamada', 'taro@film.jp', 'Japan'),
('FM-006', 'Maria Santos', 'msantos@cine.mx', 'Mexico'),
('FM-007', 'Liam O''Brien', 'liam@irishfilm.ie', 'Ireland'),
('FM-008', 'Fatima Al-Rashid', 'fatima@arabcinema.ae', 'UAE'),
('FM-009', 'Alex Rivera', 'alex.rivera@newmail.com', 'US'),   -- Same person, different email
('FM-010', 'Unknown Director', NULL, NULL);

-- FILMS (25 rows with runtime/genre/budget mess)
INSERT INTO Lone_Star_Film_Fest.Submissions.FILMS VALUES
('F-001', 'FM-001', 'Desert Bloom', '92 min', 'Drama', 2022, 'English', '$50K'),
('F-002', 'FM-001', 'Desert Bloom: Director''s Cut', '01:42:00', 'Drama', 2023, 'en', '$50,000'),  -- Same film, extended
('F-003', 'FM-002', 'Northern Lights', '1h45m', 'Sci-Fi, Drama', 2022, 'English', '$200K'),
('F-004', 'FM-002', 'Northern Lights', '105 min', 'SciFi/Drama', 2023, 'ENG', '$200,000'),  -- Resubmission
('F-005', 'FM-003', 'Monsoon Wedding', '6300s', 'Drama/Comedy/Romance', 2022, 'Hindi', '₹5Cr'),
('F-006', 'FM-003', 'Monsoon Dreams', '105m', 'Romantic Comedy', 2023, 'Hindi', '50000 USD'),  -- Title variant
('F-007', 'FM-004', 'Berlin After Dark', '88 min', 'Thriller', 2022, 'German', '€120K'),
('F-008', 'FM-004', 'Berlin After Dark', '88', 'thriller', 2023, 'de', '€120,000'),  -- Resubmission
('F-009', 'FM-005', 'Cherry Blossom', '75 min', 'Animation', 2022, 'Japanese', '¥15M'),
('F-010', 'FM-005', 'Sakura', '1:15:00', 'Anime/Drama', 2023, 'jp', '¥15,000,000'),  -- Same film, Japanese title
('F-011', 'FM-006', 'Border Town', '110 min', 'Drama', 2023, 'Spanish', '$80K'),
('F-012', 'FM-006', 'Pueblo Fronterizo', '1h50m', 'Drama', 2023, 'es', '$80,000'),  -- Same film, Spanish title
('F-013', 'FM-007', 'The Green Shore', '98 min', 'Drama/Adventure', 2022, 'English', '€50K'),
('F-014', 'FM-008', 'Sand and Stars', '120 min', 'Drama', 2023, 'Arabic', 'AED 500K'),
('F-015', 'FM-010', 'Untitled', '', '', 2023, NULL, NULL),  -- Incomplete submission
('F-016', 'FM-001', 'Sprint', '12 min', 'Short/Drama', 2022, 'English', '$5K'),  -- Short film
('F-017', 'FM-002', 'Midnight', '8m', 'Short, Horror', 2022, 'en', '$2,000'),
('F-018', 'FM-004', 'Zwischenraum', '15min', 'Experimental', 2023, 'German', 'No Budget'),
('F-019', 'FM-007', 'Emerald', '0:22:00', 'Short/Drama', 2023, 'English', '€3K'),
('F-020', 'FM-009', 'Desert Bloom', '92 min', 'Drama', 2023, 'English', '$50K'),  -- FM-009 = FM-001 resubmit!
('F-021', 'FM-003', 'City of Joy', '130 min', 'Drama', 2023, 'Hindi', '$100K'),
('F-022', 'FM-005', 'Neon Tokyo', '95 min', 'Sci-Fi', 2023, 'Japanese', '¥20M'),
('F-023', 'FM-006', 'Last Summer', '85 min', 'Comedy', 2022, 'Spanish', '$30K'),
('F-024', 'FM-008', 'Oasis', '100 min', 'Drama', 2022, 'Arabic', 'AED 300K'),
('F-025', 'FM-007', 'Wild Atlantic', '-10 min', 'Documentary', 2023, 'English', '€25K');  -- Negative runtime!

-- SCREENINGS (25+ rows)
INSERT INTO Lone_Star_Film_Fest.Submissions.SCREENINGS VALUES
('SC-001', 'F-001', 'Paramount Theatre', '2022-10-15', '7:30 PM', 250, 4.2),
('SC-002', 'F-001', 'Alamo Drafthouse', '2022-10-16', '19:30', 180, 4.1),
('SC-003', 'F-003', 'Paramount Theatre', '2022-10-15', '2:00 PM', 300, 4.5),
('SC-004', 'F-005', 'State Theatre', '2022-10-17', '8pm', 200, 3.8),
('SC-005', 'F-007', 'Alamo Drafthouse', '2022-10-17', '9:30PM', 150, 4.0),
('SC-006', 'F-009', 'Kids Cinema', '2022-10-18', '10:00 AM', 400, 4.7),
('SC-007', 'F-013', 'Paramount Theatre', '2022-10-18', '7:30 PM', 220, 3.9),
('SC-008', 'F-002', 'Paramount Theatre', '2023-10-14', '7:30 PM', 280, 4.3),
('SC-009', 'F-004', 'State Theatre', '2023-10-14', '2:00 pm', 310, 4.6),
('SC-010', 'F-006', 'Alamo Drafthouse', '2023-10-15', '8:00 PM', 190, 3.9),
('SC-011', 'F-008', 'Paramount Theatre', '2023-10-15', '21:30', 160, 4.1),
('SC-012', 'F-010', 'Kids Cinema', '2023-10-16', '10:00AM', 380, 4.8),
('SC-013', 'F-011', 'State Theatre', '2023-10-16', '7:30 PM', 200, 3.7),
('SC-014', 'F-014', 'Paramount Theatre', '2023-10-17', '7:30 PM', 170, 4.4),
('SC-015', 'F-016', 'Shorts Block A', '2022-10-16', '1:00 PM', 100, 4.0),
('SC-016', 'F-017', 'Shorts Block A', '2022-10-16', '1:00 PM', 100, 3.5),
('SC-017', 'F-018', 'Shorts Block B', '2023-10-15', '3:00 PM', 80, 3.2),
('SC-018', 'F-019', 'Shorts Block B', '2023-10-15', '3:00 PM', 80, 4.1),
('SC-019', 'F-021', 'State Theatre', '2023-10-17', '7:30 PM', 230, 4.0),
('SC-020', 'F-022', 'Alamo Drafthouse', '2023-10-18', '9:00 PM', 175, 4.3),
('SC-021', 'F-001', 'Paramount Theatre', '2022-10-15', '7:30 PM', 250, 4.2),  -- Dupe of SC-001
('SC-022', 'F-999', 'Ghost Theatre', '2023-10-20', '8:00 PM', 0, NULL),  -- Orphan + zero attendance
('SC-023', 'F-023', 'State Theatre', '2022-10-18', '4:00 PM', 140, 3.6),
('SC-024', 'F-024', 'Paramount Theatre', '2022-10-19', '8:00 PM', 160, 4.2),
('SC-025', 'F-025', 'Alamo Drafthouse', '2023-10-19', '3:00 PM', 90, 3.0);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Analyze festival submissions in Lone_Star_Film_Fest.Submissions.
 *  
 *  1. Bronze: Raw Views of FILMMAKERS, FILMS, SCREENINGS.
 *  2. Silver: 
 *     - Runtime: Parse all formats to total minutes (INT). Filter negative/zero.
 *     - Genre: SPLIT GENRE_RAW on '/' and ',' to create normalized genre tags.
 *     - Language: Map 'en'/'ENG' -> 'English', 'es' -> 'Spanish', 'de' -> 'German', 'jp' -> 'Japanese'.
 *     - Budget: Strip symbols, convert K->*1000, M->*1000000, cast to DOUBLE in USD.
 *     - Deduplicate: Flag resubmissions (same title/MAKER_ID across years).
 *  3. Gold: 
 *     - Average Audience Rating per Genre.
 *     - Top Films by total attendance across screenings.
 *  
 *  Show the SQL."
 */
