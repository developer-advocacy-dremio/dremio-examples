/*
 * Dremio "Messy Data" Challenge: Pet Adoption Records
 * 
 * Scenario: 
 * Animal shelter database with hand-entered intake forms.
 * 3 Tables: ANIMALS, ADOPTERS, ADOPTIONS.
 * 
 * Objective for AI Agent:
 * 1. Breed Standardization: Map variants ('Lab' / 'Labrador' / 'Labrador Retriever') to canonical name.
 * 2. Weight Parsing: Extract numeric value and convert units ('25 lbs' vs '11.3 kg').
 * 3. Age Estimation: Parse free-text DOB ('~2020', 'puppy', 'approx 3 years') into estimated birth year.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Happy_Paws;
CREATE FOLDER IF NOT EXISTS Happy_Paws.Shelter;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables (3 Tables)
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Happy_Paws.Shelter.ANIMALS (
    ANIMAL_ID VARCHAR,
    ANIMAL_NAME VARCHAR,
    SPECIES VARCHAR,
    BREED_RAW VARCHAR, -- 'Lab', 'Labrador', 'Labrador Retriever', 'lab mix'
    WEIGHT_RAW VARCHAR, -- '25 lbs', '11.3 kg', '30', NULL
    DOB_RAW VARCHAR, -- '2020-05-15', '~2020', 'puppy', 'approx 3 years'
    INTAKE_DT DATE
);

CREATE TABLE IF NOT EXISTS Happy_Paws.Shelter.ADOPTERS (
    ADOPTER_ID VARCHAR,
    ADOPTER_NAME VARCHAR,
    PHONE VARCHAR,
    CITY VARCHAR
);

CREATE TABLE IF NOT EXISTS Happy_Paws.Shelter.ADOPTIONS (
    ADOPT_ID VARCHAR,
    ANIMAL_ID VARCHAR,
    ADOPTER_ID VARCHAR,
    ADOPT_DT DATE,
    FEE_PAID DOUBLE,
    RETURNED BOOLEAN
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- ADOPTERS (10 rows)
INSERT INTO Happy_Paws.Shelter.ADOPTERS VALUES
('AD-001', 'Sarah Connor', '555-0101', 'Austin'),
('AD-002', 'James Kirk', '555-0202', 'Dallas'),
('AD-003', 'Ellen Ripley', '555-0303', 'Houston'),
('AD-004', 'Han Solo', '555-0404', 'San Antonio'),
('AD-005', 'Dana Scully', '555-0505', 'Austin'),
('AD-006', 'Fox Mulder', '555-0606', 'Dallas'),
('AD-007', 'Tony Stark', '555-0707', 'Houston'),
('AD-008', 'Natasha R', '555-0808', 'Austin'),
('AD-009', 'Bruce Banner', '555-0909', 'Dallas'),
('AD-010', 'Pepper Potts', '555-1010', 'San Antonio');

-- ANIMALS (25 rows with messy breeds, weights, DOBs)
INSERT INTO Happy_Paws.Shelter.ANIMALS VALUES
('AN-001', 'Buddy', 'Dog', 'Labrador Retriever', '65 lbs', '2020-03-15', '2023-01-10'),
('AN-002', 'Max', 'Dog', 'Lab', '70lbs', '2019-07-01', '2023-01-15'),                    -- No space
('AN-003', 'Luna', 'Dog', 'Labrador', '28 kg', '~2020', '2023-02-01'),                    -- Approx year
('AN-004', 'Bella', 'Cat', 'Domestic Shorthair', '10 lbs', '2021-01-01', '2023-02-10'),
('AN-005', 'Charlie', 'Cat', 'DSH', '4.5 kg', 'kitten', '2023-03-01'),                    -- Abbreviation + free text
('AN-006', 'Daisy', 'Cat', 'dom. shorthair', '9 lbs', 'approx 2 years', '2023-03-15'),    -- Lowercase variant
('AN-007', 'Rocky', 'Dog', 'German Shepherd', '80 lbs', '2018-11-20', '2023-04-01'),
('AN-008', 'Cooper', 'Dog', 'GSD', '35 kg', 'approx 5 years', '2023-04-10'),              -- Abbreviation
('AN-009', 'Sadie', 'Dog', 'German Shep', '75lbs', '2017-06-15', '2023-04-15'),            -- Truncated
('AN-010', 'Milo', 'Cat', 'Siamese', '12 lbs', '2020-08-01', '2023-05-01'),
('AN-011', 'Lucy', 'Cat', 'siamese mix', '5 kg', 'senior', '2023-05-10'),                  -- Mixed, free text age
('AN-012', 'Duke', 'Dog', 'Pit Bull', '55 lbs', '2019-01-01', '2023-05-15'),
('AN-013', 'Bear', 'Dog', 'Pitbull', '60 lbs', '~2019', '2023-06-01'),                    -- No space variant
('AN-014', 'Coco', 'Dog', 'pit bull terrier', '50lbs', 'approx 4 years', '2023-06-10'),   -- Full variant
('AN-015', 'Oreo', 'Cat', 'Tuxedo', '11 lbs', '2021-05-01', '2023-06-15'),
('AN-016', 'Shadow', 'Dog', 'lab mix', '45 lbs', 'puppy', '2023-07-01'),                  -- Young dog
('AN-017', 'Ginger', 'Cat', 'Orange Tabby', '14 lbs', 'adult', '2023-07-10'),
('AN-018', 'Zeus', 'Dog', 'Husky', '60 lbs', '2020-12-25', '2023-07-15'),
('AN-019', 'Nala', 'Dog', 'Siberian Husky', '55 lbs', '2020-12-25', '2023-07-20'),        -- Full breed name
('AN-020', 'Simba', 'Cat', 'Maine Coon', '18 lbs', '2019-03-01', '2023-08-01'),
('AN-021', 'Willow', 'Cat', 'maine coon mix', '8 kg', 'approx 3 years', '2023-08-10'),
('AN-022', 'Rex', 'Dog', 'Unknown', NULL, 'unknown', '2023-08-15'),                        -- All unknown
('AN-023', 'Patches', 'Cat', '', '10 lbs', '', '2023-09-01'),                              -- Empty strings
('AN-024', 'Thor', 'Dog', 'Golden Retriever', '75 lbs', '2018-04-10', '2023-09-10'),
('AN-025', 'Loki', 'Dog', 'Golden', '70 lbs', '2018-04-10', '2023-09-15');                -- Shortened breed

-- ADOPTIONS (20+ rows with returns and edge cases)
INSERT INTO Happy_Paws.Shelter.ADOPTIONS VALUES
('AP-001', 'AN-001', 'AD-001', '2023-01-20', 150.00, false),
('AP-002', 'AN-002', 'AD-002', '2023-01-25', 150.00, false),
('AP-003', 'AN-004', 'AD-003', '2023-02-15', 75.00, false),
('AP-004', 'AN-005', 'AD-004', '2023-03-10', 75.00, true),     -- Returned!
('AP-005', 'AN-005', 'AD-005', '2023-04-01', 75.00, false),    -- Re-adopted
('AP-006', 'AN-007', 'AD-006', '2023-04-10', 150.00, false),
('AP-007', 'AN-010', 'AD-007', '2023-05-05', 75.00, false),
('AP-008', 'AN-012', 'AD-008', '2023-05-20', 150.00, true),    -- Returned!
('AP-009', 'AN-012', 'AD-009', '2023-06-15', 100.00, false),   -- Re-adopted at lower fee
('AP-010', 'AN-015', 'AD-010', '2023-06-20', 75.00, false),
('AP-011', 'AN-018', 'AD-001', '2023-07-20', 150.00, false),   -- Same adopter, second pet
('AP-012', 'AN-020', 'AD-003', '2023-08-05', 75.00, false),
('AP-013', 'AN-024', 'AD-006', '2023-09-15', 150.00, false),
('AP-014', 'AN-003', 'AD-002', '2023-02-10', 150.00, false),
('AP-015', 'AN-006', 'AD-004', '2023-03-20', 75.00, false),
('AP-016', 'AN-009', 'AD-005', '2023-04-20', 150.00, false),
('AP-017', 'AN-011', 'AD-007', '2023-05-15', 50.00, false),    -- Senior discount
('AP-018', 'AN-016', 'AD-008', '2023-07-05', 200.00, false),   -- Puppy premium
('AP-019', 'AN-999', 'AD-001', '2023-10-01', 100.00, false),   -- Orphan animal ID
('AP-020', 'AN-022', 'AD-010', '2023-08-20', 0.00, false);     -- Fee waived

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Clean the shelter data in Happy_Paws.Shelter.
 *  
 *  1. Bronze: Raw Views of ANIMALS, ADOPTERS, ADOPTIONS.
 *  2. Silver: 
 *     - Standardize Breed: Map 'Lab'/'Labrador' -> 'Labrador Retriever', 'GSD'/'German Shep' -> 'German Shepherd'.
 *     - Parse Weight: Extract number from WEIGHT_RAW, IF contains 'kg' THEN value * 2.205 ELSE value. Cast to DOUBLE.
 *     - Estimate Birth Year: IF DOB_RAW matches date, parse. IF '~YYYY', extract year. IF 'puppy', assume current year.
 *  3. Gold: 
 *     - Adoption Rate by Species.
 *     - Return Rate (% of adoptions with RETURNED = true).
 *     - Average weight by canonical breed.
 *  
 *  Show the SQL."
 */
