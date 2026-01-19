/*
 * Veterinary Clinic Patient Management Demo
 * 
 * Scenario:
 * A veterinary clinic network wants to track patient health trends, vaccination compliance,
 * and breed-specific issues across their locations.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 * 
 * Bronze Layer:
 * - Patients: Animal demographics (Name, Species, Breed, Age).
 * - Visits: Clinical notes and vital signs (Weight, Temp).
 * - Vaccinations: Vaccine records and expiry dates.
 * 
 * Silver Layer:
 * - Patient_History: Enriched view of visits with patient details.
 * - Vaccine_Compliance: Calculated status (Overdue, Due Soon) for core vaccines.
 * 
 * Gold Layer:
 * - Breed_Health_Trends: Aggregated weight/issues by breed.
 * - Clinic_Performance: Visit volume and revenue (implied) by clinic.
 * 
 * Note: Assumes a catalog named 'VetDB' exists.
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS VetDB;
CREATE FOLDER IF NOT EXISTS VetDB.Bronze;
CREATE FOLDER IF NOT EXISTS VetDB.Silver;
CREATE FOLDER IF NOT EXISTS VetDB.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Raw Data
-------------------------------------------------------------------------------

-- 1.1 Create Tables
CREATE TABLE IF NOT EXISTS VetDB.Bronze.Patients (
    PatientID INT,
    Name VARCHAR,
    Species VARCHAR, -- Dog, Cat, Rabbit, etc.
    Breed VARCHAR,
    BirthDate DATE,
    OwnerID INT
);

CREATE TABLE IF NOT EXISTS VetDB.Bronze.Visits (
    VisitID INT,
    PatientID INT,
    VisitDate DATE,
    ClinicLocation VARCHAR,
    WeightKG DOUBLE,
    Reason VARCHAR,
    Diagnosis VARCHAR
);

CREATE TABLE IF NOT EXISTS VetDB.Bronze.Vaccinations (
    VaccineID INT,
    PatientID INT,
    VaccineName VARCHAR,
    AdministeredDate DATE,
    NextDueDate DATE
);

-- 1.2 Populate Bronze Tables

-- Insert 50 records into VetDB.Bronze.Patients
INSERT INTO VetDB.Bronze.Patients (PatientID, Name, Species, Breed, BirthDate, OwnerID) VALUES
(1, 'Bella', 'Dog', 'Labrador', '2019-05-12', 101),
(2, 'Charlie', 'Dog', 'Golden Retriever', '2020-08-23', 102),
(3, 'Luna', 'Cat', 'Siamese', '2021-01-15', 103),
(4, 'Max', 'Dog', 'German Shepherd', '2018-11-30', 104),
(5, 'Lucy', 'Cat', 'Maine Coon', '2022-03-10', 105),
(6, 'Cooper', 'Dog', 'Beagle', '2019-07-04', 106),
(7, 'Daisy', 'Dog', 'Unknown', '2020-02-14', 107),
(8, 'Milo', 'Cat', 'Tabby', '2021-09-22', 108),
(9, 'Sadie', 'Dog', 'Poodle', '2017-06-18', 109),
(10, 'Lola', 'Dog', 'Bulldog', '2021-12-05', 110),
(11, 'Buddy', 'Dog', 'Boxer', '2020-04-01', 111),
(12, 'Rocky', 'Dog', 'Rottweiler', '2019-10-31', 112),
(13, 'Bailey', 'Dog', 'Dachshund', '2022-01-20', 113),
(14, 'Leo', 'Cat', 'Persian', '2020-05-05', 114),
(15, 'Coco', 'Rabbit', 'Holland Lop', '2023-01-15', 115),
(16, 'Oliver', 'Cat', 'British Shorthair', '2019-03-12', 116),
(17, 'Teddy', 'Dog', 'Shih Tzu', '2021-08-08', 117),
(18, 'Zoe', 'Dog', 'Pugh', '2022-06-30', 118),
(19, 'Chloe', 'Cat', 'Sphynx', '2020-11-11', 119),
(20, 'Penny', 'Dog', 'Corgi', '2021-04-22', 120),
(21, 'Simba', 'Cat', 'Bengal', '2022-09-09', 121),
(22, 'Jack', 'Dog', 'Jack Russell', '2018-02-28', 122),
(23, 'Lily', 'Cat', 'Ragdoll', '2021-12-25', 123),
(24, 'Zeus', 'Dog', 'Great Dane', '2019-01-01', 124),
(25, 'Ruby', 'Dog', 'Cocker Spaniel', '2020-07-07', 125),
(26, 'Nala', 'Cat', 'Abyssinian', '2022-05-14', 126),
(27, 'Toby', 'Dog', 'Australian Shepherd', '2021-03-17', 127),
(28, 'Stella', 'Dog', 'French Bulldog', '2022-08-19', 128),
(29, 'Oreo', 'Cat', 'Tuxedo', '2020-10-02', 129),
(30, 'Bear', 'Dog', 'Bernese Mountain Dog', '2019-06-21', 130),
(31, 'Willow', 'Cat', 'Russian Blue', '2021-02-14', 131),
(32, 'Duke', 'Dog', 'Doberman', '2018-09-15', 132),
(33, 'Gizmo', 'Dog', 'Pomeranian', '2022-11-11', 133),
(34, 'Jasper', 'Cat', 'Burmese', '2020-04-20', 134),
(35, 'Riley', 'Dog', 'Vizsla', '2021-07-23', 135),
(36, 'Harley', 'Dog', 'Husky', '2019-12-12', 136),
(37, 'Murphy', 'Dog', 'Goldendoodle', '2022-02-22', 137),
(38, 'Sasha', 'Dog', 'Malamute', '2020-01-30', 138),
(39, 'Loki', 'Cat', 'Scottish Fold', '2021-05-18', 139),
(40, 'Koda', 'Dog', 'Shiba Inu', '2022-10-10', 140),
(41, 'Buster', 'Dog', 'Staffordshire', '2019-08-08', 141),
(42, 'Missy', 'Cat', 'Himalayan', '2020-12-01', 142),
(43, 'Gus', 'Dog', 'Basset Hound', '2021-06-06', 143),
(44, 'Minnie', 'Dog', 'Chihuahua', '2018-05-05', 144),
(45, 'Scout', 'Dog', 'Border Collie', '2022-09-19', 145),
(46, 'Pepper', 'Cat', 'Tortoiseshell', '2021-01-20', 146),
(47, 'Moose', 'Dog', 'Newfoundland', '2020-03-03', 147),
(48, 'Shadow', 'Dog', 'Black Lab', '2019-11-20', 148),
(49, 'Fiona', 'Dog', 'English Bulldog', '2022-04-14', 149),
(50, 'Oscar', 'Cat', 'Orange Tabby', '2021-10-31', 150);

-- Insert 50 records into VetDB.Bronze.Visits
INSERT INTO VetDB.Bronze.Visits (VisitID, PatientID, VisitDate, ClinicLocation, WeightKG, Reason, Diagnosis) VALUES
(1, 1, '2024-01-10', 'North', 30.5, 'Annual Checkup', 'Healthy'),
(2, 2, '2024-01-15', 'South', 32.0, 'Limping', 'Sprain'),
(3, 3, '2024-02-01', 'North', 4.5, 'Vomiting', 'Gastritis'),
(4, 4, '2024-02-10', 'West', 35.0, 'Annual Checkup', 'Healthy'),
(5, 5, '2024-03-05', 'East', 6.0, 'Checkup', 'Overweight'),
(6, 6, '2024-03-12', 'North', 12.0, 'Ear Infection', 'Otitis Externa'),
(7, 7, '2024-03-20', 'South', 20.0, 'Vaccination', 'Healthy'),
(8, 8, '2024-04-01', 'West', 5.0, 'Checkup', 'Healthy'),
(9, 9, '2024-04-15', 'East', 22.0, 'Skin Rash', 'Allergies'),
(10, 10, '2024-05-05', 'North', 24.0, 'Breathing Issue', 'Brachycephalic Syndrome'),
(11, 11, '2024-05-10', 'South', 28.0, 'Lump', 'Lipoma'),
(12, 12, '2024-05-20', 'West', 40.0, 'Annual Checkup', 'Healthy'),
(13, 13, '2024-06-01', 'East', 8.0, 'Back Pain', 'IVDD Monitor'),
(14, 14, '2024-06-15', 'North', 4.2, 'Eye Discharge', 'Conjunctivitis'),
(15, 15, '2024-06-20', 'South', 2.0, 'Checkup', 'Healthy'),
(16, 16, '2024-07-01', 'West', 6.5, 'Dental', 'Gingivitis'),
(17, 17, '2024-07-10', 'East', 6.0, 'Cough', 'Kennel Cough'),
(18, 18, '2024-07-20', 'North', 9.0, 'Vaccination', 'Healthy'),
(19, 19, '2024-08-01', 'South', 3.5, 'Skin Check', 'Sunburn'),
(20, 20, '2024-08-15', 'West', 12.5, 'Limping', 'Hip Dysplasia'),
(21, 21, '2024-08-20', 'East', 5.5, 'Checkup', 'Healthy'),
(22, 22, '2024-09-01', 'North', 7.0, 'Energy Loss', 'Thyroid Check'),
(23, 23, '2024-09-10', 'South', 5.8, 'Matting', 'Grooming Req'),
(24, 24, '2024-09-20', 'West', 60.0, 'Annual Checkup', 'Healthy'),
(25, 25, '2024-10-01', 'East', 14.0, 'Ear Infection', 'Otitis'),
(26, 26, '2024-10-05', 'North', 3.8, 'Checkup', 'Healthy'),
(27, 27, '2024-10-15', 'South', 22.0, 'Injury', 'Cut Paw'),
(28, 28, '2024-10-20', 'West', 11.0, 'Breathing', 'Allergies'),
(29, 29, '2024-11-01', 'East', 4.8, 'Vomiting', 'Hairball'),
(30, 30, '2024-11-10', 'North', 45.0, 'Joint Pain', 'Arthritis'),
(31, 31, '2024-11-15', 'South', 4.0, 'Checkup', 'Healthy'),
(32, 32, '2024-11-20', 'West', 34.0, 'Weight Loss', 'Diabetes Test'),
(33, 33, '2024-12-01', 'East', 3.0, 'Dental', 'Cleaning'),
(34, 34, '2024-12-05', 'North', 4.5, 'Checkup', 'Healthy'),
(35, 35, '2024-12-10', 'South', 20.0, 'Runny Nose', 'Cold'),
(36, 36, '2024-12-15', 'West', 25.0, 'Howling', 'Behavioral'),
(37, 37, '2024-12-20', 'East', 28.0, 'Matting', 'Grooming'),
(38, 38, '2025-01-05', 'North', 38.0, 'Checkup', 'Healthy'),
(39, 39, '2025-01-10', 'South', 4.2, 'Ear Mites', 'Mites'),
(40, 40, '2025-01-15', 'West', 10.0, 'Itching', 'Flea Allergy'),
(41, 41, '2025-02-01', 'East', 24.0, 'Aggression', 'Behavioral'),
(42, 42, '2025-02-05', 'North', 3.9, 'Checkup', 'Healthy'),
(43, 43, '2025-02-10', 'South', 26.0, 'Ears', 'Infection'),
(44, 44, '2025-02-15', 'West', 2.5, 'Shaking', 'Anxiety'),
(45, 45, '2025-03-01', 'East', 18.0, 'Leg Injury', 'Fracture'),
(46, 46, '2025-03-05', 'North', 4.0, 'Checkup', 'Healthy'),
(47, 47, '2025-03-10', 'South', 65.0, 'Drooling', 'Dental'),
(48, 48, '2025-03-15', 'West', 31.0, 'Weight check', 'Healthy'),
(49, 49, '2025-03-20', 'East', 23.0, 'Snoring', 'Palate Issue'),
(50, 50, '2025-04-01', 'North', 5.5, 'Checkup', 'Healthy');

-- Insert 50 records into VetDB.Bronze.Vaccinations
INSERT INTO VetDB.Bronze.Vaccinations (VaccineID, PatientID, VaccineName, AdministeredDate, NextDueDate) VALUES
(1, 1, 'Rabies', '2024-01-10', '2025-01-10'),
(2, 1, 'Distemper', '2024-01-10', '2025-01-10'),
(3, 2, 'Rabies', '2024-01-15', '2025-01-15'),
(4, 3, 'FVRCP', '2024-02-01', '2025-02-01'),
(5, 4, 'Bordetella', '2024-02-10', '2024-08-10'),
(6, 5, 'Rabies', '2024-03-05', '2025-03-05'),
(7, 6, 'Rabies', '2024-03-12', '2027-03-12'), -- 3 year
(8, 7, 'Parvo', '2024-03-20', '2025-03-20'),
(9, 8, 'Leukemia', '2024-04-01', '2025-04-01'),
(10, 9, 'Rabies', '2024-04-15', '2025-04-15'),
(11, 10, 'Distemper', '2024-05-05', '2025-05-05'),
(12, 11, 'Rabies', '2024-05-10', '2025-05-10'),
(13, 12, 'Lyme', '2024-05-20', '2025-05-20'),
(14, 13, 'Rabies', '2024-06-01', '2025-06-01'),
(15, 14, 'FVRCP', '2024-06-15', '2025-06-15'),
(16, 16, 'Rabies', '2024-07-01', '2025-07-01'),
(17, 17, 'Bordetella', '2024-07-10', '2025-01-10'),
(18, 18, 'Rabies', '2024-07-20', '2025-07-20'),
(19, 19, 'FVRCP', '2024-08-01', '2025-08-01'),
(20, 20, 'Distemper', '2024-08-15', '2025-08-15'),
(21, 21, 'Rabies', '2024-08-20', '2025-08-20'),
(22, 22, 'Rabies', '2024-09-01', '2025-09-01'),
(23, 23, 'FVRCP', '2024-09-10', '2025-09-10'),
(24, 24, 'Rabies', '2024-09-20', '2025-09-20'),
(25, 25, 'Lepto', '2024-10-01', '2025-10-01'),
(26, 26, 'Rabies', '2024-10-05', '2025-10-05'),
(27, 27, 'Rabies', '2024-10-15', '2025-10-15'),
(28, 28, 'Bordetella', '2024-10-20', '2025-04-20'),
(29, 29, 'FVRCP', '2024-11-01', '2025-11-01'),
(30, 30, 'Rabies', '2024-11-10', '2025-11-10'),
(31, 31, 'FVRCP', '2024-11-15', '2025-11-15'),
(32, 32, 'Rabies', '2024-11-20', '2025-11-20'),
(33, 33, 'Rabies', '2024-12-01', '2025-12-01'),
(34, 34, 'FVRCP', '2024-12-05', '2025-12-05'),
(35, 35, 'Distemper', '2024-12-10', '2025-12-10'),
(36, 36, 'Rabies', '2024-12-15', '2025-12-15'),
(37, 37, 'Bordetella', '2024-12-20', '2025-06-20'),
(38, 38, 'Rabies', '2025-01-05', '2026-01-05'),
(39, 39, 'FVRCP', '2025-01-10', '2026-01-10'),
(40, 40, 'Rabies', '2025-01-15', '2026-01-15'),
(41, 41, 'Distemper', '2025-02-01', '2026-02-01'),
(42, 42, 'Rabies', '2025-02-05', '2026-02-05'),
(43, 43, 'Rabies', '2025-02-10', '2026-02-10'),
(44, 44, 'Parvo', '2025-02-15', '2026-02-15'),
(45, 45, 'Rabies', '2025-03-01', '2026-03-01'),
(46, 46, 'FVRCP', '2025-03-05', '2026-03-05'),
(47, 47, 'Rabies', '2025-03-10', '2026-03-10'),
(48, 48, 'Lyme', '2025-03-15', '2026-03-15'),
(49, 49, 'Rabies', '2025-03-20', '2026-03-20'),
(50, 50, 'FVRCP', '2025-04-01', '2026-04-01');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Patient History & Compliance
-------------------------------------------------------------------------------

-- 2.1 Patient History Enriched
CREATE OR REPLACE VIEW VetDB.Silver.Patient_History AS
SELECT
    v.VisitID,
    p.PatientID,
    p.Name,
    p.Species,
    p.Breed,
    v.VisitDate,
    v.ClinicLocation,
    v.Reason,
    v.Diagnosis,
    v.WeightKG
FROM VetDB.Bronze.Visits v
JOIN VetDB.Bronze.Patients p ON v.PatientID = p.PatientID;

-- 2.2 Vaccine Compliance
CREATE OR REPLACE VIEW VetDB.Silver.Vaccine_Compliance AS
SELECT
    v.PatientID,
    p.Name,
    p.OwnerID,
    v.VaccineName,
    v.NextDueDate,
    CASE
        WHEN v.NextDueDate < CURRENT_DATE THEN 'Overdue'
        WHEN v.NextDueDate < DATE_ADD(CURRENT_DATE, 30) THEN 'Due Soon'
        ELSE 'Compliant'
    END AS Status
FROM VetDB.Bronze.Vaccinations v
JOIN VetDB.Bronze.Patients p ON v.PatientID = p.PatientID;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: Health Insights
-------------------------------------------------------------------------------

-- 3.1 Breed Health Trends
CREATE OR REPLACE VIEW VetDB.Gold.Breed_Health_Trends AS
SELECT
    Breed,
    Species,
    COUNT(DISTINCT PatientID) AS PatientCount,
    AVG(WeightKG) AS AvgWeight,
    -- Simple text aggregation or most common diagnosis would be complex in SQL,
    -- so we stick to counts.
    COUNT(VisitID) AS TotalVisits
FROM VetDB.Silver.Patient_History
GROUP BY Breed, Species;

-- 3.2 Clinic Performance
CREATE OR REPLACE VIEW VetDB.Gold.Clinic_Performance AS
SELECT
    ClinicLocation,
    COUNT(DISTINCT VisitID) AS Visits,
    COUNT(DISTINCT PatientID) AS UniquePatients,
    -- Assuming a base cost for demo revenue
    COUNT(DISTINCT VisitID) * 75.0 AS EstimatedRevenue
FROM VetDB.Silver.Patient_History
GROUP BY ClinicLocation;

-------------------------------------------------------------------------------
-- 4. DREMIO AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1 (Recall List):
"List all owners (OwnerID) and patient names from VetDB.Silver.Vaccine_Compliance where the Status is 'Overdue'."

PROMPT 2 (Breed Analysis):
"Which Dog breed has the highest average weight in VetDB.Gold.Breed_Health_Trends?"

PROMPT 3 (Clinic Stats):
"Show me the EstimatedRevenue for the 'North' clinic from VetDB.Gold.Clinic_Performance."
*/
