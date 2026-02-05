/*
 * Dremio Quantum Qubit Calibration Example
 * 
 * Domain: Deep Tech & Quantum Physics
 * Scenario: 
 * A Quantum Processor Unit (QPU) needs daily calibration.
 * We track "T1 Time" (Relaxation), "T2 Time" (Dephasing), and "Gate Fidelity" (Error rates).
 * If coherence times drop below a threshold, the qubit is "masked" from the circuit.
 * 
 * Complexity: Medium (Scientific notation, complex thresholds)
 * 
 * Instructions:
 * 1. Run the DDL steps to create the folder structure and source tables.
 * 2. Run the INSERT statements to seed the data (50+ records).
 * 3. Run the View creation steps to build the Medallion Architecture.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS Quantum_Lab;
CREATE FOLDER IF NOT EXISTS Quantum_Lab.Sources;
CREATE FOLDER IF NOT EXISTS Quantum_Lab.Bronze;
CREATE FOLDER IF NOT EXISTS Quantum_Lab.Silver;
CREATE FOLDER IF NOT EXISTS Quantum_Lab.Gold;

-------------------------------------------------------------------------------
-- 2. DDL: Create Source Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS Quantum_Lab.Sources.QPU_Topology (
    QubitID VARCHAR, -- 'Q0', 'Q1'
    Chip_Version VARCHAR, -- 'Hummingbird_V2'
    Physical_X_Coord DOUBLE,
    Physical_Y_Coord DOUBLE
);

CREATE TABLE IF NOT EXISTS Quantum_Lab.Sources.Calibration_Logs (
    CalID VARCHAR,
    QubitID VARCHAR,
    Cal_Timestamp TIMESTAMP,
    T1_Microseconds DOUBLE, -- Energy relaxation
    T2_Microseconds DOUBLE, -- Dephasing
    Readout_Error_Rate DOUBLE, -- 0.0 to 1.0 (e.g., 0.01)
    Gate_Fidelity_Percent DOUBLE -- 99.9%
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (50+ Records)
-------------------------------------------------------------------------------

-- Seed Qubits (5-Qubit Chip)
INSERT INTO Quantum_Lab.Sources.QPU_Topology VALUES
('Q0', 'Hummingbird_V2', 0.0, 0.0),
('Q1', 'Hummingbird_V2', 1.0, 0.0),
('Q2', 'Hummingbird_V2', 2.0, 0.0),
('Q3', 'Hummingbird_V2', 1.0, 1.0),
('Q4', 'Hummingbird_V2', 1.0, -1.0);

-- Seed Calibrations
-- Q0 is great. Q3 is noisy.
INSERT INTO Quantum_Lab.Sources.Calibration_Logs VALUES
('CAL-001', 'Q0', '2024-01-01 08:00:00', 95.5, 105.2, 0.005, 99.95),
('CAL-002', 'Q1', '2024-01-01 08:00:00', 80.0, 90.0, 0.01, 99.8),
('CAL-003', 'Q2', '2024-01-01 08:00:00', 85.0, 95.0, 0.008, 99.9),
('CAL-004', 'Q3', '2024-01-01 08:00:00', 30.0, 20.0, 0.05, 92.0), -- Bad
('CAL-005', 'Q4', '2024-01-01 08:00:00', 92.0, 100.0, 0.006, 99.92),
-- Next cycle
('CAL-006', 'Q0', '2024-01-02 08:00:00', 94.0, 104.0, 0.005, 99.94),
('CAL-007', 'Q1', '2024-01-02 08:00:00', 79.0, 89.0, 0.011, 99.7),
('CAL-008', 'Q2', '2024-01-02 08:00:00', 84.0, 94.0, 0.009, 99.85),
('CAL-009', 'Q3', '2024-01-02 08:00:00', 25.0, 15.0, 0.06, 90.0), -- Worsening
('CAL-010', 'Q4', '2024-01-02 08:00:00', 91.0, 99.0, 0.007, 99.91),
-- Filling history
('CAL-011', 'Q0', '2023-12-25 08:00:00', 96.0, 106.0, 0.004, 99.96),
('CAL-012', 'Q3', '2023-12-25 08:00:00', 40.0, 30.0, 0.04, 94.0),
('CAL-013', 'Q0', '2023-12-26 08:00:00', 96.0, 106.0, 0.004, 99.96),
('CAL-014', 'Q3', '2023-12-26 08:00:00', 38.0, 28.0, 0.045, 93.0),
('CAL-015', 'Q0', '2023-12-27 08:00:00', 95.8, 105.8, 0.004, 99.96),
('CAL-016', 'Q3', '2023-12-27 08:00:00', 36.0, 26.0, 0.046, 92.5),
('CAL-017', 'Q0', '2023-12-28 08:00:00', 95.6, 105.6, 0.005, 99.95),
('CAL-018', 'Q3', '2023-12-28 08:00:00', 34.0, 24.0, 0.048, 92.2),
('CAL-019', 'Q0', '2023-12-29 08:00:00', 95.5, 105.4, 0.005, 99.95),
('CAL-020', 'Q3', '2023-12-29 08:00:00', 32.0, 22.0, 0.049, 92.1),
('CAL-021', 'Q0', '2023-12-30 08:00:00', 95.5, 105.3, 0.005, 99.95),
('CAL-022', 'Q3', '2023-12-30 08:00:00', 31.0, 21.0, 0.05, 92.0),
('CAL-023', 'Q0', '2023-12-31 08:00:00', 95.5, 105.2, 0.005, 99.95),
('CAL-024', 'Q3', '2023-12-31 08:00:00', 30.5, 20.5, 0.05, 92.0),
('CAL-025', 'Q1', '2023-12-31 08:00:00', 80.5, 90.5, 0.01, 99.81),
('CAL-026', 'Q2', '2023-12-31 08:00:00', 85.5, 95.5, 0.008, 99.9),
('CAL-027', 'Q4', '2023-12-31 08:00:00', 92.5, 100.5, 0.006, 99.93),
-- More bulk data to reach 50
('CAL-028', 'Q0', '2024-01-03 08:00:00', 94.0, 104.0, 0.005, 99.94),
('CAL-029', 'Q1', '2024-01-03 08:00:00', 79.0, 89.0, 0.011, 99.7),
('CAL-030', 'Q2', '2024-01-03 08:00:00', 84.0, 94.0, 0.009, 99.85),
('CAL-031', 'Q3', '2024-01-03 08:00:00', 20.0, 10.0, 0.07, 88.0), -- Failed
('CAL-032', 'Q4', '2024-01-03 08:00:00', 91.0, 99.0, 0.007, 99.91),
('CAL-033', 'Q0', '2024-01-04 08:00:00', 94.0, 104.0, 0.005, 99.94),
('CAL-034', 'Q1', '2024-01-04 08:00:00', 79.0, 89.0, 0.011, 99.7),
('CAL-035', 'Q2', '2024-01-04 08:00:00', 84.0, 94.0, 0.009, 99.85),
('CAL-036', 'Q3', '2024-01-04 08:00:00', 10.0, 5.0, 0.10, 85.0), -- Dead
('CAL-037', 'Q4', '2024-01-04 08:00:00', 91.0, 99.0, 0.007, 99.91),
('CAL-038', 'Q0', '2024-01-05 08:00:00', 93.0, 103.0, 0.006, 99.93),
('CAL-039', 'Q1', '2024-01-05 08:00:00', 78.0, 88.0, 0.012, 99.6),
('CAL-040', 'Q2', '2024-01-05 08:00:00', 83.0, 93.0, 0.010, 99.80),
('CAL-041', 'Q3', '2024-01-05 08:00:00', 5.0, 2.0, 0.20, 70.0),
('CAL-042', 'Q4', '2024-01-05 08:00:00', 90.0, 98.0, 0.008, 99.90),
('CAL-043', 'Q0', '2024-01-06 08:00:00', 93.0, 103.0, 0.006, 99.93),
('CAL-044', 'Q1', '2024-01-06 08:00:00', 78.0, 88.0, 0.012, 99.6),
('CAL-045', 'Q3', '2024-01-06 08:00:00', 0.0, 0.0, 1.0, 0.0), -- Offline
('CAL-046', 'Q4', '2024-01-06 08:00:00', 90.0, 98.0, 0.008, 99.90),
('CAL-047', 'Q0', '2024-01-07 08:00:00', 93.0, 103.0, 0.006, 99.93),
('CAL-048', 'Q1', '2024-01-07 08:00:00', 78.0, 88.0, 0.012, 99.6),
('CAL-049', 'Q2', '2024-01-07 08:00:00', 83.0, 93.0, 0.010, 99.80),
('CAL-050', 'Q4', '2024-01-07 08:00:00', 90.0, 98.0, 0.008, 99.90);

-------------------------------------------------------------------------------
-- 4. MEDALLION ARCHITECTURE VIEWS
-------------------------------------------------------------------------------

-- 4a. BRONZE LAYER
CREATE OR REPLACE VIEW Quantum_Lab.Bronze.Bronze_Topology AS SELECT * FROM Quantum_Lab.Sources.QPU_Topology;
CREATE OR REPLACE VIEW Quantum_Lab.Bronze.Bronze_Calibrations AS SELECT * FROM Quantum_Lab.Sources.Calibration_Logs;

-- 4b. SILVER LAYER (Performance Categorization)
CREATE OR REPLACE VIEW Quantum_Lab.Silver.Silver_Qubit_Health AS
SELECT
    c.CalID,
    c.QubitID,
    c.Cal_Timestamp,
    c.T1_Microseconds,
    c.Gate_Fidelity_Percent,
    -- Determine utility
    CASE 
        WHEN c.Gate_Fidelity_Percent > 99.9 AND c.T1_Microseconds > 90.0 THEN 'Premium (Quantum Volume High)'
        WHEN c.Gate_Fidelity_Percent > 99.0 AND c.T1_Microseconds > 50.0 THEN 'Standard'
        WHEN c.Gate_Fidelity_Percent < 95.0 THEN 'Noisy / Unusable'
        ELSE 'Marginal'
    END as Qubit_Grade
FROM Quantum_Lab.Bronze.Bronze_Calibrations c;

-- 4c. GOLD LAYER (Dynamic Circuit Map)
CREATE OR REPLACE VIEW Quantum_Lab.Gold.Gold_Active_Qubits AS
SELECT
    t.QubitID,
    t.Physical_X_Coord,
    t.Physical_Y_Coord,
    AVG(h.Gate_Fidelity_Percent) as Avg_Fidelity_7d,
    MIN(h.Qubit_Grade) as Worst_Grade_Recent,
    CASE 
        WHEN AVG(h.Gate_Fidelity_Percent) < 99.0 THEN 'MASKED'
        ELSE 'ACTIVE'
    END as Circuit_Status
FROM Quantum_Lab.Bronze.Bronze_Topology t
JOIN Quantum_Lab.Silver.Silver_Qubit_Health h ON t.QubitID = h.QubitID
WHERE h.Cal_Timestamp > CURRENT_DATE - 7
GROUP BY t.QubitID, t.Physical_X_Coord, t.Physical_Y_Coord;

-------------------------------------------------------------------------------
-- 5. AI PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt: 
 * "From 'Gold_Active_Qubits', list all Qubits marked as 'MASKED'. 
 * Then check 'Silver_Qubit_Health' to see if it was due to T1 relaxation time or Gate Fidelity dropping."
 */
