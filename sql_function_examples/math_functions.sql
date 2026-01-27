-------------------------------------------------------------------------------
-- Dremio SQL Function Examples: Math Functions
-- 
-- This script demonstrates the usage of various mathematical functions 
-- available in Dremio SQL.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 1. SETUP: Create Context and Mock Data
-------------------------------------------------------------------------------

-- PREREQUISITE: Ensure a space named 'FunctionExamplesDB' exists, or run within your Home Space.
CREATE FOLDER IF NOT EXISTS FunctionExamplesDB;

-- Create a table for Mathematical Demonstrations
CREATE TABLE IF NOT EXISTS FunctionExamplesDB.MathStats (
    StatID INT,
    Description VARCHAR,
    Value1 DOUBLE,
    Value2 DOUBLE,
    Angle DOUBLE -- In degrees
);

INSERT INTO FunctionExamplesDB.MathStats (StatID, Description, Value1, Value2, Angle) VALUES
(1, 'Basic Positive', 10.5, 3.2, 45.0),
(2, 'Basic Negative', -10.5, -3.2, 90.0),
(3, 'Zero Values', 0.0, 5.0, 0.0),
(4, 'High Precision', 123.45678, 0.01234, 180.0),
(5, 'Rounding Test', 15.99, 15.01, 30.0);

-------------------------------------------------------------------------------
-- 2. EXAMPLES: Mathematical Functions
-------------------------------------------------------------------------------

-- 2.1 Basic Operations (ABS, SIGN, MOD)
-- ABS: Returns the absolute value.
-- SIGN: Returns -1, 0, or 1 for negative, zero, or positive numbers.
-- MOD: Returns the remainder of division (Value1 / Value2).
SELECT 
    Description,
    Value1,
    Value2,
    ABS(Value2) AS Absolute_Value2,
    SIGN(Value2) AS Sign_Value2,
    MOD(Value1, 4) AS Modulo_4
FROM FunctionExamplesDB.MathStats;

-- 2.2 Rounding and Truncation (ROUND, CEIL, FLOOR, TRUNC)
-- ROUND: Rounds to the nearest integer or specified decimal places.
-- CEIL / CEILING: Rounds up to the nearest integer.
-- FLOOR: Rounds down to the nearest integer.
-- TRUNC: Truncates to a specified number of decimal places (if supported, otherwise truncates to integer).
SELECT 
    Description,
    Value1,
    ROUND(Value1) AS Round_Default,
    ROUND(Value1, 1) AS Round_1_Decimal,
    CEIL(Value1) AS Ceiling_Val,
    FLOOR(Value1) AS Floor_Val,
    TRUNCATE(Value1, 1) AS Truncate_1_Decimal -- Function name is TRUNCATE in some contexts, or TRUNC
FROM FunctionExamplesDB.MathStats;

-- 2.3 Exponents and Roots (POWER, SQRT, CBRT, EXP, LN, LOG)
-- POWER(base, exponent): Raises base to the power of exponent.
-- SQRT: Square root.
-- CBRT: Cube root.
-- EXP: Returns e raised to the power of the argument.
-- LN: Natural logarithm.
-- LOG(base, value) or LOG10(value): Logarithms.
SELECT 
    Description,
    Value1,
    POWER(ABS(Value1), 2) AS Squared,
    SQRT(ABS(Value1)) AS SquareRoot,
    CBRT(ABS(Value1)) AS CubeRoot,
    EXP(1) AS Euler_Constant,
    LOG(10, 100) AS Log_Base_10
FROM FunctionExamplesDB.MathStats
WHERE Description = 'Basic Positive';

-- 2.4 Trigonometry (SIN, COS, TAN, RADIANS, DEGREES)
-- Dremio trig functions usually expect Radians.
-- RADIANS: Converts degrees to radians.
-- DEGREES: Converts radians to degrees.
SELECT
    Description,
    Angle AS Angle_Degrees,
    RADIANS(Angle) AS Angle_Radians,
    SIN(RADIANS(Angle)) AS Sine,
    COS(RADIANS(Angle)) AS Cosine,
    TAN(RADIANS(Angle)) AS Tangent
FROM FunctionExamplesDB.MathStats;

-- 2.5 Constants (PI)
SELECT 
    PI() AS Pi_Value,
    PI() * 2 AS Tau_Approx
FROM (VALUES(1));

-- 2.6 Random Numbers (RAND)
-- RAND: Returns a random float between 0 and 1.
SELECT 
    RAND() AS Random_1,
    RAND() AS Random_2
FROM (VALUES(1));
