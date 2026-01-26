/*
 * Dremio Example: Medallion Architecture with Sample Prompt
 * 
 * This script establishes 3 raw tables for an Ecommerce scenario.
 * At the bottom of this file, you will find a "Sample Prompt" that demonstrates 
 * how to use the "modeling_prompt_template.md" to generate the rest of the 
 * architecture (Bronze -> Silver -> Gold) using an LLM.
 */

-- Cleanup (Optional)
-- DROP VIEW IF EXISTS Ecommerce_Example.processed.orders_gold;
-- DROP VIEW IF EXISTS Ecommerce_Example.processed.orders_silver;
-- DROP VIEW IF EXISTS Ecommerce_Example.processed.orders_bronze;
-- DROP TABLE IF EXISTS raw_customers;
-- DROP TABLE IF EXISTS raw_products;
-- DROP TABLE IF EXISTS raw_orders;

-------------------------------------------------------------------------------
-- 1. Setup Source Tables (Raw Layer)
-------------------------------------------------------------------------------

CREATE TABLE raw_customers (customer_id INT, region VARCHAR, signup_date DATE);
INSERT INTO raw_customers VALUES
(1, 'West', '2023-11-21'),
(2, 'North', '2023-08-15'),
(3, 'North', '2023-09-24'),
(4, 'West', '2023-04-30'),
(5, 'East', '2023-02-13'),
(6, 'West', '2023-11-21'),
(7, 'East', '2023-10-10'),
(8, 'South', '2023-04-22'),
(9, 'East', '2023-06-12'),
(10, 'East', '2024-01-01'),
(11, 'East', '2023-07-16'),
(12, 'East', '2023-12-26'),
(13, 'East', '2023-12-18'),
(14, 'West', '2023-09-08'),
(15, 'South', '2023-05-01'),
(16, 'East', '2023-03-28'),
(17, 'South', '2023-06-03'),
(18, 'North', '2023-09-29'),
(19, 'West', '2023-05-31'),
(20, 'East', '2023-06-13'),
(21, 'West', '2023-07-13'),
(22, 'West', '2023-12-23'),
(23, 'East', '2023-07-22'),
(24, 'South', '2023-10-09'),
(25, 'West', '2023-01-08'),
(26, 'East', '2023-10-15'),
(27, 'North', '2023-07-19'),
(28, 'North', '2023-01-11'),
(29, 'West', '2023-12-13'),
(30, 'West', '2023-03-15'),
(31, 'West', '2023-09-05'),
(32, 'West', '2023-10-01'),
(33, 'South', '2023-06-21'),
(34, 'West', '2023-02-23'),
(35, 'North', '2023-02-18'),
(36, 'East', '2024-01-01'),
(37, 'North', '2023-07-25'),
(38, 'West', '2023-09-11'),
(39, 'West', '2023-06-15'),
(40, 'North', '2023-11-08'),
(41, 'South', '2023-07-26'),
(42, 'East', '2023-11-18'),
(43, 'South', '2023-10-13'),
(44, 'South', '2023-06-14'),
(45, 'West', '2023-10-12'),
(46, 'West', '2023-02-25'),
(47, 'South', '2023-10-11'),
(48, 'West', '2023-01-15'),
(49, 'West', '2023-05-19'),
(50, 'East', '2023-03-17'),
(51, 'North', '2023-01-25'),
(52, 'North', '2023-04-21'),
(53, 'North', '2023-02-25'),
(54, 'South', '2023-12-21'),
(55, 'South', '2023-10-12'),
(56, 'North', '2023-05-18'),
(57, 'South', '2023-08-21'),
(58, 'West', '2023-08-29'),
(59, 'East', '2023-02-17');

CREATE TABLE raw_products (product_id INT, category VARCHAR, unit_price DECIMAL(10,2));
INSERT INTO raw_products VALUES
(1, 'Home', 282.54),
(2, 'Home', 45.58),
(3, 'Toys', 296.66),
(4, 'Clothing', 177.86),
(5, 'Home', 59.5),
(6, 'Electronics', 473.42),
(7, 'Home', 146.78),
(8, 'Electronics', 44.46),
(9, 'Clothing', 34.43),
(10, 'Home', 185.77),
(11, 'Toys', 395.88),
(12, 'Home', 128.26),
(13, 'Toys', 462.07),
(14, 'Home', 82.66),
(15, 'Toys', 386.96),
(16, 'Home', 82.66),
(17, 'Clothing', 187.5),
(18, 'Toys', 221.44),
(19, 'Toys', 208.1),
(20, 'Toys', 45.54),
(21, 'Clothing', 345.15),
(22, 'Clothing', 34.16),
(23, 'Electronics', 187.98),
(24, 'Electronics', 303.59),
(25, 'Home', 305.69),
(26, 'Toys', 270.2),
(27, 'Clothing', 263.36),
(28, 'Home', 239.72),
(29, 'Clothing', 348.56),
(30, 'Clothing', 135.08),
(31, 'Clothing', 61.76),
(32, 'Home', 344.69),
(33, 'Electronics', 358.63),
(34, 'Home', 434.45),
(35, 'Toys', 77.41),
(36, 'Home', 167.76),
(37, 'Home', 327.24),
(38, 'Clothing', 27.32),
(39, 'Clothing', 468.46),
(40, 'Clothing', 107.61),
(41, 'Electronics', 342.86),
(42, 'Home', 217.15),
(43, 'Electronics', 377.14),
(44, 'Clothing', 354.49),
(45, 'Electronics', 249.35),
(46, 'Home', 181.95),
(47, 'Electronics', 317.23),
(48, 'Home', 173.5),
(49, 'Home', 247.69),
(50, 'Toys', 55.95),
(51, 'Clothing', 451.63),
(52, 'Toys', 74.2),
(53, 'Electronics', 163.86),
(54, 'Clothing', 499.24),
(55, 'Clothing', 11.49),
(56, 'Toys', 191.33),
(57, 'Clothing', 431.02),
(58, 'Toys', 137.89),
(59, 'Electronics', 208.06);

CREATE TABLE raw_orders (order_id INT, customer_id INT, product_id INT, order_date DATE, quantity INT);
INSERT INTO raw_orders VALUES
(1, 25, 36, '2024-05-10', 5),
(2, 49, 33, '2024-05-18', 5),
(3, 6, 3, '2024-03-22', 5),
(4, 36, 10, '2024-05-28', 3),
(5, 25, 47, '2024-01-17', 3),
(6, 38, 23, '2024-04-17', 3),
(7, 21, 28, '2024-03-13', 4),
(8, 39, 5, '2024-06-21', 5),
(9, 57, 9, '2024-05-11', 4),
(10, 8, 48, '2024-05-13', 3),
(11, 21, 18, '2024-05-31', 2),
(12, 16, 49, '2024-06-21', 2),
(13, 2, 53, '2024-04-24', 3),
(14, 54, 45, '2024-02-06', 2),
(15, 21, 47, '2024-03-24', 1),
(16, 7, 16, '2024-03-11', 1),
(17, 12, 17, '2024-04-17', 1),
(18, 48, 32, '2024-03-22', 1),
(19, 49, 48, '2024-01-30', 4),
(20, 6, 11, '2024-02-10', 2),
(21, 18, 26, '2024-03-24', 1),
(22, 22, 37, '2024-02-12', 3),
(23, 35, 48, '2024-06-10', 4),
(24, 26, 22, '2024-03-06', 1),
(25, 5, 32, '2024-01-23', 2),
(26, 41, 51, '2024-05-09', 2),
(27, 31, 32, '2024-04-17', 3),
(28, 7, 11, '2024-05-18', 3),
(29, 43, 16, '2024-03-23', 3),
(30, 53, 26, '2024-03-24', 3),
(31, 53, 50, '2024-05-30', 5),
(32, 39, 51, '2024-04-16', 4),
(33, 22, 24, '2024-03-05', 1),
(34, 31, 55, '2024-03-30', 3),
(35, 39, 44, '2024-03-16', 5),
(36, 55, 41, '2024-02-26', 1),
(37, 32, 3, '2024-04-21', 3),
(38, 28, 6, '2024-02-15', 3),
(39, 47, 58, '2024-04-11', 2),
(40, 53, 26, '2024-06-04', 3),
(41, 43, 26, '2024-06-03', 1),
(42, 32, 5, '2024-05-17', 4),
(43, 51, 27, '2024-03-07', 5),
(44, 28, 46, '2024-02-29', 5),
(45, 11, 11, '2024-01-08', 4),
(46, 30, 53, '2024-03-22', 4),
(47, 8, 51, '2024-03-13', 5),
(48, 41, 33, '2024-06-27', 1),
(49, 52, 34, '2024-02-13', 4),
(50, 45, 10, '2024-01-04', 2),
(51, 43, 11, '2024-04-16', 3),
(52, 27, 1, '2024-01-19', 4),
(53, 41, 13, '2024-02-11', 5),
(54, 43, 34, '2024-06-12', 2),
(55, 6, 13, '2024-03-29', 2),
(56, 50, 49, '2024-04-07', 1),
(57, 16, 40, '2024-01-30', 1),
(58, 48, 7, '2024-03-04', 5),
(59, 48, 4, '2024-06-10', 5),
(60, 54, 46, '2024-05-29', 4),
(61, 9, 11, '2024-03-13', 1),
(62, 40, 26, '2024-02-26', 1),
(63, 25, 17, '2024-05-07', 2),
(64, 44, 18, '2024-03-18', 3),
(65, 41, 43, '2024-06-03', 2),
(66, 27, 34, '2024-01-25', 2),
(67, 30, 45, '2024-04-10', 3),
(68, 44, 53, '2024-01-28', 1),
(69, 22, 52, '2024-02-12', 4),
(70, 38, 1, '2024-03-01', 5),
(71, 19, 32, '2024-04-18', 4),
(72, 41, 40, '2024-05-26', 2),
(73, 39, 40, '2024-03-25', 5),
(74, 58, 50, '2024-06-14', 2),
(75, 27, 53, '2024-03-05', 3),
(76, 46, 54, '2024-06-23', 2),
(77, 37, 26, '2024-05-18', 5),
(78, 14, 58, '2024-02-22', 4),
(79, 36, 11, '2024-05-02', 4);

/*
-------------------------------------------------------------------------------
-- 2. SAMPLE PROMPT
-------------------------------------------------------------------------------
-- Copy the block below and paste it into an LLM (like ChatGPT or Claude) 
-- to generate the rest of the architecture (Bronze, Silver, Gold views).
-- The template is available at: sample_prompts/modeling_prompt_template.md

**Role**: You are a Data Engineer expert in Dremio and the Medallion Architecture.

**Goal**: Create a Dremio SQL script that builds a Medallion architecture for the following datasets and metrics.

**Context**:
I have the following existing datasets in Dremio (Raw/Source):
1. `raw_orders` (Description: Main sales transactions with order_id, product_id, customer_id, qty, date)
2. `raw_products` (Description: Product catalog with category and unit_price)
3. `raw_customers` (Description: Customer list with region and signup date)

**Objective**:
I need to calculate the following metrics:
- Total Sales Revenue (Price * Qty)
- Sales by Region
- Top Selling Products by Category
- Monthly Revenue Trends

**Requirements**:
Generate a single SQL script that performs the following steps:
1.  **Folder Creation**: Create a folder named `Ecommerce_Example` to house the views.
2.  **Bronze Layer**: Create views in `Ecommerce_Example` for the raw datasets (direct 1:1, suffixed `_bronze`).
3.  **Silver Layer**: Create views in `Ecommerce_Example` that join Orders with Products and Customers. Clean data, calculate Line Item Total (Price * Qty). Name logic `_silver`.
4.  **Gold Layer**: Create views in `Ecommerce_Example` that aggregate the Silver data for the metrics (Revenue by Region, etc.). Name them `_gold`.

**Output Format**:
- Provide valid Dremio SQL code.
- Use `CREATE OR REPLACE VIEW` syntax.
- Add comments explaining each step.

**Specific Inputs**:
- **Target Folder**: `Ecommerce_Example`
- **Source Data Paths**:
    - `raw_orders`
    - `raw_products`
    - `raw_customers`
*/
