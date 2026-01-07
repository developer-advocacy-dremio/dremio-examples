# Dremio Industry SQL Examples

This repository contains a collection of SQL demo scripts designed to showcase **Dremio's Lakehouse capabilities** across various industry verticals. Each example implements a **Medallion Architecture** (Bronze -> Silver -> Gold) to demonstrate data curation, enrichment, and analysis.

## Overview

Each script is self-contained and performs the following:
1.  **Schema Setup**: Creates folders and tables within a specific Dremio Catalog (Source).
2.  **Data Generation**: Inserts simulated, realistic data into the **Bronze** (Raw) layer.
3.  **Data Modeling**:
    *   **Silver Layer**: Cleanses and enriches raw data (e.g., joins, calculations).
    *   **Gold Layer**: Aggregates data for business insights and reporting.
4.  **Dremio Agent Prompts**: Includes natural language prompts to test Dremio's Text-to-SQL capabilities.

## Industry Demos

| Industry | File | Description |
| :--- | :--- | :--- |
| **Capital Markets** | [capital_markets_risk_compliance.sql](sql_examples/capital_markets_risk_compliance.sql) | Risk exposure, counterparty limits, and compliance monitoring. |
| **Wealth Management** | [wealth_management_analytics.sql](sql_examples/wealth_management_analytics.sql) | Client 360, advisor performance metrics, and churn risk analysis. |
| **Supply Chain** | [supply_chain_logistics.sql](sql_examples/supply_chain_logistics.sql) | Inventory turnover, supplier reliability, and shipment delays. |
| **Retail** | [retail_customer_analytics.sql](sql_examples/retail_customer_analytics.sql) | Store performance, customer RFM segmentation, and sales insights. |
| **Healthcare** | [healthcare_readmission.sql](sql_examples/healthcare_readmission.sql) | Patient readmission rates, diagnoses trends, and cost analysis. |
| **Insurance** | [insurance_risk_analysis.sql](sql_examples/insurance_risk_analysis.sql) | Loss ratios by region, policy profitability, and claims monitoring. |
| **Manufacturing** | [manufacturing_predictive_maintenance.sql](sql_examples/manufacturing_predictive_maintenance.sql) | Predictive maintenance, sensor anomaly detection, and machine health. |
| **Energy & Utilities** | [energy_smart_grid_consumption.sql](sql_examples/energy_smart_grid_consumption.sql) | Smart meter analytics, peak load management, and revenue leakage. |
| **Telecommunications** | [telecom_customer_churn.sql](sql_examples/telecom_customer_churn.sql) | Churn prediction, call drop analysis, and customer retention. |
| **E-commerce** | [ecommerce_clickstream_analytics.sql](sql_examples/ecommerce_clickstream_analytics.sql) | Clickstream funnel analysis, conversion rates, and cart abandonment. |
| **Education** | [education_student_performance.sql](sql_examples/education_student_performance.sql) | Student performance tracking, attendance monitoring, and risk intervention. |
| **Public Sector** | [smart_city_traffic_analysis.sql](sql_examples/smart_city_traffic_analysis.sql) | Smart city traffic flow, congestion monitoring, and incident response. |
| **Real Estate** | [real_estate_market_trends.sql](sql_examples/real_estate_market_trends.sql) | Market valuation, sales velocity, and regional price trends. |
| **Hospitality** | [hospitality_revenue_management.sql](sql_examples/hospitality_revenue_management.sql) | Revenue management (RevPAR), occupancy rates, and guest satisfaction. |
| **Media** | [media_streaming_content_performance.sql](sql_examples/media_streaming_content_performance.sql) | Content performance, viewer engagement, and genre popularity. |
| **Agriculture** | [agriculture_crop_yield_optimization.sql](sql_examples/agriculture_crop_yield_optimization.sql) | Precision agriculture, crop yield optimization, and resource efficiency. |

## Usage Instructions

1.  **Prerequisites**: Access to a Dremio environment (Software or Cloud).
2.  **Catalog Setup**: Ensure you have a source/catalog named according to the demo (or use `Nessie` or `CapitalMarket` as generic placeholders). The scripts assume specific top-level folders (e.g., `CapitalMarket`, `RetailDB`).
    *   *Note: You can easily Find/Replace the catalog name in any script to match your environment.*
3.  **Run the Script**: Open a SQL Runner in Dremio, paste the content of a demo file, and execute 'Run All'.
4.  **Explore**: Navigate to the created folders (`Silver`, `Gold`) to preview the views.
5.  **Visualize**: Copy the prompts at the bottom of each file and use them in Dremio to generate instant charts.

## Architecture Pattern

-   **Bronze**: Raw ingestion tables. Data is inserted directly here.
-   **Silver**: Cleaned and joined views. This layer handles logic like `JOIN`s, `CASE` statements, and derived columns.
-   **Gold**: Aggregated business-level metrics. These views are optimized for dashboards and BI tools.
