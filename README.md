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

## SQL Function Examples

The `sql_function_examples/` directory contains practical usage examples for every Dremio function, categorized by type:

| Category | File | Description |
| :--- | :--- | :--- |
| **Math** | [`math_functions.sql`](sql_function_examples/math_functions.sql) | Arithmetic, rounding, trigonometry, logs, etc. |
| **String** | [`string_functions.sql`](sql_function_examples/string_functions.sql) | Concatenation, trimming, regex, casing, etc. |
| **Date/Time** | [`date_time_functions.sql`](sql_function_examples/date_time_functions.sql) | Intervals, timestamps, extraction, formatting. |
| **Aggregates** | [`aggregate_functions.sql`](sql_function_examples/aggregate_functions.sql) | `GROUP BY` functions, stats, string aggregation. |
| **Window** | [`window_functions.sql`](sql_function_examples/window_functions.sql) | Ranking, `LAG`/`LEAD`, moving averages. |
| **Conversion** | [`conversion_functions.sql`](sql_function_examples/conversion_functions.sql) | `CAST`, JSON conversion, Hex encoding. |
| **Conditional** | [`conditional_functions.sql`](sql_function_examples/conditional_functions.sql) | `COALESCE`, `NULLIF`, `CASE WHEN`. |

## AI Function Examples

The `sql_ai_function_examples/` directory demonstrates Dremio's Generative AI capabilities using `AI_GENERATE_TEXT`:

| Functionality | File | Description |
| :--- | :--- | :--- |
| **Sentiment & Tagging** | [`sentiment_classification.sql`](sql_ai_function_examples/sentiment_classification.sql) | Extracting sentiment ("Positive", "Negative") and classifying review topics. |
| **Data Transformation** | [`data_transformation.sql`](sql_ai_function_examples/data_transformation.sql) | Normalizing addresses, formatting phone numbers, and cleaning messy text. |

## SQL Admin Examples

The `sql_admin_examples/` directory contains scripts for administrative and operational tasks:

| Scenario | File | Description |
| :--- | :--- | :--- |
| **Access Control (RBAC)** | [`access_control_rbac.sql`](sql_admin_examples/access_control_rbac.sql) | User/Role lifecycle, fine-grained grants, and privilege management. |
| **Reflections** | [`reflection_management.sql`](sql_admin_examples/reflection_management.sql) | Creation and tuning of Raw and Aggregate reflections. |
| **Data Maintenance** | [`data_maintenance.sql`](sql_admin_examples/data_maintenance.sql) | Iceberg `OPTIMIZE`, `VACUUM` (snapshots/orphans), and `ROLLBACK`. |
| **Source & Engine** | [`source_engine_management.sql`](sql_admin_examples/source_engine_management.sql) | Metadata refreshes, engine routing, and session context. |

## Advanced Examples

For deeper technical deep-dives, explore these specialized collections:

| Collection | Directory | Description |
| :--- | :--- | :--- |
| **Ingestion** | [`sql_ingestion_examples/`](sql_ingestion_examples/) | `COPY INTO`, `CTAS`, `INSERT SELECT`, `MERGE INTO` (Upsert), and Incremental Loading patterns. |
| **Advanced DML** | [`sql_dml_examples/`](sql_dml_examples/) | `MERGE` for upserts and SCD Type 1/2 updates. |
| **Iceberg Metadata** | [`sql_iceberg_metadata_examples/`](sql_iceberg_metadata_examples/) | Inspecting history, snapshots, files, partitions, and manifests. |
| **System Tables** | [`sql_system_table_examples/`](sql_system_table_examples/) | Analyzing job performance, reflection health, and user activity. |

## Guides & Best Practices

- **[View & Reflection Strategies](view_reflection_best_practices.md)**: Exhaustive guide on Medallion View design, Reflection optimization, Wiki standards, and Tagging taxonomies.

## Security & compliance

- **[Masking & Row-Level Security Recipes](masking_recipes.md)**: A guide to implementing dynamic masking and RLS patterns (View-based).
- **[UDF Policy Recipes](udf_policy_recipes.md)**: A guide to implementing reusable Row Access and Column Masking Policies using UDFs.
- **[Privilege Management Recipes](privilege_management_recipes.md)**: A guide to granting privileges on Sources, Spaces, Folders, and Datasets.

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
| **Automotive** | [automotive_fleet_telemetry.sql](sql_examples/automotive_fleet_telemetry.sql) | Fleet management, fuel efficiency tracking, and maintenance logs. |
| **Aviation** | [aviation_flight_delays.sql](sql_examples/aviation_flight_delays.sql) | Flight operations, on-time performance analysis, and weather impact. |
| **Gaming** | [gaming_player_economy.sql](sql_examples/gaming_player_economy.sql) | Player economy, microtransactions tracking, and revenue analysis. |
| **Oil & Gas** | [oil_gas_pipeline_sensor.sql](sql_examples/oil_gas_pipeline_sensor.sql) | Pipeline sensor monitoring, leak detection, and pressure safety alerts. |
| **Pharma** | [pharma_clinical_trials.sql](sql_examples/pharma_clinical_trials.sql) | Clinical trial management, patient safety monitoring, and study efficacy. |
| **HR** | [hr_workforce_analytics.sql](sql_examples/hr_workforce_analytics.sql) | Workforce analytics, employee retention, and performance review aggregation. |
| **Cybersecurity** | [cybersecurity_threat_detection.sql](sql_examples/cybersecurity_threat_detection.sql) | Threat detection, login anomaly monitoring, and brute force analysis. |
| **AdTech** | [adtech_campaign_performance.sql](sql_examples/adtech_campaign_performance.sql) | Digital campaign tracking, impression-click attribution, and ROAS. |
| **Sports** | [sports_team_performance.sql](sql_examples/sports_team_performance.sql) | Team performance metrics, player efficiency stats, and game analysis. |
| **Non-Profit** | [nonprofit_donor_retention.sql](sql_examples/nonprofit_donor_retention.sql) | Donor retention strategies, campaign fundraising, and churn analysis. |
| **Retail Banking** | [retail_banking_transactions.sql](sql_examples/retail_banking_transactions.sql) | Branch performance, ATM usage, and account balance trends. |
| **Fraud Detection** | [credit_card_fraud_detection.sql](sql_examples/credit_card_fraud_detection.sql) | Velocity checks, geographic anomalies, and high-value flagging. |
| **Mortgage** | [mortgage_loan_portfolio.sql](sql_examples/mortgage_loan_portfolio.sql) | LTV ratios, delinquency analysis, and credit score monitoring. |
| **Fintech** | [fintech_payment_app.sql](sql_examples/fintech_payment_app.sql) | P2P transfer volume, cross-border flows, and user engagement. |
| **Inv. Banking** | [investment_banking_deal_flow.sql](sql_examples/investment_banking_deal_flow.sql) | M&A deal pipeline, probability weighting, and fee projections. |
| **Asset Mgmt** | [asset_management_portfolio_rebalancing.sql](sql_examples/asset_management_portfolio_rebalancing.sql) | Target vs actual weights, drift analysis, and rebalancing triggers. |
| **Compliance** | [regulatory_compliance_aml.sql](sql_examples/regulatory_compliance_aml.sql) | AML structuring detection, CTR triggers, and velocity monitoring. |
| **Crypto** | [crypto_exchange_analytics.sql](sql_examples/crypto_exchange_analytics.sql) | Trading volume, gas fee analysis, and pair liquidity. |
| **Private Equity** | [private_equity_fund_performance.sql](sql_examples/private_equity_fund_performance.sql) | MOIC calculations, valuation multiples, and fund-level returns. |
| **Forex** | [forex_trading_volume.sql](sql_examples/forex_trading_volume.sql) | Currency pair spreads, liquidity analysis, and tick volume. |
| **Healthcare (EHR)** | [healthcare_ehr_interoperability.sql](sql_examples/healthcare_ehr_interoperability.sql) | Unifying patient records (longitudinal view) from disparate systems. |
| **Healthcare (Operations)** | [healthcare_bed_capacity.sql](sql_examples/healthcare_bed_capacity.sql) | ER wait times, bed occupancy rates, and capacity forecasting. |
| **Healthcare (Supply Chain)** | [healthcare_medical_supply_chain.sql](sql_examples/healthcare_medical_supply_chain.sql) | Inventory tracking for PPE, surgical kits, and critical medications. |
| **Telemedicine** | [healthcare_telemedicine_trends.sql](sql_examples/healthcare_telemedicine_trends.sql) | Remote vs in-person adoption rates and outcome analysis. |
| **Insurance Claims** | [healthcare_insurance_claims.sql](sql_examples/healthcare_insurance_claims.sql) | Adjudication speed, denial reason analysis, and reimbursement rates. |
| **Patient Exp.** | [healthcare_patient_satisfaction.sql](sql_examples/healthcare_patient_satisfaction.sql) | HCAHPS survey analysis, NPS calculation, and sentiment tagging. |
| **Genomics** | [healthcare_genomic_analysis.sql](sql_examples/healthcare_genomic_analysis.sql) | Variant calling logs, high-impact mutation tracking, and gene expression. |
| **IoT / Wearables** | [healthcare_iot_wearables.sql](sql_examples/healthcare_iot_wearables.sql) | High-frequency vitals monitoring (Heart Rate, SpO2) and anomaly alerts. |
| **Mental Health** | [healthcare_mental_health_services.sql](sql_examples/healthcare_mental_health_services.sql) | Provider utilization, show/no-show rates, and appointment availability. |
| **Chronic Disease** | [healthcare_chronic_disease_mgmt.sql](sql_examples/healthcare_chronic_disease_mgmt.sql) | Diabetes/Hypertension tracking, care gaps, and biometric control. |

## Usage Instructions

1.  **Prerequisites**: Access to a Dremio environment (Software or Cloud).
2.  **Catalog Setup**: Should useable as is for Cloud, may need to add the catalog namespace for Software
    *   *Note: You can easily Find/Replace the catalog name in any script to match your environment.*
3.  **Run the Script**: Open a SQL Runner in Dremio, paste the content of a demo file, and execute 'Run All'.
4.  **Explore**: Navigate to the created folders (`Silver`, `Gold`) to preview the views.
5.  **Visualize**: Copy the prompts at the bottom of each file and use them in Dremio to generate instant charts.

## Architecture Pattern

-   **Bronze**: Raw ingestion tables. Data is inserted directly here.
-   **Silver**: Cleaned and joined views. This layer handles logic like `JOIN`s, `CASE` statements, and derived columns.
-   **Gold**: Aggregated business-level metrics. These views are optimized for dashboards and BI tools.
