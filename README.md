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
| **Govt (Tax)** | [government_tax_revenue_analysis.sql](sql_examples/government_tax_revenue_analysis.sql) | Tax collection efficiency, revenue projections, and delinquency analysis. |
| **Govt (Transit)** | [government_public_transit_ops.sql](sql_examples/government_public_transit_ops.sql) | Bus/Rail on-time performance, ridership trends, and delay analysis. |
| **Govt (Emergency)** | [government_emergency_response_911.sql](sql_examples/government_emergency_response_911.sql) | 911 response times, incident heatmaps, and dispatch efficiency. |
| **Govt (Benefits)** | [government_social_services_benefits.sql](sql_examples/government_social_services_benefits.sql) | SNAP/Unemployment application processing times and backlog tracking. |
| **Govt (Infrastr.)** | [government_infrastructure_maintenance.sql](sql_examples/government_infrastructure_maintenance.sql) | Road/Bridge maintenance work orders, cost tracking, and asset health. |
| **Govt (Spend)** | [government_procurement_spend.sql](sql_examples/government_procurement_spend.sql) | Government contract analysis, vendor diversity, and budget utilization. |
| **Govt (Permits)** | [government_permitting_and_licensing.sql](sql_examples/government_permitting_and_licensing.sql) | Zoning and building permit turnaround times and approval efficiency. |
| **Govt (Environment)** | [government_environmental_monitoring.sql](sql_examples/government_environmental_monitoring.sql) | Air Quality (AQI) monitoring, sensor aggregation, and compliance. |
| **Govt (Judicial)** | [government_court_case_management.sql](sql_examples/government_court_case_management.sql) | Court docket backlogs, case aging, and processing velocity. |
| **Govt (Sanitation)** | [government_sanitation_waste_mgmt.sql](sql_examples/government_sanitation_waste_mgmt.sql) | Waste collection routes, recycling diversion rates, and tonnage. |
| **Miscellaneous & Niche Industries**
  - **Airline Loyalty**: Mileage liability analysis.
  - **Biotech Research**: Lab experiment tracking.
  - **Casino**: Floor optimization and hold percentages.
  - **Cloud FinOps**: Infrastructure cost analysis.
  - **Content Moderation**: AI & human review queues.
  - **Crowdfunding**: Campaign goal tracking.
  - **Customer Support**: Ticket resolution & CSAT.
  - **Data Center**: Power usage and PUE metrics.
  - **Digital Rights (DRM)**: License compliance.
  - **EV Charging**: Station utilization.
  - **Event Ticketing**: Sales velocity & dynamic pricing.
  - **Fashion**: Trend forecasting vs sales.
  - **Food Delivery**: Rider logistics efficiency.
  - **Library**: Circulation and overdue tracking.
  - **Music Streaming**: Artist royalty calculations.
  - **Parking**: Garage occupancy optimization.
  - **Podcast**: Listener engagement analytics.
  - **Ride Sharing**: Driver earnings & surge pricing.
  - **Subscription Box**: Churn and retention analysis.
  - **Weather Retail**: Weather impact on sales.
  - **DevOps**: DORA metrics (Deployment Freq, Fail Rate).
  - **Solar Energy**: Panel efficiency & battery storage.
  - **Marketing Attribution**: Multi-touch attribution models.
  - **ISP Traffic**: Network congestion & packet loss.
  - **Recruitment**: HR hiring funnel analytics.
  - **Call Center**: Sentiment analysis & agent performance.
  - **Restaurant**: Menu engineering (Stars vs Dogs).
  - **Wastewater**: Effluent compliance monitoring.
  - **Museum**: Visitor dwell time analytics.
  - **Auction**: Bidding fraud and velocity.

### Financial Services & Risk (Phase 5)
| Scenario | File | Description |
| :--- | :--- | :--- |
| **AML Detection** | [aml_structuring_detection.sql](sql_examples/aml_structuring_detection.sql) | Detects structuring/smurfing patterns below \$10k thresholds. |
| **ATM Cash Mgmt** | [atm_cash_management.sql](sql_examples/atm_cash_management.sql) | Optimizes cash replenishment to minimize outages and idle cash. |
| **Basel III LCR** | [basel_liquidity_coverage.sql](sql_examples/basel_liquidity_coverage.sql) | Calculates Liquidity Coverage Ratio (HQLA / Outflows). |
| **BNPL Default** | [bnpl_default_analysis.sql](sql_examples/bnpl_default_analysis.sql) | Analyzes delinquency cohorts in Buy-Now-Pay-Later loans. |
| **Treasury Pooling** | [corporate_treasury_pooling.sql](sql_examples/corporate_treasury_pooling.sql) | Global multi-currency cash sweeping and positioning. |
| **Credit Scoring** | [credit_scoring_model.sql](sql_examples/credit_scoring_model.sql) | Generates FICO-like scores from treadlines and inquiries. |
| **ESG Scoring** | [esg_fund_scoring.sql](sql_examples/esg_fund_scoring.sql) | Weighted average ESG scores for investment portfolios. |
| **Forex P&L** | [forex_desk_pnl.sql](sql_examples/forex_desk_pnl.sql) | Real-time NOP and Floating P&L for FX dealing desks. |
| **Hedge Fund** | [hedge_fund_alpha.sql](sql_examples/hedge_fund_alpha.sql) | Decomposes returns into Alpha and Beta components. |
| **HFT Latency** | [hft_latency_analysis.sql](sql_examples/hft_latency_analysis.sql) | millisecond analysis of Order-to-Ack latency. |
| **M&A Deal Flow** | [ma_deal_flow.sql](sql_examples/ma_deal_flow.sql) | Tracks VDR activity to gauge buyer intent. |
| **Merchant Risk** | [merchant_chargeback_analysis.sql](sql_examples/merchant_chargeback_analysis.sql) | Monitors chargeback ratios for payment processors. |
| **Microfinance** | [microfinance_repayment.sql](sql_examples/microfinance_repayment.sql) | Group liability and attendance tracking for micro-loans. |
| **Mortgage Refi** | [mortgage_prepayment_risk.sql](sql_examples/mortgage_prepayment_risk.sql) | Calculates prepayment risk based on rate differentials. |
| **P2P Lending** | [p2p_lending_risk.sql](sql_examples/p2p_lending_risk.sql) | Default rates and ROI analysis for peer lending. |
| **Pension LDI** | [pension_fund_ldi.sql](sql_examples/pension_fund_ldi.sql) | Asset-Liability duration gap analysis. |
| **MiFID II** | [regulatory_reporting_mifid.sql](sql_examples/regulatory_reporting_mifid.sql) | Post-trade transparency and LIS deferral logic. |
| **Reinsurance** | [reinsurance_cat_modeling.sql](sql_examples/reinsurance_cat_modeling.sql) | Probable Maximum Loss (PML) for Hurricane/Quake zones. |
| **Robo Advisor** | [robo_advisor_rebalancing.sql](sql_examples/robo_advisor_rebalancing.sql) | Automated drift detection and rebalancing signals. |
| **Trade Surveillance**| [trade_surveillance_spoofing.sql](sql_examples/trade_surveillance_spoofing.sql) | Identifies spoofing via Order-to-Trade ratios. |

| **Legal** | [legal_matter_billing_analysis.sql](sql_examples/legal_matter_billing_analysis.sql) | Billable hours, realization rates, and matter profit margins. |
| **Construction** | [construction_project_budget_tracking.sql](sql_examples/construction_project_budget_tracking.sql) | Job costing, subcontractor expenses, and budget variance. |
| **Mining** | [mining_extraction_efficiency.sql](sql_examples/mining_extraction_efficiency.sql) | Ore grade analysis, haul cycle times, and metal production. |
| **Utilities** | [utilities_smart_water_metering.sql](sql_examples/utilities_smart_water_metering.sql) | Water consumption, leak detection, and pressure monitoring. |
| **Telecom (Infra)** | [telecom_5g_network_optimization.sql](sql_examples/telecom_5g_network_optimization.sql) | Cell tower latency, throughput, and hardware downtime tracking. |
| **Higher Ed** | [higher_ed_enrollment_funnel.sql](sql_examples/higher_ed_enrollment_funnel.sql) |  Admissions pipeline, yield rates, and matriculation analysis. |
| **Maritime** | [maritime_port_operations.sql](sql_examples/maritime_port_operations.sql) | Vessel dwell times, berth occupancy, and port throughput. |
| **Chemicals** | [chemical_batch_traceability.sql](sql_examples/chemical_batch_traceability.sql) | Ingredient lot tracking, lab purity results, and batch yield. |
| **Aerospace** | [aerospace_component_reliability.sql](sql_examples/aerospace_component_reliability.sql) | Part MTBF, flight hours, and unscheduled maintenance tracking. |
| **ESG** | [esg_carbon_footprint_tracking.sql](sql_examples/esg_carbon_footprint_tracking.sql) | Emissions (Scope 1-3), carbon intensity, and reduction progress. |

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
