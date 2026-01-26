# Dremio Lakehouse Examples & Usage Guide

**Welcome to the ultimate Dremio Lakehouse resource.**

This repository contains over **140+ industry-specific SQL scenarios**, tailored to demonstrate the power of Dremio's Medallion Architecture (Bronze -> Silver -> Gold). It also serves as a comprehensive reference for Dremio's integrations with the broader data ecosystem.

---

## 🚀 Getting Started

1.  **Select a Scenario**: Browse the [Industry Index](#-industry-solutions-index) below to find a relevant use case.
2.  **Run the Script**: Copy the SQL content into Dremio's SQL Runner and execute.
3.  **Explore Data**: The scripts automatically create specific Folders/Spaces (e.g., `RetailDB`, `FinanceDB`) containing Bronze (Raw), Silver (Clean), and Gold (Aggregated) datasets.
4.  **Visualize**: Use the pre-written **Agent Prompts** at the bottom of each file to test Text-to-SQL or build instant dashboards.

---

## 📖 Table of Contents

- [🏛️ Architecture & Best Practices](#%EF%B8%8F-architecture--best-practices)
- [🏦 Financial Services](#-financial-services)
- [🏥 Healthcare & Life Sciences](#-healthcare--life-sciences)
- [🏛️ Government & Public Sector](#%EF%B8%8F-government--public-sector)
- [🛍️ Retail, E-Commerce & Logistics](#%EF%B8%8F-retail-e-commerce--logistics)
- [📡 Technology, Media & Telecom](#-technology-media--telecom)
- [🏭 Industrial, Energy & Utilities](#-industrial-energy--utilities)
- [🎓 Other Industries](#-other-industries)
- [🛠️ Technical Demos (Admin, AI, Functions)](#%EF%B8%8F-technical-demos)
- [🔌 Connectivity (Iceberg Catalog & Python)](#-connectivity--integrations)

---

## 🏛️ Architecture & Best Practices

All examples follow the **Medallion Architecture**:
-   **🥉 Bronze**: Raw ingestion tables. (Seeded with 50+ records for realism).
-   **🥈 Silver**: Cleaned, verified views. Handles JOINs, type casting, and derived logic.
-   **🥇 Gold**: Business-level aggregates. Optimized for BI dashboards and reporting.

**Key Guides:**
-   **[View & Reflection Strategy](view_reflection_best_practices.md)**: How to layer views and accelerate queries.
-   **[Masking & Security](masking_recipes.md)**: Implementing Row-Level Security (RLS) and Column Masking.
-   **[Privilege Management](privilege_management_recipes.md)**: Managing access control via RBAC.

---

## 🏦 Financial Services

### Retail & Commercial Banking
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Retail Banking Transactions** | [retail_banking_transactions.sql](sql_examples/retail_banking_transactions.sql) | Branch performance and account trends. |
| **Customer Churn** | [retail_banking_churn.sql](sql_examples/retail_banking_churn.sql) | Activity-based churn prediction models. |
| **ATM Cash Mgmt** | [atm_cash_management.sql](sql_examples/atm_cash_management.sql) | Cash replenishment optimization. |
| **Mortgage Portfolio** | [mortgage_loan_portfolio.sql](sql_examples/mortgage_loan_portfolio.sql) | LTV ratios and delinquency tracking. |
| **Mortgage Applications** | [mortgage_application_pipeline.sql](sql_examples/mortgage_application_pipeline.sql) | Underwriting funnel conversation analysis. |
| **Mortgage Refinance** | [mortgage_prepayment_risk.sql](sql_examples/mortgage_prepayment_risk.sql) | Prepayment risk based on rate spreads. |
| **BNPL Analysis** | [bnpl_default_analysis.sql](sql_examples/bnpl_default_analysis.sql) | Buy-Now-Pay-Later cohort delinquency. |
| **P2P Lending** | [p2p_lending_risk.sql](sql_examples/p2p_lending_risk.sql) | Default rates and investor ROI. |
| **Microfinance** | [microfinance_repayment.sql](sql_examples/microfinance_repayment.sql) | Group liability loan tracking. |
| **Credit Scoring** | [credit_scoring_model.sql](sql_examples/credit_scoring_model.sql) | FICO-like scoring from alternative data. |
| **HNW Onboarding** | [hnw_client_onboarding.sql](sql_examples/hnw_client_onboarding.sql) | VIP Client onboarding SLA tracking. |
| **Branch Staffing** | [branch_staffing_optimization.sql](sql_examples/branch_staffing_optimization.sql) | Aligning staff with footfall traffic. |
| **Mobile Payments** | [fintech_payment_app.sql](sql_examples/fintech_payment_app.sql) | P2P transfer volume and user engagement. |

### Capital Markets & Trading
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Trade Efficiency** | [trade_efficiency_analysis.sql](sql_examples/trade_efficiency_analysis.sql) | Straight-Through-Processing (STP) metrics. |
| **Market Data Quality** | [market_data_feed_quality.sql](sql_examples/market_data_feed_quality.sql) | Detecting gaps and staleness in tick feeds. |
| **HFT Latency** | [hft_latency_analysis.sql](sql_examples/hft_latency_analysis.sql) | Order-to-Ack latency analysis (microseconds). |
| **Trade Surveillance** | [trade_surveillance_spoofing.sql](sql_examples/trade_surveillance_spoofing.sql) | Detecting spoofing/layering patterns. |
| **Forex P&L** | [forex_desk_pnl.sql](sql_examples/forex_desk_pnl.sql) | Real-time Net Open Position (NOP) monitoring. |
| **Forex Volume** | [forex_trading_volume.sql](sql_examples/forex_trading_volume.sql) | Liquidity analysis by currency pair. |
| **CDS Exposure** | [credit_default_swap_exposure.sql](sql_examples/credit_default_swap_exposure.sql) | Credit Default Swap net notional risk. |
| **Bond Laddering** | [bond_laddering.sql](sql_examples/bond_laddering.sql) | Fixed income maturity profiles and cash flow. |
| **ABS Tranches** | [abs_tranche_performance.sql](sql_examples/abs_tranche_performance.sql) | Asset-Backed Security underlying collateral check. |
| **Syndicated Loans** | [syndicated_loan_admin.sql](sql_examples/syndicated_loan_admin.sql) | Complex loan administration and splits. |
| **Crypto Trading** | [crypto_exchange_analytics.sql](sql_examples/crypto_exchange_analytics.sql) | Gas fees, liquidity, and volume metrics. |
| **Crypto Custody** | [crypto_custody_reconciliation.sql](sql_examples/crypto_custody_reconciliation.sql) | On-chain vs Off-chain balance recon. |

### Financial Services - Advanced Scenarios (Phase 6)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Corporate Card Abuse** | Detecting expense fraud like weekend spending or blocked MCCs. | `MCC_Codes`, `Weekend_Flag` | [corporate_card_abuse.sql](sql_examples/corporate_card_abuse.sql) |
| **SWIFT Payment Tracking** | Monitoring international wire transfers for delays (>24h). | `MT103`, `AckNackLogs` | [swift_payment_tracking.sql](sql_examples/swift_payment_tracking.sql) |
| **ABS Tranche Performance** | Tracking delinquency rates in asset-backed security pools. | `DelinquencyRate`, `TrancheTriggers` | [abs_tranche_performance.sql](sql_examples/abs_tranche_performance.sql) |
| **VC Deal Sourcing** | Analyzing the venture capital deal funnel by sector. | `FunnelConversion`, `SectorHeatmap` | [venture_capital_deal_sourcing.sql](sql_examples/venture_capital_deal_sourcing.sql) |
| **Nostro/Vostro Reconciliation** | Matching internal ledgers against external SWIFT statements. | `ReconBreaks`, `AgingAnalysis` | [nostro_vostro_reconciliation.sql](sql_examples/nostro_vostro_reconciliation.sql) |

### Healthcare & Life Sciences - Expanded (Phase 7)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Opioid Monitoring** | Tracking MME (Morphine Milligram Equivalents) to flag high-risk prescriptions. | `MME_Calc`, `RiskAlerts` | [healthcare_opioid_monitoring.sql](sql_examples/healthcare_opioid_monitoring.sql) |
| **Sepsis Early Warning** | Real-time vitals monitoring against SIRS criteria for early detection. | `SIRS_Logic`, `VitalsStream` | [healthcare_sepsis_early_warning.sql](sql_examples/healthcare_sepsis_early_warning.sql) |
| **Radiology Workflow** | Monitoring turnaround times (TAT) for STAT vs Routine exams. | `TAT_Calc`, `SLA_Breach` | [healthcare_radiology_workflow.sql](sql_examples/healthcare_radiology_workflow.sql) |
| **Oncology Pathways** | Tracking adherence to NCCN chemotherapy guidelines vs variances. | `PathwayCompliance`, `VarianceAnalysis` | [healthcare_oncology_pathways.sql](sql_examples/healthcare_oncology_pathways.sql) |
| **EMS Dispatch** | Analyzing Chute Time and Travel Time to optimize ambulance coverage. | `ResponseMetrics`, `GeoHeatmap` | [healthcare_ems_dispatch.sql](sql_examples/healthcare_ems_dispatch.sql) |
| **HAI Tracking** | Monitoring Hospital Acquired Infections (CLABSI/CAUTI) vs POA. | `InfectionRate`, `DeviceDays` | [healthcare_hai_tracking.sql](sql_examples/healthcare_hai_tracking.sql) |
| **Device Maintenance** | Tracking preventive maintenance schedules for critical assets (MRI, Vents). | `NextDueDate`, `RiskFlag` | [healthcare_device_maintenance.sql](sql_examples/healthcare_device_maintenance.sql) |
| **Patient No-Show** | Predicting appointment no-shows based on historical patient behavior. | `NoShowRate`, `RiskPrediction` | [healthcare_patient_no_show.sql](sql_examples/healthcare_patient_no_show.sql) |
| **OR Utilization** | maximizing Operating Room block usage efficiency. | `UtilizationPct`, `BlockSchedule` | [healthcare_or_utilization.sql](sql_examples/healthcare_or_utilization.sql) |
| **Blood Bank Inventory** | Managing blood product supply and expiration risks. | `ExpiryStatus`, `ShortageAlert` | [healthcare_blood_bank_inventory.sql](sql_examples/healthcare_blood_bank_inventory.sql) |
| **Pharmacy Safety** | Reconciling Dispensed vs Administered meds to detect errors/diversion. | `VarianceDetect`, `SafetyLoop` | [healthcare_pharmacy_safety.sql](sql_examples/healthcare_pharmacy_safety.sql) |
| **Home Health Routing** | Optimizing visiting nurse schedules by geographic clusters. | `ClusterLoad`, `RouteEff` | [healthcare_home_visiting_nurse.sql](sql_examples/healthcare_home_visiting_nurse.sql) |
| **ED Diversion** | Analyzing root causes (No Beds, CT Down) of ED diversion hours. | `DiversionImpact`, `RevenueLoss` | [healthcare_ed_diversion.sql](sql_examples/healthcare_ed_diversion.sql) |
| **SDOH Analysis** | Risk stratifying patients using Social Determinants (Food/Housing). | `SocialRiskScore`, `InterventionTarget` | [healthcare_sdoh_analysis.sql](sql_examples/healthcare_sdoh_analysis.sql) |
| **Claims Appeals** | Tracking success rates of appealing insurance claim denials. | `OverturnRate`, `RecoveredRevenue` | [healthcare_claims_appeals.sql](sql_examples/healthcare_claims_appeals.sql) |
| **Provider Credentialing** | Monitoring license and DEA registration expiries for compliance. | `ExpiryAlert`, `ComplianceDash` | [healthcare_provider_credentialing.sql](sql_examples/healthcare_provider_credentialing.sql) |
| **Vaccine Cold Chain** | IoT monitoring of refrigerator temperatures to prevent vaccine spoilage. | `ExcursionDetect`, `SpoilageRisk` | [healthcare_vaccine_cold_chain.sql](sql_examples/healthcare_vaccine_cold_chain.sql) |
| **SNF Referral Leakage** | Tracking discharged patients sent to out-of-network nursing facilities. | `NetworkLeakage`, `ReferralPatterns` | [healthcare_snf_referral_leakage.sql](sql_examples/healthcare_snf_referral_leakage.sql) |
| **Nurse Staffing Acuity** | calculating HPPD (Hours Per Patient Day) tailored to patient severity. | `AcuityScore`, `StaffingVariance` | [healthcare_nurse_staffing_acuity.sql](sql_examples/healthcare_nurse_staffing_acuity.sql) |
| **ACO Shared Savings** | Tracking PMPY spend against benchmarks for Value-Based Care. | `PMPY_Calc`, `SavingsPool` | [healthcare_aco_shared_savings.sql](sql_examples/healthcare_aco_shared_savings.sql) |

### Investment & Wealth Management
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Wealth Management** | [wealth_management_analytics.sql](sql_examples/wealth_management_analytics.sql) | Advisor performance and Client 360. |
| **Robo Advisor** | [robo_advisor_rebalancing.sql](sql_examples/robo_advisor_rebalancing.sql) | Automated portfolio drift rebalancing. |
| **Asset Mgmt** | [asset_management_portfolio_rebalancing.sql](sql_examples/asset_management_portfolio_rebalancing.sql) | Target weighting and drift analysis. |
| **ESG Scoring** | [esg_fund_scoring.sql](sql_examples/esg_fund_scoring.sql) | Portfolio-level ESG compliance scoring. |
| **Hedge Fund Alpha** | [hedge_fund_alpha.sql](sql_examples/hedge_fund_alpha.sql) | Separating Alpha vs Beta returns. |
| **Private Equity** | [private_equity_fund_performance.sql](sql_examples/private_equity_fund_performance.sql) | MOIC, IRR, and valuation multiples. |
| **Venture Capital** | [venture_capital_deal_sourcing.sql](sql_examples/venture_capital_deal_sourcing.sql) | Deal flow tracking and funnel conversion. |
| **M&A Deal Flow** | [ma_deal_flow.sql](sql_examples/ma_deal_flow.sql) | Investment banking pipeline analysis. |
| **Treasury Pooling** | [corporate_treasury_pooling.sql](sql_examples/corporate_treasury_pooling.sql) | Global cash positioning and sweeping. |
| **Pension Funds** | [pension_fund_ldi.sql](sql_examples/pension_fund_ldi.sql) | Liability-Driven Investing (LDI) gaps. |

### Risk, Compliance & Operations
| Scenario | File | Description |
| :--- | :--- | :--- |
| **AML Detection** | [regulatory_compliance_aml.sql](sql_examples/regulatory_compliance_aml.sql) | Anti-Money Laundering monitoring. |
| **Structuring/Smurfing** | [aml_structuring_detection.sql](sql_examples/aml_structuring_detection.sql) | Detecting split transactions below thresholds. |
| **KYC Remediation** | [kyc_remediation.sql](sql_examples/kyc_remediation.sql) | Document expiry and compliance workflow. |
| **Fraud Detection** | [credit_card_fraud_detection.sql](sql_examples/credit_card_fraud_detection.sql) | Velocity and geographic anomaly checks. |
| **Corporate Card** | [corporate_card_abuse.sql](sql_examples/corporate_card_abuse.sql) | Expense policy violation monitoring. |
| **Merchant Risk** | [merchant_chargeback_analysis.sql](sql_examples/merchant_chargeback_analysis.sql) | Chargeback ratio monitoring. |
| **Stress Testing** | [stress_testing_ccar.sql](sql_examples/stress_testing_ccar.sql) | CCAR/DFAST economic scenario modeling. |
| **Basel III** | [basel_liquidity_coverage.sql](sql_examples/basel_liquidity_coverage.sql) | LCR (Liquidity Coverage Ratio) calculation. |
| **MiFID II** | [regulatory_reporting_mifid.sql](sql_examples/regulatory_reporting_mifid.sql) | Trade transparency reporting. |
| **SWIFT Payments** | [swift_payment_tracking.sql](sql_examples/swift_payment_tracking.sql) | International wire transfer tracking. |
| **Nostro/Vostro** | [nostro_vostro_reconciliation.sql](sql_examples/nostro_vostro_reconciliation.sql) | Inter-bank account reconciliation. |
| **POS Lifecycle** | [pos_terminal_lifecycle.sql](sql_examples/pos_terminal_lifecycle.sql) | Payment terminal fleet maintenance. |

### Insurance
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Insurance Risk** | [insurance_risk_analysis.sql](sql_examples/insurance_risk_analysis.sql) | Loss ratios and policy profitability. |
| **Fraud Rings** | [insurance_claims_fraud_rings.sql](sql_examples/insurance_claims_fraud_rings.sql) | Detecting networks of colluding claimants. |
| **Reinsurance** | [reinsurance_cat_modeling.sql](sql_examples/reinsurance_cat_modeling.sql) | Catastrophe (CAT) modeling and exposure. |

---

## 🏥 Healthcare & Life Sciences

### Clinical & Patient Care
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Readmission** | [healthcare_readmission.sql](sql_examples/healthcare_readmission.sql) | Hospital readmission risk analysis. |
| **EHR Interop** | [healthcare_ehr_interoperability.sql](sql_examples/healthcare_ehr_interoperability.sql) | Unified patient records (Longitudinal). |
| **Telemedicine** | [healthcare_telemedicine_trends.sql](sql_examples/healthcare_telemedicine_trends.sql) | Remote care adoption analysis. |
| **IoT Vitals** | [healthcare_iot_wearables.sql](sql_examples/healthcare_iot_wearables.sql) | Real-time wearable data processing. |
| **Patient Satisfaction**| [healthcare_patient_satisfaction.sql](sql_examples/healthcare_patient_satisfaction.sql) | NPS and HCAHPS survey analysis. |
| **Mental Health** | [healthcare_mental_health_services.sql](sql_examples/healthcare_mental_health_services.sql) | Service utilization and access metrics. |
| **Chronic Disease** | [healthcare_chronic_disease_mgmt.sql](sql_examples/healthcare_chronic_disease_mgmt.sql) | Long-term care management. |

### Operations & Research
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Bed Capacity** | [healthcare_bed_capacity.sql](sql_examples/healthcare_bed_capacity.sql) | ER throughput and occupancy forecasting. |
| **Supply Chain** | [healthcare_medical_supply_chain.sql](sql_examples/healthcare_medical_supply_chain.sql) | PPE and surgical inventory management. |
| **Claims Processing** | [healthcare_insurance_claims.sql](sql_examples/healthcare_insurance_claims.sql) | Denial analysis and adjudication speed. |
| **Clinical Trials** | [pharma_clinical_trials.sql](sql_examples/pharma_clinical_trials.sql) | Patient safety and trial efficacy. |
| **Genomics** | [healthcare_genomic_analysis.sql](sql_examples/healthcare_genomic_analysis.sql) | Variant calling and mutation tracking. |
| **Lab Research** | [biotech_lab_research_tracking.sql](sql_examples/biotech_lab_research_tracking.sql) | Experiment logs and results tracking. |

---

## 🏛️ Government & Public Sector

### Civic Services & Operations (Phase 8)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **311 Service Requests** | Analyzing citizen complaints (potholes, noise, trash) and resolution times. | `RequestType`, `SLA_Breach` | [government_311_service_requests.sql](sql_examples/government_311_service_requests.sql) |
| **Voter Turnout** | Analyzing voter registration and turnout by precinct and demographics. | `TurnoutPct`, `DemographicShift` | [government_voter_turnout.sql](sql_examples/government_voter_turnout.sql) |
| **Public Housing** | Managing public housing unit turnover, occupancy rates, and waitlists. | `VacancyDays`, `WaitlistAging` | [government_public_housing.sql](sql_examples/government_public_housing.sql) |
| **Parks Utilization** | Tracking facility bookings, event attendance, and revenue. | `UtilizationRate`, `RevenuePerAcre` | [government_parks_rec_utilization.sql](sql_examples/government_parks_rec_utilization.sql) |
| **Food Safety** | Restaurant health inspection grades and violation tracking. | `ViolationScore`, `PassRate` | [government_food_safety_inspections.sql](sql_examples/government_food_safety_inspections.sql) |

### Public Safety & Justice (Phase 8)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Crime Statistics** | Geo-spatial analysis of crime types and hotspots. | `CrimeRate`, `HotspotDetection` | [government_crime_statistics.sql](sql_examples/government_crime_statistics.sql) |
| **Fire Response** | Fire department response times and property loss estimates. | `ResponseTime`, `LossEstimate` | [government_fire_incident_response.sql](sql_examples/government_fire_incident_response.sql) |
| **Building Codes** | Tracking building inspections, permits, and code violations. | `ViolationType`, `SafetyRisk` | [government_building_code_violations.sql](sql_examples/government_building_code_violations.sql) |
| **Disaster Relief** | Allocating and tracking disaster relief funds and resources. | `FundAllocation`, `ImpactZone` | [government_disaster_relief.sql](sql_examples/government_disaster_relief.sql) |
| **Homeless Services** | Shelter capacity monitoring and street outreach contacts. | `ShelterOccupancy`, `OutreachMetrics` | [government_homeless_services.sql](sql_examples/government_homeless_services.sql) |

### Finance & Administration (Phase 8)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Grant Management** | Tracking federal/state grant lifecycles, awards, and burn rates. | `BurnRate`, `ComplianceStatus` | [government_grant_management.sql](sql_examples/government_grant_management.sql) |
| **Pension Liability** | Analysis of public employee pension funding ratios and liabilities. | `FundingRatio`, `LiabilityProjection` | [government_pension_liability.sql](sql_examples/government_pension_liability.sql) |
| **Econ Development** | Tracking business registrations, closures, and job creation. | `BizGrowthRate`, `JobCreation` | [government_economic_development.sql](sql_examples/government_economic_development.sql) |
| **FOIA Records** | Managing Freedom of Information Act (FOIA) request backlogs. | `ProcessingTime`, `BacklogCount` | [government_public_records_foia.sql](sql_examples/government_public_records_foia.sql) |
| **Lobbying Reg** | Tracking registered lobbyists, clients, and reported spending. | `SpendAnalysis`, `InfluenceMapping` | [government_lobbying_registration.sql](sql_examples/government_lobbying_registration.sql) |

### Infrastructure & Utilities (Phase 8)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Water Quality** | Monitoring water contaminants (Lead, Copper) against safety limits. | `ContaminantLevel`, `SafetyBreach` | [government_water_quality.sql](sql_examples/government_water_quality.sql) |
| **Snow Removal** | Optimizing snow plow routes and salt material usage. | `RouteEfficiency`, `MaterialUsage` | [government_snow_removal.sql](sql_examples/government_snow_removal.sql) |
| **School Bus Ops** | Route optimization and on-time performance for school transport. | `OnTimePct`, `FuelCost` | [government_school_bus_logistics.sql](sql_examples/government_school_bus_logistics.sql) |
| **Meter Parking** | Analyzing parking meter revenue and violation ticketing by zone. | `RevenuePerSpace`, `ViolationRate` | [government_meter_parking_revenue.sql](sql_examples/government_meter_parking_revenue.sql) |
| **Census Demo** | Analyzing population shifts and demographic trends over time. | `PopGrowth`, `DemographicIndex` | [government_census_demographics.sql](sql_examples/government_census_demographics.sql) |

### General & Legacy Scenarios
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Smart City Traffic**| [smart_city_traffic_analysis.sql](sql_examples/smart_city_traffic_analysis.sql) | Congestion monitoring and sensor data. |
| **Tax Revenue** | [government_tax_revenue_analysis.sql](sql_examples/government_tax_revenue_analysis.sql) | Collection efficiency and projections. |
| **Public Transit** | [government_public_transit_ops.sql](sql_examples/government_public_transit_ops.sql) | Bus/Rail on-time performance. |
| **Emergency (911)** | [government_emergency_response_911.sql](sql_examples/government_emergency_response_911.sql) | Incident response times and heatmaps. |
| **Social Services** | [government_social_services_benefits.sql](sql_examples/government_social_services_benefits.sql) | Benefits application processing speed. |
| **Infrastructure** | [government_infrastructure_maintenance.sql](sql_examples/government_infrastructure_maintenance.sql) | Road/Bridge maintenance tracking. |
| **Procurement** | [government_procurement_spend.sql](sql_examples/government_procurement_spend.sql) | Vendor spend analysis and budget. |
| **Permitting** | [government_permitting_and_licensing.sql](sql_examples/government_permitting_and_licensing.sql) | License approval bottlenecks. |
| **Environmental** | [government_environmental_monitoring.sql](sql_examples/government_environmental_monitoring.sql) | Air quality and pollution sensors. |
| **Judicial** | [government_court_case_management.sql](sql_examples/government_court_case_management.sql) | Court docket backlog analysis. |
| **Waste Mgmt** | [government_sanitation_waste_mgmt.sql](sql_examples/government_sanitation_waste_mgmt.sql) | Sanitation route optimization. |

---

## 🛍️ Retail, E-Commerce & Logistics

| Scenario | File | Description |
| :--- | :--- | :--- |
| **Customer 360** | [retail_customer_analytics.sql](sql_examples/retail_customer_analytics.sql) | RFM Segmentation and sales trends. |
| **E-Comm Funnel** | [ecommerce_clickstream_analytics.sql](sql_examples/ecommerce_clickstream_analytics.sql) | Clickstream, cart abandonment, conversion. |
| **Supply Chain** | [supply_chain_logistics.sql](sql_examples/supply_chain_logistics.sql) | Inventory turnover and shipping delays. |
| **Marketing** | [marketing_multi_touch_attribution.sql](sql_examples/marketing_multi_touch_attribution.sql) | Ad campaign ROI and attribution. |
| **Weather Impact** | [weather_impact_on_retail_sales.sql](sql_examples/weather_impact_on_retail_sales.sql) | Correlating sales with weather patterns. |
| **Food Delivery** | [food_delivery_logistics.sql](sql_examples/food_delivery_logistics.sql) | Rider efficiency and delivery times. |
| **Subscription** | [subscription_box_service_analytics.sql](sql_examples/subscription_box_service_analytics.sql) | Churn and retention analysis. |
| **Fashion Trends** | [fashion_trend_forecasting.sql](sql_examples/fashion_trend_forecasting.sql) | SKU velocity vs trend data. |
| **Crowdfunding** | [crowdfunding_campaign_analytics.sql](sql_examples/crowdfunding_campaign_analytics.sql) | Campaign goal tracking metrics. |
| **Auctions** | [online_auction_analytics.sql](sql_examples/online_auction_analytics.sql) | Bidding fraud and velocity. |

---

## 📡 Technology, Media & Telecom

| Scenario | File | Description |
| :--- | :--- | :--- |
| **Telecom Churn** | [telecom_customer_churn.sql](sql_examples/telecom_customer_churn.sql) | Subscriber retention analysis. |
| **5G Networks** | [telecom_5g_network_optimization.sql](sql_examples/telecom_5g_network_optimization.sql) | Cell tower latency and downtime. |
| **ISP Traffic** | [isp_network_traffic_analysis.sql](sql_examples/isp_network_traffic_analysis.sql) | Bandwidth monitoring and congestion. |
| **Streaming Media** | [media_streaming_content_performance.sql](sql_examples/media_streaming_content_performance.sql) | Content popularity and engagement. |
| **Gaming Economy** | [gaming_player_economy.sql](sql_examples/gaming_player_economy.sql) | Microtransactions and virtual goods. |
| **AdTech** | [adtech_campaign_performance.sql](sql_examples/adtech_campaign_performance.sql) | Impression and click attribution. |
| **Music Royalties** | [music_streaming_royalties.sql](sql_examples/music_streaming_royalties.sql) | Calculation of artist payouts. |
| **Podcast Stats** | [podcast_listener_analytics.sql](sql_examples/podcast_listener_analytics.sql) | Listener demographics and drop-off. |
| **SaaS Support** | [customer_support_ticket_analysis.sql](sql_examples/customer_support_ticket_analysis.sql) | Ticket resolution time and CSAT. |
| **Content Ops** | [content_moderation_queue.sql](sql_examples/content_moderation_queue.sql) | Moderator efficiency tracking. |
| **DevOps** | [devops_dora_metrics.sql](sql_examples/devops_dora_metrics.sql) | Deployment frequency and fail rates. |
| **Cloud FinOps** | [cloud_infrastructure_cost_analysis.sql](sql_examples/cloud_infrastructure_cost_analysis.sql) | Cloud spend optimization. |
| **Cybersecurity** | [cybersecurity_threat_detection.sql](sql_examples/cybersecurity_threat_detection.sql) | Threat detection and anomaly monitoring. |
| **Data Center** | [data_center_power_usage.sql](sql_examples/data_center_power_usage.sql) | PUE efficiency tracking. |
| **DRM** | [digital_rights_management_compliance.sql](sql_examples/digital_rights_management_compliance.sql) | License usage and compliance. |

---

## 🏭 Industrial, Energy & Utilities

| Scenario | File | Description |
| :--- | :--- | :--- |
| **Manufacturing** | [manufacturing_predictive_maintenance.sql](sql_examples/manufacturing_predictive_maintenance.sql) | Sensor anomaly detection (IIoT). |
| **Automotive** | [automotive_fleet_telemetry.sql](sql_examples/automotive_fleet_telemetry.sql) | Fleet fuel and maintenance tracking. |
| **Energy Grid** | [energy_smart_grid_consumption.sql](sql_examples/energy_smart_grid_consumption.sql) | Peak load management. |
| **Oil & Gas** | [oil_gas_pipeline_sensor.sql](sql_examples/oil_gas_pipeline_sensor.sql) | Leak detection and pressure monitoring. |
| **Water Utilities** | [utilities_smart_water_metering.sql](sql_examples/utilities_smart_water_metering.sql) | Consumption and leak analysis. |
| **Solar Power** | [solar_energy_production_monitoring.sql](sql_examples/solar_energy_production_monitoring.sql) | Panel efficiency and output. |
| **Mining** | [mining_extraction_efficiency.sql](sql_examples/mining_extraction_efficiency.sql) | Ore grade and extraction rates. |
| **Chemicals** | [chemical_batch_traceability.sql](sql_examples/chemical_batch_traceability.sql) | Batch tracking and purity. |
| **Aerospace** | [aerospace_component_reliability.sql](sql_examples/aerospace_component_reliability.sql) | Component MTBF and reliability. |
| **Construction** | [construction_project_budget_tracking.sql](sql_examples/construction_project_budget_tracking.sql) | Job costing and budget variance. |
| **Agriculture** | [agriculture_crop_yield_optimization.sql](sql_examples/agriculture_crop_yield_optimization.sql) | Crop yield analysis. |
| **Wastewater** | [wastewater_treatment_compliance.sql](sql_examples/wastewater_treatment_compliance.sql) | Effluent quality monitoring. |
| **EV Charging** | [electric_vehicle_charging_station_utilization.sql](sql_examples/electric_vehicle_charging_station_utilization.sql) | Station usage analytics. |

### Manufacturing & Supply Chain (Phase 17)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Quality Inspection** | AI-driven visual inspection defects and confidence scoring. | `ConfidenceScore`, `DefectHeatmap` | [manufacturing_quality_inspection.sql](sql_examples/manufacturing_quality_inspection.sql) |
| **Reverse Logistics** | Managing returns processing, refurbishment, and restocking. | `ReturnReason`, `RestockCost` | [supply_chain_reverse_logistics.sql](sql_examples/supply_chain_reverse_logistics.sql) |
| **Auto Picking** | Automated warehouse robot efficiency and battery drain analysis. | `BatteryLevel`, `PickPathEff` | [warehouse_automated_picking.sql](sql_examples/warehouse_automated_picking.sql) |
| **Assembly Line** | Balancing "Takt Time" vs Actual Cycle Time. | `CycleTime`, `StationLoad` | [manufacturing_assembly_line.sql](sql_examples/manufacturing_assembly_line.sql) |
| **Component Sourcing** | Single-source risk analysis for critical electronic components. | `RiskScore`, `GeoDiversity` | [supply_chain_component_sourcing.sql](sql_examples/supply_chain_component_sourcing.sql) |
| **Steel Mill Energy** | Tracking energy consumption per ton produced in Arc Furnaces. | `KWhPerTon`, `PeakLoad` | [manufacturing_steel_mill_energy.sql](sql_examples/manufacturing_steel_mill_energy.sql) |
| **Pharma Genealogy** | Recursive batch tracking from raw materials to finished pills. | `BatchTrace`, `IngredientSource` | [manufacturing_pharma_genealogy.sql](sql_examples/manufacturing_pharma_genealogy.sql) |
| **Aerospace Compliance** | Tracking supplier certifications and part compliance for safety. | `CertExpiry`, `ComplianceStatus` | [manufacturing_aerospace_compliance.sql](sql_examples/manufacturing_aerospace_compliance.sql) |
| **Packaging Waste** | Sustainability metrics for CPG packaging and recyclability. | `RecyclablePct`, `WasteTonnage` | [supply_chain_packaging_waste.sql](sql_examples/supply_chain_packaging_waste.sql) |
| **Custom Orders** | Tracking Made-to-Order (MTO) furniture production timelines. | `CustomSLA`, `ProductionStage` | [manufacturing_custom_orders.sql](sql_examples/manufacturing_custom_orders.sql) |

---

## 🎓 Other Industries

### Education & Academic
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Student Performance**| [education_student_performance.sql](sql_examples/education_student_performance.sql) | Grading and attendance tracking. |
| **Higher Ed Enrollment**| [higher_ed_enrollment_funnel.sql](sql_examples/higher_ed_enrollment_funnel.sql) | Application to Matriculation funnel. |
| **Library** | [library_circulation_management.sql](sql_examples/library_circulation_management.sql) | Book circulation and overdue fines. |

### Travel, Transport & Hospitality
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Hospitality** | [hospitality_revenue_management.sql](sql_examples/hospitality_revenue_management.sql) | Hotel RevPAR and occupancy. |
| **Aviation** | [aviation_flight_delays.sql](sql_examples/aviation_flight_delays.sql) | On-time performance analysis. |
| **Airline Loyalty** | [airline_loyalty_program_analytics.sql](sql_examples/airline_loyalty_program_analytics.sql) | Mileage liability and tiers. |
| **Real Estate** | [real_estate_market_trends.sql](sql_examples/real_estate_market_trends.sql) | Property valuation and sales velocity. |
| **Ride Sharing** | [ride_sharing_operations_analytics.sql](sql_examples/ride_sharing_operations_analytics.sql) | Driver earnings and surge pricing. |
| **Maritime** | [maritime_port_operations.sql](sql_examples/maritime_port_operations.sql) | Vessel dwell time and throughput. |
| **Parking** | [parking_garage_occupancy_management.sql](sql_examples/parking_garage_occupancy_management.sql) | Occupancy optimization. |

### Specialized & Niche
| Scenario | File | Description |
| :--- | :--- | :--- |
| **Legal** | [legal_matter_billing_analysis.sql](sql_examples/legal_matter_billing_analysis.sql) | Law firm billable hours tracking. |
| **HR Analytics** | [hr_workforce_analytics.sql](sql_examples/hr_workforce_analytics.sql) | Employee retention and performance. |
| **Recruitment** | [hr_recruitment_pipeline_analytics.sql](sql_examples/hr_recruitment_pipeline_analytics.sql) | Hiring funnel metrics. |
| **Non-Profit** | [nonprofit_donor_retention.sql](sql_examples/nonprofit_donor_retention.sql) | Donor engagement and fundraising. |
| **Sports** | [sports_team_performance.sql](sql_examples/sports_team_performance.sql) | Player stats and game analysis. |
| **Casino** | [casino_floor_optimization.sql](sql_examples/casino_floor_optimization.sql) | Slot machine hold percentages. |
| **Event Ticketing** | [event_ticketing_sales_velocity.sql](sql_examples/event_ticketing_sales_velocity.sql) | Sales tracking and dynamic pricing. |
| **Restaurants** | [restaurant_menu_engineering.sql](sql_examples/restaurant_menu_engineering.sql) | Menu item profitability analysis. |
| **Museums** | [museum_visitor_analytics.sql](sql_examples/museum_visitor_analytics.sql) | Visitor flow and dwell times. |
| **GDPR Privacy** | [gdpr_dsar_tracking.sql](sql_examples/gdpr_dsar_tracking.sql) | Privacy request tracking. |
| **ESG Footprint** | [esg_carbon_footprint_tracking.sql](sql_examples/esg_carbon_footprint_tracking.sql) | Carbon emissions reporting. |
| **Call Centers** | [call_center_sentiment_analysis.sql](sql_examples/call_center_sentiment_analysis.sql) | Agent performance and sentiment. |

### Services, Lifestyle & Hospitality (Phase 17)
| Scenario | Description | Key Features | File |
| :--- | :--- | :--- | :--- |
| **Fitness Trends** | User engagement analysis with wearable fitness tracker data. | `StepGoal`, `HeartRateZone` | [lifestyle_fitness_tracker_trends.sql](sql_examples/lifestyle_fitness_tracker_trends.sql) |
| **Music Streaming** | A/B testing of recommendation algorithms and skip rates. | `SkipRate`, `AB_VariantResult` | [services_music_streaming.sql](sql_examples/services_music_streaming.sql) |
| **Campus Housing** | Optimizing university dorm assignments and occupancy. | `OccupancyRate`, `Preferences` | [services_campus_housing.sql](sql_examples/services_campus_housing.sql) |
| **Baggage Handling** | Root cause analysis for airline lost or delayed luggage. | `MishandleRate`, `ScanGap` | [services_baggage_handling.sql](sql_examples/services_baggage_handling.sql) |
| **Hotel Housekeeping** | Room turnover efficiency and maid productivity tracking. | `TurnoverTime`, `RoomStatus` | [services_hotel_housekeeping.sql](sql_examples/services_hotel_housekeeping.sql) |
| **Cruise Spend** | Analyzing onboard guest spending (Casino, Dining, Shore Ex). | `SpendCategory`, `GuestSegment` | [services_cruise_spend.sql](sql_examples/services_cruise_spend.sql) |
| **Theme Park Queues** | Forecasting wait times and optimizing line management. | `AvgWaitTime`, `PeakHour` | [services_theme_park_queues.sql](sql_examples/services_theme_park_queues.sql) |
| **Rental Car Fleet** | Utilization rates, mileage optimization, and idle time. | `UtilizationPct`, `MaintenanceDue` | [services_rental_car_fleet.sql](sql_examples/services_rental_car_fleet.sql) |
| **Coworking Space** | Desk occupancy hotspots and meeting room usage. | `DeskUtil`, `MeetingDensity` | [services_coworking_space.sql](sql_examples/services_coworking_space.sql) |
| **Kitchen Display** | Restaurant ticket times and station bottleneck analysis. | `PrepTime`, `OrderLag` | [services_kitchen_display_system.sql](sql_examples/services_kitchen_display_system.sql) |

---

## 🚀 Unique & Emerging Domains

| Scenario | File | Description |
| :--- | :--- | :--- |
| **Space Telemetry** | [space_satellite_telemetry.sql](sql_examples/space_satellite_telemetry.sql) | Satellite health & debris collision avoidance. |
| **Esports Analytics** | [esports_tournament_analytics.sql](sql_examples/esports_tournament_analytics.sql) | Match strategy & player performance metrics. |
| **Veterinary Health** | [veterinary_patient_health.sql](sql_examples/veterinary_patient_health.sql) | Patient tracking & breed health trends. |
| **Fine Art Market** | [fine_art_provenance_valuation.sql](sql_examples/fine_art_provenance_valuation.sql) | Provenance tracking & portfolio valuation. |
| **Aquaculture** | [aquaculture_farm_monitoring.sql](sql_examples/aquaculture_farm_monitoring.sql) | Fish farming water quality & yields. |
| **Forestry** | [forestry_sustainable_harvest.sql](sql_examples/forestry_sustainable_harvest.sql) | LiDAR biomass estimation & harvest planning. |
| **Film Production** | [film_production_logistics.sql](sql_examples/film_production_logistics.sql) | Shooting schedules & daily burn rates. |
| **Autonomous Vehicles** | [autonomous_vehicle_sensor_fusion.sql](sql_examples/autonomous_vehicle_sensor_fusion.sql) | Sensor reliability & safety incidents. |
| **Smart Home** | [smart_home_energy_optimization.sql](sql_examples/smart_home_energy_optimization.sql) | Energy vampire detection & cost savings. |
| **Genealogy** | [genealogy_record_linkage.sql](sql_examples/genealogy_record_linkage.sql) | Ancestry tree matching with census records. |


### 🌐 Comprehensive Multi-Source Patterns
| Pattern | File | Description |
| :--- | :--- | :--- |
| **Retail Customer 360** | [retail_customer_360.sql](customer360/retail_customer_360.sql) | Unified profile from ERP, CRM, and Web sources. |
| **Hybrid Supply Chain** | [hybrid_supply_chain.sql](customer360/hybrid_supply_chain.sql) | Control tower merging Snowflake, S3, and Postgres. |

---

## 🛠️ Technical Demos

**SQL Functions**
-   [Math Functions](sql_function_examples/math_functions.sql)
-   [String Functions](sql_function_examples/string_functions.sql)
-   [Date/Time Functions](sql_function_examples/date_time_functions.sql)
-   [Aggregates](sql_function_examples/aggregate_functions.sql)
-   [Window Functions](sql_function_examples/window_functions.sql)
-   [Type Conversion](sql_function_examples/conversion_functions.sql)
-   [Conditional Logic](sql_function_examples/conditional_functions.sql)

**Generative AI**
-   [Sentiment & Tagging](sql_ai_function_examples/sentiment_classification.sql)
-   [Data Transformation](sql_ai_function_examples/data_transformation.sql)

**Admin & Operations**
-   [Access Control (RBAC)](sql_admin_examples/access_control_rbac.sql)
-   [Reflections Tuning](sql_admin_examples/reflection_management.sql)
-   [Data Maintenance (Vacuum/Optimize)](sql_admin_examples/data_maintenance.sql)
-   [Engine Management](sql_admin_examples/source_engine_management.sql)

**Advanced DML & Ingestion**
-   [`sql_ingestion_examples/`](sql_ingestion_examples/) (Copy Into, Merge, Incremental)
-   [`sql_dml_examples/`](sql_dml_examples/) (SCD Type 2)
-   [`sql_iceberg_metadata_examples/`](sql_iceberg_metadata_examples/) (Snapshots, Manifests)
-   [`sql_system_table_examples/`](sql_system_table_examples/) (Job History, Audit Logs)

---

## 🤖 Prompt Engineering Templates

Speed up your data modeling with our ready-to-use GenAI prompts.

-   **[Modeling Prompt Template](sample_prompts/modeling_prompt_template.md)**: A universal template to generate Bronze-Silver-Gold architectures from any dataset list.
-   **[Example Application](sample_prompts/example_prompt_application.sql)**: A working example using 3 raw tables (Commerce) to demonstrate the prompt in action.
-   **[Prompt Library (Built-in Agent)](sample_prompts/prompt_library.md)**: A cookbook of prompts optimized for Dremio's **Integrated Query Agent**.
-   **[External Agent Skills & Python](sample_prompts/external_agent_skills.md)**: Guide for using external agents (Claude/Cursor) to write Python and use the `dremio-agent-skill`.

---


## 🔌 Connectivity & Integrations

**Dremio Interfaces** (Direct Access)
-   **[Arrow Flight](dremio_interfaces/arrow_flight.md)**: High-performance data science.
-   **[REST API](dremio_interfaces/rest_api.md)**: Job automation.
-   **[JDBC/ODBC](dremio_interfaces/jdbc.md)**: BI tools.
-   **[dremio-simple-query](dremio_interfaces/dremio_simple_query.md)**: Python wrapper.
-   **[dremioframe](dremio_interfaces/dremioframe.md)**: DataFrame API.
-   **[dremio-cli](dremio_interfaces/dremio_cli.md)**: Command-line admin execution.

**Iceberg Catalog Integrations** (External Engines)
-   **[Apache Spark](other_engines/apache_spark.md)**
-   **[Apache Flink](other_engines/apache_flink.md)**
-   **[Trino](other_engines/trino.md)**
-   **[StarRocks](other_engines/starrocks.md)**
-   **[PyIceberg](other_engines/pyiceberg.md)**
-   **[DuckDB](other_engines/duckdb.md)**
-   **[Polars](other_engines/polars.md)**
-   **[Daft](other_engines/daft.md)**
-   **[Spice.ai](other_engines/spiceai.md)**
-   Native Iceberg: [Java](other_engines/iceberg_java.md) | [Rust](other_engines/iceberg_rust.md) | [Go](other_engines/iceberg_go.md)

---
