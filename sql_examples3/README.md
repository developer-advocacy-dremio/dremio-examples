# Dremio SQL Examples Phase 8: Raw & Messy Data
## "The AI Agent's Crucible"

This directory (`sql_examples3`) contains 10 SQL scenarios designed specifically to **challenge and train Dremio AI Agents**.

Unlike the previous examples, these datasets are **intentionally "dirty"**. They simulate real-world data dumps from legacy systems, OCR scans, and loosely governed IoT networks.

### The Objective
An AI Agent analyzing these files should not just "run them". It should be able to:
1.  **Detect Issues**: Identify duplicates, nulls, outliers, and schema drift.
2.  **Propose cleaning strategies**: Regex patterns, fuzzy matching, deduplication logic.
3.  **Architect Solutions**: Design a Medallion Architecture (Bronze -> Silver -> Gold) to sanitize this data.

### The Scenarios

| File | Domain | "Messy" Characteristics |
| :--- | :--- | :--- |
| **[legacy_banking_system.sql](legacy_banking_system.sql)** | Banking | Opaque column names (`C1`, `C2`), duplicate keys, deleted flags. |
| **[messy_iot_sensors.sql](messy_iot_sensors.sql)** | IoT | Timestamp logic errors, calibration outliers (`-9999`), exact duplicates. |
| **[obfuscated_ad_clicks.sql](obfuscated_ad_clicks.sql)** | AdTech | Key mismatches (`INT` vs `HASH`), timezone alignment issues. |
| **[broken_healthcare_claims.sql](broken_healthcare_claims.sql)** | Healthcare | Mixed date formats (`MM-DD` vs `DD-MM`), OCR currency artifacts. |
| **[unstructured_retail_inventory.sql](unstructured_retail_inventory.sql)** | Retail | Fuzzy string matching needed for product names, mixed currency symbols. |
| **[fragmented_logistics_tracking.sql](fragmented_logistics_tracking.sql)** | Logistics | Split data across tables, overlapping IDs, inconsistent status codes. |
| **[erroneous_energy_readings.sql](erroneous_energy_readings.sql)** | Energy | Impossible physics (negative amps), massive outlier filtering. |
| **[dirty_telecom_cdr.sql](dirty_telecom_cdr.sql)** | Telecom | Regex cleaning for phone numbers (`+1`, `.`, `-`), duration unit normalization. |
| **[mismatched_hr_records.sql](mismatched_hr_records.sql)** | HR | Entity resolution across tables using fuzzy logic on Names/Emails. |
| **[json_heavy_game_events.sql](json_heavy_game_events.sql)** | Gaming | Extraction of fields from varying JSON schemas stored in a string column. |
| **[multilingual_content_moderation.sql](multilingual_content_moderation.sql)** | Social | 3 tables (`USERS`, `COMMENTS`, `FLAGGED_TERMS`). Cleaning Mojibake and filtering spam. |
| **[smart_grid_voltage.sql](smart_grid_voltage.sql)** | Energy | 3 tables (`METERS`, `READINGS`, `EVENT_LOGS`). Detecting voltage spikes and outages. |
| **[cross_border_crypto.sql](cross_border_crypto.sql)** | Finance | 3 tables (`WALLETS`, `TRANSACTIONS`, `RATES`). Normalizing symbols and flagging risky transfers. |
| **[genomic_sequence_alignment.sql](genomic_sequence_alignment.sql)** | Biotech | 3 tables (`SAMPLES`, `READS`, `INSTRUMENTS`). Filtering low-quality DNA reads. |
| **[public_transit_schedules.sql](public_transit_schedules.sql)** | Transport | 3 tables (`ROUTES`, `TRIPS`, `STOP_TIMES`). Analyzing bus lateness and schedule adherence. |
| **[saas_subscription_churn.sql](saas_subscription_churn.sql)** | SaaS | 3 tables (`CUSTOMERS`, `PLANS`, `SUBSCRIPTIONS`). Calculating MRR and identifying churn. |
| **[ev_charging_network.sql](ev_charging_network.sql)** | Auto | 3 tables (`SITES`, `CHARGERS`, `SESSIONS`). Detecting charger faults and idle fees. |
| **[digital_ad_bidding.sql](digital_ad_bidding.sql)** | AdTech | 3 tables (`CAMPAIGNS`, `BIDS`, `WINS`). Calculating Win Rate and CPM spend. |
| **[online_education_proctoring.sql](online_education_proctoring.sql)** | EdTech | 3 tables (`STUDENTS`, `EXAMS`, `TELEMETRY`). Flagging cheating via background app events. |
| **[waste_management_logistics.sql](waste_management_logistics.sql)** | City | 3 tables (`ZONES`, `BINS`, `PICKUPS`). Optimizing routes based on fill levels. |
| **[smart_ag_irrigation.sql](smart_ag_irrigation.sql)** | AgriTech | 3 tables (`FIELDS`, `SENSORS`, `READINGS`). Triggering irrigation based on soil moisture. |
| **[drone_delivery_flight_paths.sql](drone_delivery_flight_paths.sql)** | Logistics | 3 tables (`DRONES`, `ORDERS`, `TELEMETRY`). Tracking flight paths and GPS errors. |
| **[cloud_costing_tags.sql](cloud_costing_tags.sql)** | FinOps | 3 tables (`RESOURCES`, `TAGS`, `COST_LOGS`). Attributing costs to normalized cost centers. |
| **[customer_support_tickets.sql](customer_support_tickets.sql)** | Support | 3 tables (`AGENTS`, `CUSTOMERS`, `TICKETS`). Measuring SLA breaches and resolution time. |
| **[music_streaming_rights.sql](music_streaming_rights.sql)** | Media | 3 tables (`ARTISTS`, `SONGS`, `STREAMS`). Calculating royalties for streams > 30s. |
| **[pharmaceutical_clinical_trials.sql](pharmaceutical_clinical_trials.sql)** | Pharma | 3 tables (`TRIALS`, `PATIENTS`, `VISITS`). Analyzing bio-markers by treatment group. |
| **[tax_authority_fraud.sql](tax_authority_fraud.sql)** | Govt | 3 tables (`FILERS`, `RETURNS`, `AUDITS`). Detecting mismatching bank accounts and low tax rates. |
| **[factory_predictive_maintenance.sql](factory_predictive_maintenance.sql)** | Mfg | 3 tables (`LINES`, `MACHINES`, `SENSORS`). Predicting failures from temp/vibration spikes. |
| **[university_course_enrollment.sql](university_course_enrollment.sql)** | Edu | 3 tables (`STUDENTS`, `COURSES`, `ENROLLMENTS`). Auditing prerequisites and class capacity. |
| **[telecom_network_latency.sql](telecom_network_latency.sql)** | Telecom | 3 tables (`TOWERS`, `DEVICES`, `PINGS`). Identifying dead zones and high latency towers. |
| **[messy_airline_logs.sql](messy_airline_logs.sql)** | Airline | IATA vs ICAO codes, timezone offsets leading to negative flight times. |
| **[dirty_ecommerce_clickstream.sql](dirty_ecommerce_clickstream.sql)** | E-commerce | Bot traffic filtering (User-Agents), URL parameter parsing. |
| **[fragmented_supply_chain.sql](fragmented_supply_chain.sql)** | Supply Chain | Unit mismatches (Kg vs Lbs), currency conversion (EUR vs USD). |
| **[unstructured_real_estate.sql](unstructured_real_estate.sql)** | Real Estate | Address parsing (Street/City/Zip), unstructured amenities JSON. |
| **[noisy_social_media.sql](noisy_social_media.sql)** | Social Media | Emoji/Non-ASCII spam detection, relative timestamp normalized. |
| **[mixed_weather_data.sql](mixed_weather_data.sql)** | Weather | Fahrenheit vs Celsius units, broken sensor codes (9999). |
| **[drifting_ride_share.sql](drifting_ride_share.sql)** | Ride Share | GPS drift/outliers (Null Island), duplicate status events. |
| **[legacy_hospital_records.sql](legacy_hospital_records.sql)** | Hospital | Gender encoding drift (M/F vs 1/2), DOB standardization. |
| **[chaotic_university_enrollment.sql](chaotic_university_enrollment.sql)** | Education | Student ID migration (Old vs New format), Grade mixing (Letter vs GPA). |
| **[duplicate_loyalty_accounts.sql](duplicate_loyalty_accounts.sql)** | Retail | Entity resolution for users with multiple email/phone signups. |
| **[streaming_service_buffering.sql](streaming_service_buffering.sql)** | Streaming | Session concurrency (device sharing), buffering event chaining. |
| **[legacy_library_catalog.sql](legacy_library_catalog.sql)** | Library | ISBN-10 vs 13, Roman numeral dates, Call number sorting. |
| **[hotel_booking_overlaps.sql](hotel_booking_overlaps.sql)** | Hotel | Double bookings (date overlap), room type code confusion. |
| **[car_maintenance_history.sql](car_maintenance_history.sql)** | Automotive | Odometer rollbacks (decreasing values), VIN typos (OCR errors). |
| **[insurance_policy_renewals.sql](insurance_policy_renewals.sql)** | Insurance | Policy date gaps and overlaps, negative premiums. |
| **[construction_project_logs.sql](construction_project_logs.sql)** | Construction | Circular dependencies in tasks, messy budget text fields. |
| **[restaurant_pos_variations.sql](restaurant_pos_variations.sql)** | Restaurant | Menu item name variations, tax calculation errors. |
| **[voting_machine_logs.sql](voting_machine_logs.sql)** | Government | Precinct ID variations, timestamp sorting, adjudicated vote flags. |
| **[fitness_tracker_sync.sql](fitness_tracker_sync.sql)** | Health | Timezone jumps during travel, sensor artifacts (>220 bpm). |
| **[warehouse_robot_telemetry.sql](warehouse_robot_telemetry.sql)** | IoT | Robot ID collisions (duplicates), battery level nulls/gaps. |
| **[multilingual_content_moderation.sql](multilingual_content_moderation.sql)** | Social | Mojibake encoding detection, mixed charsets and control characters. |
| **[smart_grid_voltage.sql](smart_grid_voltage.sql)** | Energy | Voltage outliers (>250V), missing intervals, blackout flags. |
| **[cross_border_crypto.sql](cross_border_crypto.sql)** | Finance | Wallet address validation, decimal precision loss, mixed casing. |
| **[genomic_sequence_alignment.sql](genomic_sequence_alignment.sql)** | Science | DNA string parsing, noise (N) calculation, invalid characters. |
| **[public_transit_schedules.sql](public_transit_schedules.sql)** | Logistics | GTFS delay logic, midnight timestamp crossing, route loops. |
| **[saas_subscription_churn.sql](saas_subscription_churn.sql)** | SaaS | Billing cycle overlaps, upgrade priority logic, messy plan names. |
| **[ev_charging_network.sql](ev_charging_network.sql)** | Auto | Connector compatibility issues, charging spikes, impossible kWh. |
| **[digital_ad_bidding.sql](digital_ad_bidding.sql)** | AdTech | Micro-second timestamps, auction ID mismatches, currency precision. |
| **[online_education_proctoring.sql](online_education_proctoring.sql)** | EdTech | Time gaps in telemetry, gaze coordinate parsing, alt-tab detection. |
| **[waste_management_logistics.sql](waste_management_logistics.sql)** | IoT | Bin sensor drift, missed pickup reconciliation, route optimization. |
| **[smart_ag_irrigation.sql](smart_ag_irrigation.sql)** | Agriculture | Soil sensor drift, time aggregation (Hourly vs Daily), rain correlation. |
| **[drone_delivery_flight_paths.sql](drone_delivery_flight_paths.sql)** | Logistics | 3D coordinate parsing, FAA altitude violations, geofence breaches. |
| **[cloud_costing_tags.sql](cloud_costing_tags.sql)** | Cloud | Tag case sensitivity normalization, instance type parsing, JSON extraction. |
| **[customer_support_tickets.sql](customer_support_tickets.sql)** | Support | Ticket re-open loops, priority code standardization (P1 vs High). |
| **[music_streaming_rights.sql](music_streaming_rights.sql)** | Media | Fuzzy artist name matching, micro-royalty precision, metadata drift. |
| **[esports_tournament_stats.sql](esports_tournament_stats.sql)** | Gaming | Gamertag changes, match restart overlaps, KDA string parsing. |
| **[corporate_expense_reports.sql](corporate_expense_reports.sql)** | Finance | OCR typos (1OO.00), currency mixing, duplicate expense detection. |
| **[gig_economy_driver_pay.sql](gig_economy_driver_pay.sql)** | Gig | Pay component reconciliation (Surge/Tip/Fee), cancellation logic. |
| **[smart_home_conflicts.sql](smart_home_conflicts.sql)** | IoT | Device state thrashing, conflicting modes (Heat + Cool). |
| **[clinical_trial_data.sql](clinical_trial_data.sql)** | Healthcare | Dosage unit conversion (mg/g), PII suppression in IDs. |

### Usage
Run these scripts to seed the "Bronze/Raw" layer in your Dremio environment. Then, ask your AI Agent:
> "Look at the `Old_Bank_System` folder. Inspect the data quality and propose a SQL view to clean it."
