/*
 * Marketing: Influencer Campaign ROI
 * 
 * Scenario:
 * Analyzing engagement rates and Cost-Per-Acquisition (CPA) for social creators.
 * 
 * Architecture: Medallion (Bronze -> Silver -> Gold)
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Folder Structure
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS SocialMediaDB;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Influencers;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Influencers.Bronze;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Influencers.Silver;
CREATE FOLDER IF NOT EXISTS SocialMediaDB.Influencers.Gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER: Social Data
-------------------------------------------------------------------------------

-- CreatorProfiles Table
CREATE TABLE IF NOT EXISTS SocialMediaDB.Influencers.Bronze.CreatorProfiles (
    CreatorHandle VARCHAR,
    Platform VARCHAR, -- Instagram, TikTok, YouTube
    FollowerCount INT,
    ContractCost DOUBLE
);

INSERT INTO SocialMediaDB.Influencers.Bronze.CreatorProfiles VALUES
('@janesmith', 'Instagram', 500000, 5000.0),
('@bobtech', 'YouTube', 1200000, 15000.0),
('@dance_sarah', 'TikTok', 3000000, 8000.0),
('@foodie_mike', 'Instagram', 250000, 2500.0),
('@travel_tom', 'Instagram', 100000, 1000.0),
('@gamer_girl', 'Twitch', 800000, 5000.0),
('@fitness_phil', 'YouTube', 500000, 6000.0),
('@beauty_beth', 'TikTok', 2000000, 4000.0),
('@comedy_chris', 'TikTok', 1500000, 3000.0),
('@tech_tim', 'YouTube', 900000, 10000.0),
('@diy_dave', 'YouTube', 400000, 4000.0),
('@mom_life', 'Instagram', 150000, 1500.0),
('@pet_pat', 'TikTok', 500000, 1000.0),
('@finance_frank', 'YouTube', 300000, 5000.0),
('@yoga_yasmin', 'Instagram', 200000, 2000.0),
('@chef_carl', 'TikTok', 800000, 2500.0),
('@art_anna', 'Instagram', 300000, 2800.0),
('@music_max', 'YouTube', 600000, 7000.0),
('@crypto_carl', 'Twitter', 100000, 5000.0),
('@news_nancy', 'Twitter', 500000, 3000.0),
('@auto_alex', 'YouTube', 700000, 8500.0),
('@book_betty', 'Instagram', 50000, 500.0),
('@garden_gary', 'YouTube', 200000, 2500.0),
('@history_hank', 'TikTok', 900000, 2000.0),
('@science_sue', 'YouTube', 1100000, 12000.0),
('@fashion_faye', 'Instagram', 600000, 5500.0),
('@gym_greg', 'TikTok', 400000, 1200.0),
('@life_coach_lisa', 'Instagram', 80000, 800.0),
('@skincare_sam', 'TikTok', 1200000, 3500.0),
('@gadget_guy', 'YouTube', 1500000, 14000.0),
('@vegan_vicky', 'Instagram', 350000, 3200.0),
('@retro_rob', 'YouTube', 250000, 3000.0),
('@woodwork_will', 'YouTube', 180000, 2000.0),
('@decor_deb', 'Instagram', 450000, 4000.0),
('@knit_kat', 'Instagram', 70000, 700.0),
('@dance_dan', 'TikTok', 2500000, 7000.0),
('@prank_paul', 'YouTube', 4000000, 20000.0),
('@movie_mary', 'YouTube', 650000, 6500.0),
('@car_carl', 'TikTok', 300000, 1000.0),
('@boat_bill', 'Instagram', 40000, 400.0),
('@plane_pete', 'YouTube', 550000, 7000.0),
('@train_tom', 'YouTube', 120000, 1500.0),
('@lego_larry', 'Instagram', 220000, 2100.0),
('@puzzle_pat', 'TikTok', 100000, 400.0),
('@magic_mike', 'YouTube', 850000, 9000.0),
('@skate_steve', 'Instagram', 180000, 1800.0),
('@surf_sarah', 'Instagram', 230000, 2300.0),
('@snow_sam', 'TikTok', 450000, 1500.0),
('@hike_harry', 'Instagram', 90000, 900.0),
('@camp_cathy', 'YouTube', 320000, 3500.0);

-- CampaignPosts Table
CREATE TABLE IF NOT EXISTS SocialMediaDB.Influencers.Bronze.CampaignPosts (
    PostID VARCHAR,
    CreatorHandle VARCHAR,
    Likes INT,
    Comments INT,
    Shares INT,
    Conversions INT, -- Sales/Signups
    PostDate DATE
);

INSERT INTO SocialMediaDB.Influencers.Bronze.CampaignPosts VALUES
('P-001', '@janesmith', 25000, 500, 200, 150, '2025-05-01'),
('P-002', '@bobtech', 80000, 2000, 500, 400, '2025-05-02'),
('P-003', '@dance_sarah', 150000, 3000, 10000, 200, '2025-05-03'), -- Viral but low conversion
('P-004', '@foodie_mike', 12000, 300, 100, 50, '2025-05-01'),
('P-005', '@travel_tom', 8000, 150, 50, 20, '2025-05-04'),
('P-006', '@gamer_girl', 40000, 1200, 300, 250, '2025-05-05'),
('P-007', '@fitness_phil', 35000, 800, 400, 300, '2025-05-06'),
('P-008', '@beauty_beth', 100000, 2500, 1500, 400, '2025-05-07'),
('P-009', '@comedy_chris', 200000, 5000, 12000, 100, '2025-05-08'), -- Viral, very low conv
('P-010', '@tech_tim', 60000, 1500, 800, 500, '2025-05-09'), -- High conversion
('P-011', '@diy_dave', 25000, 600, 300, 150, '2025-05-10'),
('P-012', '@mom_life', 10000, 400, 200, 80, '2025-05-11'),
('P-013', '@pet_pat', 45000, 900, 2000, 40, '2025-05-12'),
('P-014', '@finance_frank', 15000, 800, 400, 300, '2025-05-13'), -- High conv rate
('P-015', '@yoga_yasmin', 18000, 350, 150, 60, '2025-05-14'),
('P-016', '@chef_carl', 75000, 1500, 3000, 200, '2025-05-15'),
('P-017', '@art_anna', 22000, 500, 300, 90, '2025-05-16'),
('P-018', '@music_max', 50000, 1200, 600, 100, '2025-05-17'),
('P-019', '@crypto_carl', 5000, 1000, 200, 50, '2025-05-18'),
('P-020', '@news_nancy', 12000, 2000, 5000, 10, '2025-05-19'), -- Low conversion
('P-021', '@auto_alex', 55000, 1800, 700, 450, '2025-05-20'),
('P-022', '@book_betty', 4000, 200, 50, 30, '2025-05-21'),
('P-023', '@garden_gary', 18000, 600, 250, 120, '2025-05-22'),
('P-024', '@history_hank', 85000, 2200, 1500, 150, '2025-05-23'),
('P-025', '@science_sue', 95000, 3000, 2000, 600, '2025-05-24'), -- Star performer
('P-026', '@fashion_faye', 42000, 1100, 400, 350, '2025-05-25'),
('P-027', '@gym_greg', 30000, 600, 1200, 80, '2025-05-26'),
('P-028', '@life_coach_lisa', 7000, 300, 100, 40, '2025-05-27'),
('P-029', '@skincare_sam', 110000, 2800, 1800, 420, '2025-05-28'),
('P-030', '@gadget_guy', 140000, 3500, 1200, 800, '2025-05-29'), -- Huge conversion
('P-031', '@vegan_vicky', 28000, 700, 300, 180, '2025-05-30'),
('P-032', '@retro_rob', 22000, 900, 400, 100, '2025-06-01'),
('P-033', '@woodwork_will', 16000, 500, 200, 90, '2025-06-02'),
('P-034', '@decor_deb', 38000, 900, 600, 220, '2025-06-03'),
('P-035', '@knit_kat', 6000, 250, 100, 45, '2025-06-04'),
('P-036', '@dance_dan', 220000, 4000, 15000, 180, '2025-06-05'),
('P-037', '@prank_paul', 450000, 10000, 50000, 50, '2025-06-06'), -- Worst ROI likely
('P-038', '@movie_mary', 48000, 1300, 500, 150, '2025-06-07'),
('P-039', '@car_carl', 25000, 700, 800, 60, '2025-06-08'),
('P-040', '@boat_bill', 3000, 100, 20, 10, '2025-06-09'),
('P-041', '@plane_pete', 45000, 1100, 400, 220, '2025-06-10'),
('P-042', '@train_tom', 11000, 400, 150, 50, '2025-06-11'),
('P-043', '@lego_larry', 19000, 500, 200, 110, '2025-06-12'),
('P-044', '@puzzle_pat', 8000, 150, 300, 15, '2025-06-13'),
('P-045', '@magic_mike', 70000, 2000, 1500, 350, '2025-06-14'),
('P-046', '@skate_steve', 15000, 400, 200, 50, '2025-06-15'),
('P-047', '@surf_sarah', 20000, 500, 300, 100, '2025-06-16'),
('P-048', '@snow_sam', 35000, 800, 1000, 70, '2025-06-17'),
('P-049', '@hike_harry', 8000, 200, 50, 40, '2025-06-18'),
('P-050', '@camp_cathy', 29000, 750, 300, 140, '2025-06-19');

-------------------------------------------------------------------------------
-- 2. SILVER LAYER: Engagement Analysis
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SocialMediaDB.Influencers.Silver.PerformanceMetrics AS
SELECT 
    cp.CreatorHandle,
    p.Platform,
    p.FollowerCount,
    p.ContractCost,
    cp.Likes + cp.Comments + cp.Shares AS TotalInteractions,
    cp.Conversions,
    -- Calculate Engagement Rate: Interactions / Followers
    (CAST((cp.Likes + cp.Comments + cp.Shares) AS DOUBLE) / p.FollowerCount) * 100 AS EngagementRatePct,
    -- Calculate Cost Per Acquisition (CPA): Cost / Conversions
    CASE 
        WHEN cp.Conversions > 0 THEN CAST(p.ContractCost AS DOUBLE) / cp.Conversions 
        ELSE NULL 
    END AS CPA_Cost
FROM SocialMediaDB.Influencers.Bronze.CampaignPosts cp
JOIN SocialMediaDB.Influencers.Bronze.CreatorProfiles p ON cp.CreatorHandle = p.CreatorHandle;

-------------------------------------------------------------------------------
-- 3. GOLD LAYER: ROI Scorecard
-------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SocialMediaDB.Influencers.Gold.CreatorScorecard AS
SELECT 
    CreatorHandle,
    Platform,
    EngagementRatePct,
    CPA_Cost,
    CASE 
        WHEN CPA_Cost < 20.0 THEN 'High Performer'
        WHEN CPA_Cost < 50.0 THEN 'Average Performer'
        ELSE 'Low Performer'
    END AS PerformanceTier
FROM SocialMediaDB.Influencers.Silver.PerformanceMetrics;

-------------------------------------------------------------------------------
-- 4. AGENT PROMPTS
-------------------------------------------------------------------------------

/*
PROMPT 1:
"List the top 5 creators by Engagement Rate from the Silver layer."

PROMPT 2:
"Identify all 'High Performer' creators in the SocialMediaDB.Influencers.Gold.CreatorScorecard view."

PROMPT 3:
"Comparing platforms, which one has the lowest average CPA based on the Gold layer data?"
*/
