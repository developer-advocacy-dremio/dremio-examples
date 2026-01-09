/*
 * AI Text Analysis Demo
 * 
 * Scenario:
 * Analyzing customer product reviews to extract sentiment and classify feedback.
 * 
 * Requirements:
 * - Dremio environment with Generative AI features enabled.
 * - Connected AI provider (e.g., OpenAI, Azure OpenAI).
 */

-------------------------------------------------------------------------------
-- 0. SETUP: Create Dummy Data
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS RetailDB.AI_Lab;

CREATE TABLE IF NOT EXISTS RetailDB.AI_Lab.Customer_Reviews (
    ReviewID INT,
    Product VARCHAR,
    ReviewText VARCHAR
);

INSERT INTO RetailDB.AI_Lab.Customer_Reviews (ReviewID, Product, ReviewText) VALUES
(1, 'Wireless Headphones', 'The battery life is amazing, but the ear cups are a bit uniformtable after an hour.'),
(2, 'Smart Watch', 'Completely stopped working after two days. Terrible support experience.'),
(3, 'Running Shoes', 'Love the cushioning! Best run I have had in years. Highly recommend.'),
(4, 'Coffee Maker', 'It makes okay coffee, but it is way too loud and vibrates the whole counter.'),
(5, 'Laptop Stand', 'Sturdy and adjustable. Exactly what I needed for my home office.');

-------------------------------------------------------------------------------
-- 1. SENTIMENT ANALYSIS
-- Use AI to determine if a review is Positive, Negative, or Neutral.
-------------------------------------------------------------------------------

SELECT
    ReviewID,
    Product,
    ReviewText,
    AI_GENERATE_TEXT(
        'Analyze the sentiment of the following customer review. Respond with only one word: "Positive", "Negative", or "Neutral". Review: ' || ReviewText
    ) AS Sentiment
FROM RetailDB.AI_Lab.Customer_Reviews;

-------------------------------------------------------------------------------
-- 2. CLASSIFICATION & TAGGING
-- Extract specific issues or topics from the text.
-------------------------------------------------------------------------------

SELECT
    ReviewID,
    Product,
    ReviewText,
    AI_GENERATE_TEXT(
        'Identify the main topic of this review (e.g., "Quality", "Support", "Performance", "Comfort", "Price"). Return only the topic name. Review: ' || ReviewText
    ) AS Main_Topic
FROM RetailDB.AI_Lab.Customer_Reviews;

-------------------------------------------------------------------------------
-- 3. CLEANING & SUMMARIZATION
-- Fix typos or summarize long reviews.
-------------------------------------------------------------------------------

SELECT
    ReviewID,
    ReviewText,
    AI_GENERATE_TEXT(
        'Correct any spelling mistakes in the following text and return the cleaned version: ' || ReviewText
    ) AS Cleaned_Text
FROM RetailDB.AI_Lab.Customer_Reviews;
