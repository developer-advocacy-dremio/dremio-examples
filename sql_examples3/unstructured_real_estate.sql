/*
 * Dremio "Messy Data" Challenge: Unstructured Real Estate Listings
 * 
 * Scenario: 
 * MLS (Multiple Listing Service) data from various legacy brokers.
 * 'Address' is a raw string, not broken into Street/City/Zip.
 * 'Amenities' is sometimes a CSV string, sometimes JSON-like.
 * 'Price' fields mix '$' symbols and 'K' notation (e.g. '$500K').
 * 
 * Objective for AI Agent:
 * 1. Parse 'Address' using String Splitting or Regex to extract Zip Code.
 * 2. Clean 'Price' to numeric (expand 'K' to thousands).
 * 3. Extract specific amenities (e.g. 'Pool', 'Garage') into boolean flags.
 */

-------------------------------------------------------------------------------
-- 1. SETUP: Create Raw Folder
-------------------------------------------------------------------------------

CREATE FOLDER IF NOT EXISTS MLS_Data_Lake;
CREATE FOLDER IF NOT EXISTS MLS_Data_Lake.Raw_Feed;

-------------------------------------------------------------------------------
-- 2. DDL: Create Raw Tables
-------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS MLS_Data_Lake.Raw_Feed.LISTINGS (
    LIST_ID VARCHAR,
    RAW_ADDR VARCHAR,
    PRICE_TXT VARCHAR,
    AMENITIES VARCHAR, -- messy encoding
    SQFT INT
);

-------------------------------------------------------------------------------
-- 3. SEED DATA (Parsing Nightmares)
-------------------------------------------------------------------------------

-- Standard-ish
INSERT INTO MLS_Data_Lake.Raw_Feed.LISTINGS VALUES
('L-100', '123 Main St, Springfield, IL, 62704', '$250,000', 'Garage, Pool, AC', 2000),
('L-101', '456 Oak Ave, Chicago, IL, 60614', '$500,000', 'Gym, Doorman', 1500);

-- K Notation / Weird Prices
INSERT INTO MLS_Data_Lake.Raw_Feed.LISTINGS VALUES
('L-102', '789 Pine Rd, Austin, TX 73301', '$1.2M', '["Pool", "Spa"]', 3000), -- JSON array + M notation
('L-103', '101 Maple Ln; Miami; FL; 33101', '850K', 'Pool;Dock;View', 2500); -- Semicolon address + K notation

-- Missing tokens
INSERT INTO MLS_Data_Lake.Raw_Feed.LISTINGS VALUES
('L-104', 'One Infinite Loop, Cupertino CA', '$3M', 'Tech Hub', 5000), -- No Zip?
('L-105', 'Unknown Address', 'Call for Price', '', 0);

-- Bulk Fill
INSERT INTO MLS_Data_Lake.Raw_Feed.LISTINGS VALUES
('L-200', '100 1st Ave, NY, NY, 10001', '$1M', 'Elevator', 800),
('L-201', '200 2nd Ave, NY, NY 10002', '$1.2M', 'Elevator, Gym', 900),
('L-202', '300 3rd Ave, NY, NY 10003', '$1,500,000', 'Elevator/Gym', 1000), -- Slash separator
('L-203', '400 4th St, Seattle WA 98101', '500000', 'None', 600),
('L-204', '500 5th St, Seattle, WA, 98102', '550k', 'Balcony', 650),
('L-205', '600 6th St, Seattle, WA 98103', '600k', 'Balcony|View', 700), -- Pipe separator
('L-206', '700 7th St, San Francisco, CA', '$1.5M', '{"pool": true, "gym": false}', 800), -- JSON object
('L-207', '800 8th St, San Francisco, CA 94101', '$2m', '{"pool": false}', 850),
('L-208', '900 9th St, Boston, MA, 02108', '800,000', 'Roof Deck', 1100),
('L-209', '10 Broad St, Boston MA 02109', '850000', 'Roof Deck', 1150),
('L-210', '11 High St, Denver CO 80202', '400K', 'Parking', 1200),
('L-211', '12 Low St, Denver CO 80203', '450K', 'Parking, Storage', 1250),
('L-212', '13 W St, Boulder CO 80302', '900K', 'Mountain View', 2000),
('L-213', '14 E St, Boulder CO', '1M', 'View', 2500),
('L-214', '15 N St, Portland OR 97201', '600000', 'Garden', 1500),
('L-215', '16 S St, Portland, Oregon, 97202', '650,000.00', 'Garden', 1600),
('L-216', '17 Up St, Atlanta GA 30303', '300K', 'Pool', 1800),
('L-217', '18 Down St, Atlanta GA', '$350k', 'Pool', 1900),
('L-218', '19 Left St, Dallas TX 75201', '500K', 'Garage', 2000),
('L-219', '20 Right St, Dallas TX 75202', '550K', 'Garage', 2100),
('L-220', '21 Center St, Houston TX 77002', '400K', 'AC', 2200),
('L-221', '22 Side St, Houston TX, 77003', '450 k', 'AC, Pool', 2300), -- Space in K
('L-222', '23 Back St, Phoenix AZ 85001', '350000', 'Desert Landscaping', 2000),
('L-223', '24 Front St, Phoenix AZ', '$375,000', 'Pool', 2100),
('L-224', '25 Top St, Las Vegas NV 89101', '400,000', 'Casino View', 1500),
('L-225', '26 Bottom St, Las Vegas, NV', '450000', 'Slot Machine', 1600),
('L-226', '27 Hill St, Nashville TN 37201', '600 K', 'Music Studio', 2000),
('L-227', '28 Dale St, Nashville TN', '700K', 'Studio', 2500),
('L-228', '29 River Rd, Memphis TN 38103', '200K', 'River View', 1500),
('L-229', '30 Lake Rd, Memphis TN', '250K', 'Boat Dock', 1800),
('L-230', '31 Ocean Dr, Miami FL 33139', '5M', 'Beach Access', 4000),
('L-231', '32 Bay Dr, Miami FL', '6.5M', 'Beach', 5000),
('L-232', '33 Gulf Blvd, Tampa FL 33602', '800K', 'Pool', 2000),
('L-233', '34 Sea Way, Tampa FL', '850K', 'Pool', 2100),
('L-234', '35 Mountain Ln, Salt Lake City UT 84101', '700K', 'Ski Access', 3000),
('L-235', '36 Valley Ln, Salt Lake City UT', '750K', 'Ski', 3200),
('L-236', '37 Plain Rd, Omaha NE 68102', '200K', 'Cornfield', 4000),
('L-237', '38 Field Rd, Omaha NE', '220K', 'Barn', 4500),
('L-238', '39 City Pl, Chicago IL 60601', '400K', 'Lobby', 800),
('L-239', '40 Town Pl, Chicago IL', '450K', 'Doorman', 900),
('L-240', '41 Suburb Ct, Naperville IL 60540', '500K', 'Yard', 2500),
('L-241', '42 Rural Ct, Plainfield IL', '300K', 'Acreage', 5000),
('L-242', '43 State St, Madison WI 53703', '350K', 'Cheese Cellar', 1500),
('L-243', '44 Capitol Ave, Madison WI', '400K', 'Near Capitol', 1600),
('L-244', '45 Lake St, Milwaukee WI 53202', '300K', 'Brewery Nearby', 1400),
('L-245', '46 River St, Milwaukee WI', '320K', 'Riverwalk', 1500),
('L-246', '47 Forest Dr, Seattle WA', '1.1M', 'Trees', 2000),
('L-247', '48 Rain Ct, Seattle WA', '1.2M', 'Rain Garden', 2200),
('L-248', '49 Tech Blvd, San Jose CA 95113', '2M', 'Smart Home', 1800),
('L-249', '50 Silicon Way, Palo Alto CA 94301', '5M', 'Solar Panels', 2500);

-------------------------------------------------------------------------------
-- 4. EXAMPLE AI AGENT PROMPT
-------------------------------------------------------------------------------
/*
 * Prompt to Copy/Paste into Dremio AI:
 * "Parsing unstructured listings in MLS_Data_Lake.Raw_Feed.LISTINGS.
 *  
 *  1. Bronze: Raw View.
 *  2. Silver: 
 *     - Extract ZIP Code from 'RAW_ADDR' (look for 5 digits at the end).
 *     - Clean 'PRICE_TXT' into numeric Price_USD (Handle 'K' and 'M' suffixes).
 *     - Flag 'Has_Pool': True if 'AMENITIES' contains 'Pool' (case insensitive).
 *  3. Gold: 
 *     - Average Price per SqFt by extracted ZIP Code.
 *  
 *  Show me the SQL."
 */
