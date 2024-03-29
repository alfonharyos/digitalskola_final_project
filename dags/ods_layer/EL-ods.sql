-- TRUNCATE ODS_LAYER.temperature;
-- TRUNCATE ODS_LAYER.precipitation;
-- TRUNCATE ODS_LAYER.checkin;
-- TRUNCATE ODS_LAYER.review;
-- TRUNCATE ODS_LAYER.location CASCADE;
-- TRUNCATE ODS_LAYER.business_attributes CASCADE;
-- TRUNCATE ODS_LAYER.business CASCADE;
-- TRUNCATE ODS_LAYER.business_hours;
-- TRUNCATE ODS_LAYER.date_time CASCADE;
-- TRUNCATE ODS_LAYER.user CASCADE;
-- TRUNCATE ODS_LAYER.tip CASCADE;

-- /* 
-- ==========================================================================================================
-- Insert ODS_LAYER Business 
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.business(
--         business_id,
--         name,
--         stars,
--         review_count,
--         is_open,
--         categories
-- )
-- SELECT  YB.business_id,
--         YB.name,
--         YB.stars,
--         YB.review_count,
--         YB.is_open,
--         YB.categories
-- FROM RAW_LAYER.yelp_business AS YB;


-- /* 
-- ==========================================================================================================
-- Insert Business location 
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.location (
--     business_id,
--     address, 
--     city, 
--     state, 
--     postal_code,
--     latitude, 
--     longitude
-- )
-- SELECT DISTINCT ON (YB.state, YB.postal_code, YB.latitude, YB.longitude)
--     YB.business_id,
--     YB.address, 
--     YB.city, 
--     YB.state, 
--     YB.postal_code, 
--     YB.latitude, 
--     YB.longitude
-- FROM RAW_LAYER.yelp_business AS YB;


-- /* 
-- ==========================================================================================================
-- Insert Business Attributes 
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.business_attributes (
--     business_id, 
--     NoiseLevel, 
--     BikeParking, 
--     RestaurantsAttire, 
--     BusinessAcceptsCreditCards, 
--     BusinessParking, 
--     RestaurantsReservations, 
--     GoodForKids, 
--     RestaurantsTakeOut,
--     Caters, 
--     WiFi, 
--     RestaurantsDelivery, 
--     HasTV, 
--     RestaurantsPriceRange2, 
--     Alcohol, 
--     Music, 
--     BusinessAcceptsBitcoin, 
--     GoodForDancing, 
--     DogsAllowed, 
--     BestNights, 
--     RestaurantsGoodForGroups, 
--     OutdoorSeating, 
--     HappyHour, 
--     RestaurantsTableService, 
--     GoodForMeal, 
--     WheelchairAccessible, 
--     Ambience, 
--     CoatCheck, 
--     DriveThru, 
--     Smoking, 
--     BYOB, 
--     Corkage)
-- SELECT YB.business_id, 
--     attributes->>'NoiseLevel', 
--     attributes->>'BikeParking', 
--     attributes->>'RestaurantsAttire', 
--     attributes->>'BusinessAcceptsCreditCards', 
--     attributes->>'BusinessParking', 
--     attributes->>'RestaurantsReservations', 
--     attributes->>'GoodForKids', 
--     attributes->>'RestaurantsTakeOut', 
--     attributes->>'Caters', 
--     attributes->>'WiFi', 
--     attributes->>'RestaurantsDelivery', 
--     attributes->>'HasTV', 
--     attributes->>'RestaurantsPriceRange2', 
--     attributes->>'Alcohol', 
--     attributes->>'Music', 
--     attributes->>'BusinessAcceptsBitcoin', 
--     attributes->>'GoodForDancing', 
--     attributes->>'DogsAllowed', 
--     attributes->>'BestNights', 
--     attributes->>'RestaurantsGoodForGroups', 
--     attributes->>'OutdoorSeating', 
--     attributes->>'HappyHour', 
--     attributes->>'RestaurantsTableService', 
--     attributes->>'GoodForMeal', 
--     attributes->>'WheelchairAccessible', 
--     attributes->>'Ambience', 
--     attributes->>'CoatCheck', 
--     attributes->>'DriveThru', 
--     attributes->>'Smoking', 
--     attributes->>'BYOB', 
--     attributes->>'Corkage'
-- FROM RAW_LAYER.yelp_business AS YB;


-- /* 
-- ==========================================================================================================
-- Insert Business hours 
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.business_hours (
--     business_id, 
--     monday, 
--     tuesday, 
--     wednesday, 
--     thursday, 
--     friday, 
--     saturday, 
--     sunday)
-- SELECT YB.business_id, 
--     hours->>'Monday', 
--     hours->>'Tuesday', 
--     hours->>'Wednesday', 
--     hours->>'Thursday', 
--     hours->>'Friday', 
--     hours->>'Saturday', 
--     hours->>'Sunday'
-- FROM RAW_LAYER.yelp_business AS YB;


-- /*
-- ==========================================================================================================
-- Loading timestamps - yelping_since - from user table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT YU.yelping_since,
--        EXTRACT(DAY FROM YU.yelping_since),
--        EXTRACT(WEEK FROM YU.yelping_since),
--        EXTRACT(MONTH FROM YU.yelping_since),
--        EXTRACT(YEAR FROM YU.yelping_since)
-- FROM RAW_LAYER.yelp_user AS YU
-- ON CONFLICT (timestamp) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading user data from RAW to ods
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.user (
-- 	user_id, 
-- 	name, 
-- 	review_count, 
-- 	yelping_since, 
-- 	useful, 
-- 	funny, 
-- 	cool, 
-- 	elite, 
-- 	friends,
-- 	fans, 
-- 	average_stars, 
-- 	compliment_hot, 
-- 	compliment_more, 
-- 	compliment_profile, 
-- 	compliment_cute,
-- 	compliment_list, 
-- 	compliment_note, 
-- 	compliment_plain, 
-- 	compliment_cool, 
-- 	compliment_funny,
-- 	compliment_writer, 
-- 	compliment_photos
-- )
-- SELECT YU.user_id, 
--        YU.name, 
--        YU.review_count, 
--        YU.yelping_since, 
--        YU.useful, 
--        YU.funny, 
--        YU.cool, 
--        YU.elite, 
--        YU.friends,
--        YU.fans, 
--        YU.average_stars, 
--        YU.compliment_hot, 
--        YU.compliment_more, 
--        YU.compliment_profile, 
--        YU.compliment_cute,
--        YU.compliment_list, 
--        YU.compliment_note, 
--        YU.compliment_plain, 
--        YU.compliment_cool, 
--        YU.compliment_funny,
--        YU.compliment_writer, 
--        YU.compliment_photos
-- FROM RAW_LAYER.yelp_user AS YU
-- LEFT JOIN ODS_LAYER.user AS OU ON YU.user_id = OU.user_id
-- WHERE OU.user_id IS NULL;


-- /*
-- ==========================================================================================================
-- Loading timestamps - yelping_tip - from user table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT YT.timestamp,
--        EXTRACT(DAY FROM YT.timestamp),
--        EXTRACT(WEEK FROM YT.timestamp),
--        EXTRACT(MONTH FROM YT.timestamp),
--        EXTRACT(YEAR FROM YT.timestamp)
-- FROM RAW_LAYER.yelp_tip AS YT
-- ON CONFLICT (timestamp) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading tips from staging.yelping_tip table to ods
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.tip (
--         user_id, 
--         business_id, 
--         text, 
--         timestamp, 
--         compliment_count
--         )
-- SELECT  YT.user_id, 
--         YT.business_id, 
--         YT.text, 
--         YT.timestamp, 
--         YT.compliment_count
-- FROM RAW_LAYER.yelp_tip AS YT;


/*
==========================================================================================================
Loading checkin data from staging.yelping_checkin table to ods
==========================================================================================================
*/

-- INSERT INTO ODS_LAYER.checkin (business_id, date)
-- SELECT  YC.business_id, 
--         TO_TIMESTAMP(t.date, 'YYYY-MM-DD HH24:MI:SS')
-- FROM RAW_LAYER.yelp_checkin AS YC
-- JOIN (
--   SELECT business_id, unnest(string_to_array(date, ',')) AS date
--   FROM RAW_LAYER.yelp_checkin AS YC
-- ) AS t ON YC.business_id = t.business_id;

-- /*
-- ==========================================================================================================
-- Loading timestamps - checkin - from checkin table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT OC.timestamp,
--        EXTRACT(DAY FROM OC.timestamp),
--        EXTRACT(WEEK FROM OC.timestamp),
--        EXTRACT(MONTH FROM OC.timestamp),
--        EXTRACT(YEAR FROM OC.timestamp)
-- FROM ODS_LAYER.checkin AS OC
-- ON CONFLICT (timestamp) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading timestamps - yelping_review - from user table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT YR.timestamp,
--        EXTRACT(DAY FROM YR.timestamp),
--        EXTRACT(WEEK FROM YR.timestamp),
--        EXTRACT(MONTH FROM YR.timestamp),
--        EXTRACT(YEAR FROM YR.timestamp)
-- FROM RAW_LAYER.yelp_review AS YR
-- ON CONFLICT (timestamp) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading review data from staging.yelping_review table to ods
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.review (
-- 	review_id, 
-- 	user_id, 
-- 	business_id, 
-- 	stars, 
-- 	useful, 
-- 	funny, 
-- 	cool, 
-- 	text, 
-- 	timestamp
-- )
-- SELECT YR.review_id, 
--        YR.user_id, 
--        YR.business_id, 
--        YR.stars, 
--        YR.useful, 
--        YR.funny, 
--        YR.cool, 
--        YR.text, 
--        YR.timestamp
-- FROM RAW_LAYER.yelp_review AS YR
-- LEFT JOIN ODS_LAYER.review AS ORV ON YR.review_id = ORV.review_id
-- WHERE ORV.review_id IS NULL
-- RIGHT JOIN ODS_LAYER.review AS ORV ON YU.user_id;


-- /*
-- ==========================================================================================================
-- Loading timestamps - yelping_review - from user table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT RT.date,
--        EXTRACT(DAY FROM RT.date),
--        EXTRACT(WEEK FROM RT.date),
--        EXTRACT(MONTH FROM RT.date),
--        EXTRACT(YEAR FROM RT.date)
-- FROM RAW_LAYER.weather_temperature AS RT
-- ON CONFLICT (timestamp) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading temperature data from staging.weather_temperature table to ods
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.temperature (
--         date, 
--         temp_min, 
--         temp_max, 
--         temp_normal_min, 
--         temp_normal_max)
-- SELECT  TO_DATE(RT.date, 'YYYYMMDD'), 
--         RT.min, 
--         RT.max,         
--         RT.normal_min, 
--         RT.normal_max
-- FROM    RAW_LAYER.weather_temperature AS RT
-- LEFT JOIN ODS_LAYER.date_time AS DT ON RT.date = TO_CHAR(DT.timestamp, 'YYYYMMDD')
-- WHERE DT.timestamp IS NULL
-- ON CONFLICT (date) DO NOTHING;


-- /*
-- ==========================================================================================================
-- Loading timestamps - yelping_review - from user table to date_time table
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.date_time (timestamp, day, week, month, year)
-- SELECT RP.date,
--        EXTRACT(DAY FROM RP.date),
--        EXTRACT(WEEK FROM RP.date),
--        EXTRACT(MONTH FROM RP.date),
--        EXTRACT(YEAR FROM RP.date)
-- FROM RAW_LAYER.weather_precipitation AS RP
-- ON CONFLICT (timestamp) DO NOTHING;

-- /*
-- ==========================================================================================================
-- Loading precipitation data from staging.weather_precipitation table to ods
-- ==========================================================================================================
-- */
-- INSERT INTO ODS_LAYER.precipitation (
--         date, 
--         precipitation, 
--         precipitation_normal
--         )
-- SELECT  RP.date, 
--         RP.precipitation, 
--         RP.precipitation_normal
-- FROM    RAW_LAYER.weather_precipitation AS RP
-- LEFT JOIN ODS_LAYER.date_time AS DT ON RP.date = TO_CHAR(DT.timestamp, 'YYYYMMDD')
-- WHERE DT.timestamp IS NULL
-- ON CONFLICT (date) DO NOTHING;