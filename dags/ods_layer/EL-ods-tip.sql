/*
==========================================================================================================
timestamps - from yelping_tip table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, day, month, year)
SELECT YT.timestamp,
       EXTRACT(DAY FROM YT.timestamp),
       EXTRACT(MONTH FROM YT.timestamp),
       EXTRACT(YEAR FROM YT.timestamp)
FROM RAW_LAYER.yelp_tip AS YT
ON CONFLICT (timestamp) DO NOTHING;

/*
==========================================================================================================
Insert tips from yelping_tip table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.tip (
        user_id, 
        business_id, 
        text, 
        timestamp, 
        compliment_count
        )
SELECT  YT.user_id, 
        YT.business_id, 
        YT.text, 
        YT.timestamp, 
        YT.compliment_count
FROM RAW_LAYER.yelp_tip AS YT;
