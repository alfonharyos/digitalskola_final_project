/*
==========================================================================================================
timestamps - from yelping_review table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, day, month, year)
SELECT YR.timestamp,
       EXTRACT(DAY FROM YR.timestamp),
       EXTRACT(MONTH FROM YR.timestamp),
       EXTRACT(YEAR FROM YR.timestamp)
FROM RAW_LAYER.yelp_review AS YR
ON CONFLICT (timestamp) DO NOTHING;


/*
==========================================================================================================
Insert review data from yelping_review table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.review (
    review_id, 
    user_id, 
    business_id, 
    stars, 
    useful, 
    funny, 
    cool, 
    text, 
    timestamp
)
SELECT YR.review_id, 
       YR.user_id, 
       YR.business_id, 
       YR.stars, 
       YR.useful, 
       YR.funny, 
       YR.cool, 
       YR.text, 
       YR.timestamp
FROM RAW_LAYER.yelp_review AS YR
INNER JOIN RAW_LAYER.yelp_user AS YU
ON YR.user_id = YU.user_id;


