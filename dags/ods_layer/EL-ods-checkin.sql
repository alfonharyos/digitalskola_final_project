/*
==========================================================================================================
Insert checkin data from RAW_LAYER.yelp_checkin table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.checkin (business_id, date)
SELECT YC.business_id, YC.date
FROM RAW_LAYER.yelp_checkin AS YC;

