
/* 
==========================================================================================================
Insert Business location 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.location (
    business_id,
    address, 
    city, 
    state, 
    postal_code,
    latitude, 
    longitude
)
SELECT DISTINCT ON (YB.state, YB.postal_code, YB.latitude, YB.longitude)
    YB.business_id,
    YB.address, 
    YB.city, 
    YB.state, 
    YB.postal_code, 
    YB.latitude, 
    YB.longitude
FROM RAW_LAYER.yelp_business AS YB;
