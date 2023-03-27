
/* 
==========================================================================================================
Insert Business hours 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.business_hours (
    business_id, 
    monday, 
    tuesday, 
    wednesday, 
    thursday, 
    friday, 
    saturday, 
    sunday)
SELECT YB.business_id, 
    hours->>'Monday', 
    hours->>'Tuesday', 
    hours->>'Wednesday', 
    hours->>'Thursday', 
    hours->>'Friday', 
    hours->>'Saturday', 
    hours->>'Sunday'
FROM RAW_LAYER.yelp_business AS YB;
