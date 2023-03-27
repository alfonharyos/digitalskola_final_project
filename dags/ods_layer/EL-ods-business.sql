
/* 
==========================================================================================================
Insert ODS_LAYER Business 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.business(
        business_id,
        name,
        stars,
        review_count,
        is_open,
        categories
)
SELECT  YB.business_id,
        YB.name,
        YB.stars,
        YB.review_count,
        YB.is_open,
        YB.categories
FROM RAW_LAYER.yelp_business AS YB;
