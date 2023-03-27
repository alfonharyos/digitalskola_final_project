/*
==========================================================================================================
timestamps - yelping_since - from user table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, day, month, year)
SELECT YU.yelping_since,
       EXTRACT(DAY FROM YU.yelping_since),
       EXTRACT(MONTH FROM YU.yelping_since),
       EXTRACT(YEAR FROM YU.yelping_since)
FROM RAW_LAYER.yelp_user AS YU
ON CONFLICT (timestamp) DO NOTHING;


/*
==========================================================================================================
Insert user data from RAW to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.user (
	user_id, 
	name, 
	review_count, 
	yelping_since, 
	useful, 
	funny, 
	cool, 
	elite, 
	friends,
	fans, 
	average_stars, 
	compliment_hot, 
	compliment_more, 
	compliment_profile, 
	compliment_cute,
	compliment_list, 
	compliment_note, 
	compliment_plain, 
	compliment_cool, 
	compliment_funny,
	compliment_writer, 
	compliment_photos
)
SELECT YU.user_id, 
       YU.name, 
       YU.review_count, 
       YU.yelping_since, 
       YU.useful, 
       YU.funny, 
       YU.cool, 
       YU.elite, 
       YU.friends,
       YU.fans, 
       YU.average_stars, 
       YU.compliment_hot, 
       YU.compliment_more, 
       YU.compliment_profile, 
       YU.compliment_cute,
       YU.compliment_list, 
       YU.compliment_note, 
       YU.compliment_plain, 
       YU.compliment_cool, 
       YU.compliment_funny,
       YU.compliment_writer, 
       YU.compliment_photos
FROM RAW_LAYER.yelp_user AS YU;