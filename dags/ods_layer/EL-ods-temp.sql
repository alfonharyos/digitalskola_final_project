/*
==========================================================================================================
Loading timestamps - from weather_temperature table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, day, month, year)
SELECT RT.date,
       EXTRACT(DAY FROM RT.date),
       EXTRACT(MONTH FROM RT.date),
       EXTRACT(YEAR FROM RT.date)
FROM RAW_LAYER.weather_temperature AS RT
ON CONFLICT (timestamp) DO NOTHING;


/*
==========================================================================================================
Loading temperature data from weather_temperature table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.temperature (
        date, 
        temp_min, 
        temp_max, 
        temp_normal_min, 
        temp_normal_max)
SELECT  RT.date, 
        RT.min, 
        RT.max,         
        RT.normal_min, 
        RT.normal_max
FROM    RAW_LAYER.weather_temperature AS RT;

