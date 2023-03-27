/*
==========================================================================================================
timestamps - from weather_precipitation table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, day, month, year)
SELECT RP.date,
       EXTRACT(DAY FROM RP.date),
       EXTRACT(MONTH FROM RP.date),
       EXTRACT(YEAR FROM RP.date)
FROM RAW_LAYER.weather_precipitation AS RP
ON CONFLICT (timestamp) DO NOTHING;


/*
==========================================================================================================
Loading precipitation data from weather_precipitation table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.precipitation (
        date, 
        precipitation, 
        precipitation_normal
        )
SELECT  RP.date, 
        RP.precipitation, 
        RP.precipitation_normal
FROM    RAW_LAYER.weather_precipitation AS RP;