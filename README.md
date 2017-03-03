# PostgresTimeseriesAnalysis #

## Project ##

This is an example project for Timeseries Analysis with PostgreSQL. 

## Dataset ##

The data is the [Quality Controlled Local Climatological Data (QCLCD)]: 

> Quality Controlled Local Climatological Data (QCLCD) consist of hourly, daily, and monthly summaries for approximately 
> 1,600 U.S. locations. Daily Summary forms are not available for all stations. Data are available beginning January 1, 2005 
> and continue to the present. Please note, there may be a 48-hour lag in the availability of the most recent data.

The data is available at:

* [http://www.ncdc.noaa.gov/orders/qclcd/](http://www.ncdc.noaa.gov/orders/qclcd/)

## Query ##

### Calculate Seconds between two Timestamps ###

```
CREATE OR REPLACE FUNCTION sample.DateDiffSeconds(start_t TIMESTAMP, end_t TIMESTAMP) 
RETURNS INT AS $$
DECLARE
    diff_interval INTERVAL; 
    diff INT = 0;
   BEGIN
    -- Difference between End and Start Timestamp:
    diff_interval = end_t - start_t;
    
    -- Calculate the Difference in Seconds:
    diff = ((DATE_PART('day', end_t - start_t) * 24 + 
            DATE_PART('hour', end_t - start_t)) * 60 +
            DATE_PART('minute', end_t - start_t)) * 60 +
            DATE_PART('second', end_t - start_t);
            
     RETURN diff;
   END;
   $$ LANGUAGE plpgsql;
```

### Find Missing Values ###

```
SELECT  *
FROM (SELECT 
        weather_data.wban as WbanIdentifier, 
        weather_data.datetime as MeasurementDateTime,                 
        LAG(weather_data.datetime, 1, Null) OVER (PARTITION BY weather_data.wban ORDER BY weather_data.datetime) AS PreviousMeasurementDateTime
     FROM sample.weather_data) LagSelect
WHERE sample.datediffseconds (PreviousMeasurementDateTime, MeasurementDateTime) > 3600;
```


[PostgreSQL]: https://www.postgresql.org
[Quality Controlled Local Climatological Data (QCLCD)]: https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/quality-controlled-local-climatological-data-qclcd
