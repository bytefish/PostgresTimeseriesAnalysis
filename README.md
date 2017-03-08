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

## Enable PostgreSQL Statistics ##

First of all we need to find out, which ``postgresql.config`` is currently loaded:

```sql
-- Show the currently used config file:
SHOW config_file;
```

The ``pg_stat_statements`` module must be configured in the ``postgresq.conf``:

```
shared_preload_libraries='pg_stat_statements'

pg_stat_statements.max = 10000
pg_stat_statements.track = all
```

Now we can load the ``pg_stat_statements`` and query the most recent queries:

```sql
-- Load the pg_stat_statements:
create extension pg_stat_statements;

-- Show recent Query statistics:  
select * 
from pg_stat_statements
order by queryid desc;
```

## Enable Parallel Queries ##

First of all we need to find out, which ``postgresql.config`` is currently loaded:

```sql
-- Show the currently used config file:
SHOW config_file;
```

Then we need to set the parameters ``max_worker_processes``and ``max_parallel_workers_per_gather``:

```
max_worker_processes = 8		# (change requires restart)
max_parallel_workers_per_gather = 4	# taken from max_worker_processes
```

## Queries ##

### How to calculate the seconds between two Timestamps ###

First we define a Function ``DateDiffSeconds`` to calculate the seconds between two timestamps. 

This can be done using PostgreSQL ``DATE_PART`` function:

```sql
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
   $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
   COST 1000;
```

### Find Missing Values (without Index) ###

Now we identify the timestamps where the difference between two measurements is greater than 1 hour:

```sql
SELECT  *
FROM (SELECT 
        weather_data.wban as WbanIdentifier, 
        weather_data.datetime as MeasurementDateTime,                 
        LAG(weather_data.datetime, 1, Null) OVER (PARTITION BY weather_data.wban ORDER BY weather_data.datetime) AS PreviousMeasurementDateTime
     FROM sample.weather_data) LagSelect
WHERE sample.datediffseconds (PreviousMeasurementDateTime, MeasurementDateTime) > 3600;
```

Execution Time:

```
Total query runtime: 2 min.
17043 rows retrieved.
```

### Find Missing Values (with Index) ###

First create an index:

```sql
CREATE INDEX idx_weather_data_datetime ON sample.weather_data (wban, datetime) 
``` 

Then execute the query to find missing values again:

Execution Time:

```
Total query runtime: 40 secs.
17043 rows retrieved.
``` 

### Initial Work on Interpolation ###

First we define a function to get the first and last value of a result set:

```sql
-- Create a function that always returns the first non-NULL item
CREATE OR REPLACE FUNCTION sample.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;
 
-- And then wrap an aggregate around it
CREATE AGGREGATE sample.FIRST (
        sfunc    = sample.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
 
-- Create a function that always returns the last non-NULL item
CREATE OR REPLACE FUNCTION sample.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;
 
-- And then wrap an aggregate around it
CREATE AGGREGATE sample.LAST (
        sfunc    = sample.last_agg,
        basetype = anyelement,
        stype    = anyelement
);
``` 


The following query builds a query, which interpolates the values in 5 Minute intervals.

```sql
with bounded as (
    select 
    	datetime, 
    	'epoch'::timestamp + '5 Minute'::interval * (extract(epoch from datetime)::int4 / 300) as slice, 
    	temperature,
    	LAG(temperature) OVER(PARTITION BY wban ORDER BY datetime) AS prevval, 
        LEAD(temperature) OVER(PARTITION BY wban ORDER BY datetime) AS nextval
    from sample.weather_data
    where wban = '53922'
    order by datetime asc
    limit 100
), 
dense as(
	select slice
	from generate_series('2015-03-01'::timestamp, '2015-03-02'::timestamp, '5 Minute'::Interval)  s(slice)
),
filled as(
    select slice,
       temperature,
       coalesce(temperature,
                sample.linear_interpolate(slice, 
                                          sample.last(datetime) over (lookback),
                                          sample.last(temperature) over (lookback),
                                          sample.last(datetime) over (lookforward),
                                          sample.last(temperature) over (lookforward))) interpolated
    from bounded right join dense
    using (slice)
    window 
        lookback as (order by slice, datetime),
        lookforward as (order by slice desc, datetime desc)
    order by slice, datetime
)
select 
	slice, 
    min(interpolated), 
    max(interpolated),
    avg(interpolated)
from filled
group by slice
order by slice;
```

Next we define a function to do a Linear Interpolation between timesteps:

```sql
CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i timestamp, x_0 timestamp, y_0 double precision, x_1 timestamp, y_1 double precision)
RETURNS DOUBLE PRECISION AS $$
   BEGIN
   	return sample.linear_interpolate(
        sample.timestamp_to_seconds(x_i),
        sample.timestamp_to_seconds(x_0),
        y_0,
        sample.timestamp_to_seconds(x_1),
        y_1);
   END;
   $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
   COST 1000;


CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i int, x_0 int, y_0 DOUBLE PRECISION, x_1 int, y_1 DOUBLE PRECISION) 
RETURNS DOUBLE PRECISION AS $$
DECLARE
    x INT = 0; 
    m DOUBLE PRECISION = 0;
    n DOUBLE PRECISION = 0;
   BEGIN
    -- Function to solve is: y = mx + n, so first solve m:
    m = (y_1 - y_0) / (x_1 - x_0);
    n = y_0;
    x = (x_i - x_0);
    
    RETURN (m * x + n);
   END;
   $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
   COST 1000;
```

## Additional Resources ##

* http://tapoueh.org/blog/2013/08/20-Window-Functions
* https://www.compose.com/articles/metrics-maven-window-functions-in-postgresql/
* https://wiki.postgresql.org/images/a/a2/PostgreSQL_Window_Functions.pdf
* http://blog.cleverelephant.ca/2016/03/parallel-postgis.html

### Resampling and Interpolation ###

* https://content.pivotal.io/blog/time-series-analysis-part-3-resampling-and-interpolation


[PostgreSQL]: https://www.postgresql.org
[Quality Controlled Local Climatological Data (QCLCD)]: https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/quality-controlled-local-climatological-data-qclcd
