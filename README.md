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

First we define a Function ``datediff_seconds`` to calculate the seconds between two timestamps. 

This can be done using PostgreSQL ``DATE_PART`` function:

```sql
CREATE OR REPLACE FUNCTION sample.datediff_seconds(start_t TIMESTAMP, end_t TIMESTAMP) 
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
WHERE sample.datediff_seconds (PreviousMeasurementDateTime, MeasurementDateTime) > 3600;
```

Execution Time:

```
Successfully run. Total query runtime: 33 secs 728 msec.
17043 rows affected.
```

### Find Missing Values (with Index) ###

First create an index:

```sql
CREATE INDEX idx_weather_data_datetime ON sample.weather_data (wban, datetime) 
``` 

Then execute the query to find missing values again:

Execution Time:

```
Successfully run. Total query runtime: 21 secs 824 msec.
17043 rows affected.
``` 

### Linear Interpolation with PostgreSQL ###

First we define a function to get the first and last value of a result set:

```sql
CREATE OR REPLACE FUNCTION sample.first_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $1;
$$;
 
CREATE AGGREGATE sample.FIRST (
        sfunc    = sample.first_agg,
        basetype = anyelement,
        stype    = anyelement
);
 
CREATE OR REPLACE FUNCTION sample.last_agg ( anyelement, anyelement )
RETURNS anyelement LANGUAGE SQL IMMUTABLE STRICT AS $$
        SELECT $2;
$$;
 
CREATE AGGREGATE sample.LAST (
        sfunc    = sample.last_agg,
        basetype = anyelement,
        stype    = anyelement
);
``` 

Then we define a method to turn a Timestamp into seconds since ``epoch``:

```sql
CREATE OR REPLACE FUNCTION sample.timestamp_to_seconds(timestamp_t TIMESTAMP) 
RETURNS INT AS $$
DECLARE
    seconds INT = 0;
   BEGIN
    seconds = select extract('epoch' from timestamp_t);
    
    RETURN diff;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
COST 1000;
```

Next we define a general function to do a Linear Interpolation between two values:

```sql
CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i int, x_0 int, y_0 DOUBLE PRECISION, x_1 int, y_1 DOUBLE PRECISION) 
RETURNS DOUBLE PRECISION AS $$
DECLARE
    x INT = 0; 
    m DOUBLE PRECISION = 0;
    n DOUBLE PRECISION = 0;
   BEGIN

    m = (y_1 - y_0) / (x_1 - x_0);
    n = y_0;
    x = (x_i - x_0);
    
    RETURN (m * x + n);
   END;
   $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
   COST 1000;
```

And now we can use ``sample.linear_interpolate`` and ``timestamp_to_seconds`` to interpolate between two timestamps:

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

```

### Interpolate Temperatures in Intervals ###

The following query then builds a query, which interpolates the values in intervals for a station:

```sql
CREATE OR REPLACE FUNCTION sample.interpolate_temperature(station_p varchar(255), start_t timestamp, end_t timestamp, slice_t interval)
RETURNS TABLE(r_wban varchar(255), r_slice timestamp, min_temp DOUBLE PRECISION, max_temp DOUBLE PRECISION, avg_temp DOUBLE PRECISION) AS $$
   BEGIN
   RETURN QUERY 
   WITH bounded_series AS (
            SELECT
              wban,
                datetime,
                'epoch'::timestamp + '5 Minute'::interval * (extract(epoch from datetime)::int4 / 300) AS slice,
                temperature
            FROM sample.weather_data w
            WHERE w.wban = station_p
            ORDER BY datetime ASC
        ),
        dense_series AS (
            SELECT station_p as wban, slice
            FROM generate_series(start_t, end_t, slice_t)  s(slice)
        ),
        filled_series AS (
            SELECT
               wban,
               slice,
               temperature,
               COALESCE(temperature, sample.linear_interpolate(slice,
                                                  sample.last(datetime) over (lookback),
                                                  sample.last(temperature) over (lookback),
                                                  sample.last(datetime) over (lookforward),
                                                  sample.last(temperature) over (lookforward))) interpolated
            FROM bounded_series 
                RIGHT JOIN dense_series
            USING (wban, slice)
            WINDOW
                lookback AS (ORDER BY slice, datetime),
                lookforward AS (ORDER BY slice DESC, datetime DESC)
            ORDER BY slice, datetime
        )
        SELECT
            wban AS r_wban,
            slice AS r_slice,
            MIN(interpolated) as min_temp,
            MAX(interpolated) as max_temp,
            AVG(interpolated) as avg_temp
        FROM filled_series
        GROUP BY slice, wban
        ORDER BY wban, slice;
    END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
COST 1000;
```

#### Example Query ####

We can then call the function like this to build 5 Minute slices for the results:

```sql
select * from sample.interpolate_temperature('53922', '2015-03-01', '2015-03-20', '5 Minute')
```

And get the interpolated temperatures back:

<table>
    <thead>
        <th>
            <td>r_wban</td>
        </th>
        <th>
            <td>r_slice</td>
        </th>
        <th>
            <td>min_temp</td>
        </th>
        <th>
            <td>max_temp</td>
        </th>
        <th>
            <td>avg_temp</td>
        </th>
    </thead>
    <tbody>
        <tr>
            <td>53922</td>	
            <td>2015-03-01 00:50:00</td>	
            <td>-1.10000002384186</td>	
            <td>-1</td>	
            <td>-1.05000001192093</td>
        </tr>
        <tr>
            <td>53922</td>
            <td>2015-03-01 00:55:00</td>
            <td>-1.10000002384186</td>
            <td>-1.10000002384186</td>
            <td>-1.10000002384186</td>
        </tr>
        <tr>
            <td>53922</td>	
            <td>2015-03-01 01:00:00</td>	
            <td>-1.10000002384186</td>	
            <td>-1.10000002384186</td>	    
            <td>-1.10000002384186</td>
        </tr>
        <tr>
            <td>53922</td>	
            <td>2015-03-01 01:05:00</td>	
            <td>-1.10000002384186</td>	
            <td>-1.10000002384186</td>	    
            <td>-1.10000002384186</td>
        </tr>
        <tr>
            <td>53922</td>
            <td>2015-03-01 01:10:00</td>
            <td>-1.10000002384186</td>
            <td>-1.10000002384186</td>
            <td>-1.10000002384186</td>
        </tr>
        <tr>
            <td>53922</td>	
            <td>2015-03-01 01:15:00</td>	
            <td>-1.08717951102135</td>	
            <td>-1.08717951102135</td>	    
            <td>-1.08717951102135</td>
        </tr>
        <tr>
            <td>53922</td>	
            <td>2015-03-01 01:20:00</td>
            <td>-1.02307694691878</td>	
            <td>-1.02307694691878</td>
            <td>-1.02307694691878</td></tr>
        <tr>
            <td>53922</td>
            <td>2015-03-01 01:25:00</td>
            <td>-0.958974382816217</td>
            <td>-0.958974382816217</td>
            <td>-0.958974382816217</td>
        </tr>
    </tbody>
</table>

#### Query Plan #### 

The query leads to the following Query Plan (I will probably investigate if it can be optimized):

```
Sort  (cost=93788.91..93789.41 rows=200 width=548) (actual time=1214.739..1215.225 rows=5473 loops=1)
  Sort Key: filled.slice, filled.wban
  Sort Method: quicksort  Memory: 620kB
  CTE bounded
    ->  Sort  (cost=90748.69..90754.65 rows=2383 width=26) (actual time=1082.909..1082.981 rows=1067 loops=1)
          Sort Key: weather_data.datetime
          Sort Method: quicksort  Memory: 132kB
          ->  Seq Scan on weather_data  (cost=0.00..90615.02 rows=2383 width=26) (actual time=742.759..1082.495 rows=1067 loops=1)
                Filter: ((wban)::text = '53922'::text)
                Rows Removed by Filter: 4495195
  CTE dense
    ->  Function Scan on generate_series s  (cost=0.00..10.00 rows=1000 width=524) (actual time=1.579..3.347 rows=5473 loops=1)
  CTE filled
    ->  Sort  (cost=2979.12..2981.62 rows=1000 width=544) (actual time=1198.141..1198.764 rows=5515 loops=1)
          Sort Key: dense.slice, bounded.datetime
          Sort Method: quicksort  Memory: 623kB
          ->  WindowAgg  (cost=399.29..2929.29 rows=1000 width=544) (actual time=1110.224..1194.257 rows=5515 loops=1)
                ->  Sort  (cost=399.29..401.79 rows=1000 width=548) (actual time=1110.216..1110.967 rows=5515 loops=1)
                      Sort Key: dense.slice DESC, bounded.datetime DESC
                      Sort Method: quicksort  Memory: 623kB
                      ->  WindowAgg  (cost=326.96..349.46 rows=1000 width=548) (actual time=1097.938..1106.938 rows=5515 loops=1)
                            ->  Sort  (cost=326.96..329.46 rows=1000 width=536) (actual time=1097.930..1098.374 rows=5515 loops=1)
                                  Sort Key: dense.slice, bounded.datetime
                                  Sort Method: quicksort  Memory: 473kB
                                  ->  Merge Left Join  (cost=251.16..277.13 rows=1000 width=536) (actual time=1093.907..1096.604 rows=5515 loops=1)
                                        Merge Cond: (((dense.wban)::text = (bounded.wban)::text) AND (dense.slice = bounded.slice))
                                        ->  Sort  (cost=69.83..72.33 rows=1000 width=524) (actual time=10.015..10.417 rows=5473 loops=1)
                                              Sort Key: dense.wban, dense.slice
                                              Sort Method: quicksort  Memory: 449kB
                                              ->  CTE Scan on dense  (cost=0.00..20.00 rows=1000 width=524) (actual time=1.583..7.642 rows=5473 loops=1)
                                        ->  Sort  (cost=181.33..187.29 rows=2383 width=536) (actual time=1083.882..1083.927 rows=700 loops=1)
                                              Sort Key: bounded.wban, bounded.slice
                                              Sort Method: quicksort  Memory: 132kB
                                              ->  CTE Scan on bounded  (cost=0.00..47.66 rows=2383 width=536) (actual time=1082.919..1083.381 rows=1067 loops=1)
  ->  HashAggregate  (cost=32.50..35.00 rows=200 width=548) (actual time=1208.114..1210.604 rows=5473 loops=1)
        Group Key: filled.slice, filled.wban
        ->  CTE Scan on filled  (cost=0.00..20.00 rows=1000 width=532) (actual time=1198.144..1201.491 rows=5515 loops=1)
Planning time: 0.484 ms
Execution time: 1216.413 ms
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
