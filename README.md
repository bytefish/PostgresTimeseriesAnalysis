# PostgresTimeseriesAnalysis #

There was an interesting article by the [jOOQ] team on how to fill gaps in data using SQL:

* [Using IGNORE NULLS With SQL Window Functions to Fill Gaps]

I have been experimenting with using SQL for Linear Interpolation two years ago, and the whole code 
is heavily based on a great article by [Caleb Welton](https://github.com/cwelton):

* [Time Series Analysis Part 3: Resampling and Interpolation]

It would be a waste to silo all this in a private repository, so I will share it. 

To reproduce the example, please see the section [How to Reproduce this Experiment](#how-to-reproduce-this-experiment).

This article can also be found on my personal website:

* [https://bytefish.de/blog/postgresql_interpolation/](https://bytefish.de/blog/postgresql_interpolation/)

## Dataset ##

The dataset is the [Quality Controlled Local Climatological Data (QCLCD)] for 2014 and 2015. It contains hourly weather 
measurements for more than 1,600 US Weather Stations. It is a great dataset to learn about data processing and data 
visualization:

> The Quality Controlled Local Climatological Data (QCLCD) consist of hourly, daily, and monthly summaries for approximately 
> 1,600 U.S. locations. Daily Summary forms are not available for all stations. Data are available beginning January 1, 2005 
> and continue to the present. Please note, there may be a 48-hour lag in the availability of the most recent data.

The data is available as CSV files at:

* [http://www.ncdc.noaa.gov/orders/qclcd/](http://www.ncdc.noaa.gov/orders/qclcd/)

Download the file ``QCLCD201503.zip`` from:

* [http://www.ncdc.noaa.gov/orders/qclcd/](http://www.ncdc.noaa.gov/orders/qclcd/)

[Quality Controlled Local Climatological Data (QCLCD)]: 

## Are there missing values? ##

Devices might break. Networks can be down. Disks still run full in 2019. The sky is the limit, when it comes to invalid or missing measurements in data.

The [Quality Controlled Local Climatological Data (QCLCD)] has hourly measurements of weather stations. So to find missing data we will look for gaps in data greater than 1 hour. 

How can we do this? [Window Functions]! 

[Window Functions] can be hard to grasp and I can't go into all details here. The best introduction to [Window Functions] was written by [Dimitri Fontaine] and I highly recommend reading it:

* [Understanding Window Functions] ([Dimitri Fontaine])

To identify gaps we first need a way calculate the interval between two timestamps, so I will 
define a function ``datediff_seconds`` to calculate the length between two timestamp values:

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

Now we can use the [LAG] operator to identity gaps larger 3600 seconds, which is an hour:

```sql
SELECT  *
FROM (SELECT 
        weather_data.wban as wban, 
        weather_data.datetime as current_datetime,                 
        LAG(weather_data.datetime, 1, NULL) OVER (PARTITION BY weather_data.wban ORDER BY weather_data.datetime) AS previous_datetime
     FROM sample.weather_data) lag_select
WHERE sample.datediff_seconds (previous_datetime, current_datetime) > 3600;
```

And we can see there are 17,043 affected rows, which is the number of gaps in the data:

```
Successfully run. Total query runtime: 33 secs 590 msec.
17043 rows affected.
```

## Linear Interpolation with SQL ##

First of all we write a function to do a [Linear Interpolation] between two points:

```sql
CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i INTEGER, x_0 INTEGER, y_0 DOUBLE PRECISION, x_1 int, y_1 DOUBLE PRECISION)
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

We are working with the ``TIMESTAMP`` datatype, so in order to put it into the ``linear_interpolate`` function, 
we need to transform the ``TIMESTAMP`` into its representation of seconds since epoch:

```sql
CREATE OR REPLACE FUNCTION sample.timestamp_to_seconds(timestamp_t TIMESTAMP)
RETURNS INT AS $$
  DECLARE
    seconds INT = 0;
  BEGIN
    seconds = extract('epoch' from timestamp_t);

    RETURN seconds;
  END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
COST 1000;
```

This makes it possible to write an overload, that takes the timestamps and returns the interpolated value of a given timestamp ``x_i``:

```sql
CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i TIMESTAMP, x_0 TIMESTAMP, y_0 DOUBLE PRECISION, x_1 TIMESTAMP, y_1 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
  BEGIN
   	RETURN sample.linear_interpolate(
   	  sample.timestamp_to_seconds(x_i),
      sample.timestamp_to_seconds(x_0),
      y_0,
      sample.timestamp_to_seconds(x_1),
      y_1);
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
COST 1000;
```

And that's it?

## Linear Interpolation of the QCLCD Weather Data ##

As a final example I want to show how to use the functions to interpolate the sample weather data, which had 17,043 missing measurements. 

The idea is quite simple: First of all we will put all measurements into a time slice of a given interval length. So we know, that we have 
a value for the expected point in time. We will then build a dense series using the [generate_series] method with the given ``slice_t`` interval, which has all the slices we expect.

The ``bounded_series`` and ``dense_series`` will then be joined, which means: The joined series will have ``NULL`` for the measurements, which indicates the slice has to be interpolated. A window function will be used to identify the first and last non-null value of a value, so we get the two points for the ``linear_interpolate``function.

To make this work we need to ignore NULL values, just like in the [jOOQ] article. The PostgreSQL Wiki has a great article on it, which shows how to implement such a function with a PostgreSQL AGGREGATE:

* [https://wiki.postgresql.org/wiki/First/last_(aggregate)](https://wiki.postgresql.org/wiki/First/last_(aggregate))

I simple copy and pasted it and appended the Schema:

```sql
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

And finally we can write the function to interpolate the measurements:

```sql
CREATE OR REPLACE FUNCTION sample.interpolate_temperature(wban_p TEXT, start_t TIMESTAMP, end_t TIMESTAMP, slice_t INTERVAL)
RETURNS TABLE(
  r_wban TEXT,
  r_slice TIMESTAMP,
  min_temp DOUBLE PRECISION,
  max_temp DOUBLE PRECISION,
  avg_temp DOUBLE PRECISION
) AS $$
  DECLARE
    slice_seconds INT = 0;
  BEGIN

    slice_seconds = EXTRACT(epoch FROM slice_t)::int4;
    
    RETURN QUERY
    -- bounded_series assigns all values into a time slice with a given interval length in slice_t:
    WITH bounded_series AS (
      SELECT wban,
             datetime,
             'epoch'::timestamp + slice_t * (extract(epoch from datetime)::int4 / slice_seconds) AS slice,
             temperature
      FROM sample.weather_data w
      WHERE w.wban = wban_p
      ORDER BY wban, slice, datetime ASC
    ),
    -- dense_series uses generate_series to generate the intervals we expect in the data:
    dense_series AS (
      SELECT wban_p as wban, slice
      FROM generate_series(start_t, end_t, slice_t)  s(slice)
      ORDER BY wban, slice
    ),
    -- filled_series now uses a WINDOW function for find the first / last not null
    -- value in a WINDOW and uses sample.linear_interpolate to interpolate the slices
    -- between both values.
    --
    -- Finally we have to GROUP BY the slice and wban and take the AVG, MIN and MAX
    -- value in the slice. You can also add more Operators there, it is just an
    -- example:
    filled_series AS (
      SELECT wban,
             slice,
             temperature,
             COALESCE(temperature, sample.linear_interpolate(slice,
               sample.last(datetime) over (lookback),
               sample.last(temperature) over (lookback),
               sample.last(datetime) over (lookforward),
               sample.last(temperature) over (lookforward))) interpolated
      FROM bounded_series
        RIGHT JOIN dense_series USING (wban, slice)
      WINDOW
        lookback AS (ORDER BY slice, datetime),
        lookforward AS (ORDER BY slice DESC, datetime DESC)
       ORDER BY slice, datetime)
    SELECT wban AS r_wban,
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

With the function we can now interpolate the temperature for a given station with any interval:

```sql
SELECT * FROM sample.interpolate_temperature('00102', '2015-03-23', '2015-03-30', '1 hour'::interval)
```

And that's it!

## How to Reproduce this Experiment ##

The was a highly interesting article on the [Machine Learning Reproducibility crisis] lately, which discussed the 
problem of reproducing the results of Machine Learning papers. It's something I also felt long time ago, that's 
why you will always be able to reproduce the examples I share in this blog.

It's probably best to add a section on how to reproduce this article and use the example.

### Database ###

#### Creating a User ####

Create the user ``philipp`` for connecting to the databases:

```
postgres=# CREATE USER philipp WITH PASSWORD 'test_pwd';
CREATE ROLE
```

Then we can create the test database ``sampledb`` and set the owner to ``philipp``:

```
postgres=# CREATE DATABASE sampledb WITH OWNER philipp; 
```

#### Creating the Database ####

There are two scripts to create the database in the following folder of the project:

* [PostgresTimeseriesAnalysis/sql]

To create the database execute the ``create_database.bat`` (Windows) or ``create_database.sh`` (Linux).

Alternatively you can simply copy and paste [10_create_database.sql] and [20_sample_data.sql] into an editor of your choice and execute it.

[PostgresTimeseriesAnalysis/sql]: https://github.com/bytefish/PostgresTimeseriesAnalysis/tree/master/PostgresTimeseriesAnalysis/sql
[10_create_database.sql]: https://github.com/bytefish/PostgresTimeseriesAnalysis/blob/master/PostgresTimeseriesAnalysis/sql/sql/10_create_database.sql
[20_sample_data.sql]: https://github.com/bytefish/PostgresTimeseriesAnalysis/blob/master/PostgresTimeseriesAnalysis/sql/sql/20_sample_data.sql

#### Enable PostgreSQL Statistics ####

Find out which ``postgresql.config`` is currently loaded:

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

#### Enable Parallel Queries ####

Find out, which ``postgresql.config`` is currently loaded:

```sql
-- Show the currently used config file:
SHOW config_file;
```

Then set the parameters ``max_worker_processes``and ``max_parallel_workers_per_gather``:

```
max_worker_processes = 8		# (change requires restart)
max_parallel_workers_per_gather = 4	# taken from max_worker_processes
```

### Dataset ###

The dataset is the [Quality Controlled Local Climatological Data (QCLCD)] for 2014 and 2015. It contains hourly weather 
measurements for more than 1,600 US Weather Stations. It is a great dataset to learn about data processing and data 
visualization:

> The Quality Controlled Local Climatological Data (QCLCD) consist of hourly, daily, and monthly summaries for approximately 
> 1,600 U.S. locations. Daily Summary forms are not available for all stations. Data are available beginning January 1, 2005 
> and continue to the present. Please note, there may be a 48-hour lag in the availability of the most recent data.

The data is available as CSV files at:

* [http://www.ncdc.noaa.gov/orders/qclcd/](http://www.ncdc.noaa.gov/orders/qclcd/)

Download the file ``QCLCD201503.zip`` from:

* [http://www.ncdc.noaa.gov/orders/qclcd/](http://www.ncdc.noaa.gov/orders/qclcd/)

### Application ###

The application is a Java application, which can be started with an IDE of your choice:

* [WeatherDataStreamingExample.java]

You probably need to adjust the connection string to the database:

```java
private static final String databaseUri = "jdbc:postgresql://127.0.0.1:5432/sampledb?user=philipp&password=test_pwd";
```

And change the path to the CSV files, if the path differs: 

```java
final Path csvStationDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503station.txt");
final Path csvLocalWeatherDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503hourly.txt");
```

Once executed the application parses the CSV files and writes the data into the specified database.

[WeatherDataStreamingExample.java]: https://github.com/bytefish/PostgresTimeseriesAnalysis/blob/master/PostgresTimeseriesAnalysis/src/main/java/app/WeatherDataStreamingExample.java
[jOOQ]: https://www.jooq.org/
[Using IGNORE NULLS With SQL Window Functions to Fill Gaps]: https://blog.jooq.org/2019/04/24/using-ignore-nulls-with-sql-window-functions-to-fill-gaps/
[Time Series Analysis Part 3: Resampling and Interpolation]: https://content.pivotal.io/blog/time-series-analysis-part-3-resampling-and-interpolation
[Machine Learning Reproducibility crisis]: https://towardsdatascience.com/why-git-and-git-lfs-is-not-enough-to-solve-the-machine-learning-reproducibility-crisis-f733b49e96e8
[generate_series]: https://www.postgresql.org/docs/current/functions-srf.html
[Linear Interpolation]: https://en.wikipedia.org/wiki/Linear_interpolation
[Window Functions]: https://www.postgresql.org/docs/current/functions-window.html
[Understanding Window Functions]: https://tapoueh.org/blog/2013/08/understanding-window-functions/
[Dimitri Fontaine]: https://tapoueh.org
[LAG]: https://docs.microsoft.com/en-us/sql/t-sql/functions/lag-transact-sql
[Quality Controlled Local Climatological Data (QCLCD)]: https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/quality-controlled-local-climatological-data-qclcd

## Additional Resources ##

* http://tapoueh.org/blog/2013/08/20-Window-Functions
* https://www.compose.com/articles/metrics-maven-window-functions-in-postgresql/
* https://wiki.postgresql.org/images/a/a2/PostgreSQL_Window_Functions.pdf
* http://blog.cleverelephant.ca/2016/03/parallel-postgis.html

### Resampling and Interpolation ###

* https://content.pivotal.io/blog/time-series-analysis-part-3-resampling-and-interpolation


[PostgreSQL]: https://www.postgresql.org
[Quality Controlled Local Climatological Data (QCLCD)]: https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/quality-controlled-local-climatological-data-qclcd
