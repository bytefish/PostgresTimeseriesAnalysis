-- Copyright (c) Philipp Wagner. All rights reserved.
-- Licensed under the MIT license. See LICENSE file in the project root for full license information.

DO $$
  BEGIN
    -----------------------------
    -- Schema
    -----------------------------
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'sample') THEN
      CREATE SCHEMA sample;
    END IF;

    -----------------------------
    -- Tables
    -----------------------------
    IF NOT EXISTS (
    	SELECT 1
    	FROM information_schema.tables
    	WHERE  table_schema = 'sample'
    	AND table_name = 'station'
    ) THEN

    CREATE TABLE sample.station
    (
    	station_id SERIAL PRIMARY KEY,
    	wban TEXT NOT NULL,
    	name TEXT NOT NULL,
    	state TEXT,
    	location TEXT,
    	latitude REAL NOT NULL,
    	longitude REAL NOT NULL,
    	ground_height SMALLINT,
    	station_height SMALLINT,
    	TimeZone SMALLINT
    );

    END IF;

    IF NOT EXISTS (
    	SELECT 1
    	FROM information_schema.tables
    	WHERE  table_schema = 'sample'
    	AND table_name = 'weather_data'
    ) THEN

    CREATE TABLE sample.weather_data
    (
    	wban TEXT,
    	dateTime TIMESTAMP,
    	temperature REAL,
    	windSpeed REAL,
    	stationPressure REAL,
    	skyCondition TEXT
    );

    END IF;

    -----------------------------
    -- Indexes
    -----------------------------
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uk_station_wban') THEN
    	ALTER TABLE sample.station
    		ADD CONSTRAINT uk_station_wban
    		UNIQUE (wban);
    END IF;

    -----------------------------
    -- Security
    -----------------------------
    REVOKE ALL ON sample.station FROM public;
    REVOKE ALL ON sample.weather_data FROM public;

  END;
$$;

-----------------------------
-- Functions
-----------------------------
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

CREATE OR REPLACE FUNCTION sample.datediff_seconds(start_t TIMESTAMP, end_t TIMESTAMP)
RETURNS INT AS $$
    SELECT EXTRACT(epoch FROM $2 - $1) 
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sample.timestamp_to_seconds(timestamp_t TIMESTAMP)
RETURNS DOUBLE PRECISION AS $$
    SELECT EXTRACT(epoch from timestamp_t)
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i DOUBLE PRECISION, 
    x_0 DOUBLE PRECISION, 
    y_0 DOUBLE PRECISION, 
    x_1 DOUBLE PRECISION, 
    y_1 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
    SELECT (($5 - $3) / ($4 - $2)) * ($1 - $2) + $3;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION sample.linear_interpolate(x_i TIMESTAMP, x_0 TIMESTAMP, y_0 DOUBLE PRECISION, x_1 TIMESTAMP, y_1 DOUBLE PRECISION)
RETURNS DOUBLE PRECISION AS $$
    SELECT sample.linear_interpolate(sample.timestamp_to_seconds($1), 
        sample.timestamp_to_seconds($2), 
        $3, 
        sample.timestamp_to_seconds($4),
        $5);
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION sample.interpolate_temperature(wban_p TEXT, start_t TIMESTAMP, end_t TIMESTAMP, slice_t INTERVAL)
RETURNS TABLE(
    r_wban TEXT,
    r_slice TIMESTAMP,
    min_temp DOUBLE PRECISION,
    max_temp DOUBLE PRECISION,
    avg_temp DOUBLE PRECISION
) AS $$
    -- bounded_series assigns all values into a time slice with a given interval length in slice_t:
    WITH bounded_series AS (
      SELECT wban,
             datetime,
             'epoch'::timestamp + $4 * (extract(epoch from datetime)::int4 / EXTRACT(epoch FROM $4)::int4) AS slice,
             temperature
      FROM sample.weather_data w
      WHERE w.wban = $1
      ORDER BY wban, slice, datetime ASC
    ),
    -- dense_series uses generate_series to generate the intervals we expect in the data:
    dense_series AS (
      SELECT $1 as wban, slice
      FROM generate_series($2, $3, $4)  s(slice)
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
    
$$ LANGUAGE SQL;