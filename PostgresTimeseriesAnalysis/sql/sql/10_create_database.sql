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
	wban VARCHAR(255) NOT NULL,
	name VARCHAR(255) NOT NULL,
	state VARCHAR(255), 
	location VARCHAR(255),
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
	wban VARCHAR(255),
	dateTime TIMESTAMP,
	temperature REAL,
	windSpeed REAL,
	stationPressure REAL,
	skyCondition VARCHAR(255)	
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

END
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

CREATE OR REPLACE FUNCTION sample.timestamp_to_seconds(start_t TIMESTAMP, end_t TIMESTAMP) 
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

    m = (y_1 - y_0) / (x_1 - x_0);
    n = y_0;
    x = (x_i - x_0);
    
    RETURN (m * x + n);
   END;
   $$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
   COST 1000;