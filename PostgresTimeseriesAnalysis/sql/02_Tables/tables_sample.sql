DO $$
BEGIN

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

END;
$$;