DO $$
BEGIN

IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uk_station_wban') THEN
	ALTER TABLE sample.station
		ADD CONSTRAINT uk_station_wban
		UNIQUE (wban);
END IF;

END;
$$;