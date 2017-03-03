DO $$
BEGIN

REVOKE ALL ON sample.station FROM public;
REVOKE ALL ON sample.weather_data FROM public;

END;
$$;