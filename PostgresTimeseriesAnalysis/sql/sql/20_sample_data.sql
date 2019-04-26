DO $$
BEGIN

IF EXISTS (
	SELECT 1 
	FROM information_schema.tables 
	WHERE  table_schema = 'sample' 
	AND table_name = 'station'
) THEN


INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00100', 'ARKADELPHIA', 'AR', 'DEXTER B FLORENCE MEM FLD AP', 34.09972, -93.06583, 182, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00100');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00101', 'BISHKEK', 'NULL', 'MANAS INTERNATIONAL AIRPORT', 43.067, 74.483, 2090, NULL, 6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00101');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00102', 'KIANA', 'AK', 'BOB BARKER MEMORIAL AIRPORT', 66.983, -160.433, 168, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00102');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00103', 'WALES', 'AK', 'WALES AIRPORT', 65.617, -168.1, 23, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00103');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00104', 'NIKOLAI', 'AK', 'NIKOLAI AIRPORT', 63.017, -154.367, 414, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00104');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00105', 'MARSHALL', 'AK', 'MARSHALL DON HUNTER SR AIRPORT', 61.867, -162.033, 102, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00105');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00106', 'AKIAK', 'AK', 'KIPNUK AIRPORT', 60.9, -161.233, 30, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00106');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00107', 'NEW STUYAHOK', 'AK', 'NEW STUYAHOK AIRPORT', 59.45, -157.333, 302, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00107');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00108', 'SCAMMON BAY', 'AK', 'SCAMMON BAY AIRPORT', 61.85, -165.567, 14, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00108');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00109', 'MANOKOTAK', 'AK', 'MANOKOTAK AIRPORT', 58.983, -159.05, 52, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00109');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00110', 'NELSON LAGOON', 'AK', 'NELSON LAGOON AIRPORT', 56.017, -161.167, 14, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00110');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00111', 'SHAKTOOLIK', 'AK', 'SHAKTOOLIK AIRPORT', 64.37, -161.224, 23, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00111');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00112', 'KOLIGANEK', 'AK', 'KOLIGANEK AIRPORT', 59.733, -157.267, 270, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00112');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00113', 'NOORVIK', 'AK', 'ROBERT (BOB) CURTIS MEMORIAL AIRPORT', 66.818, -161.022, 56, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00113');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00114', 'STARR SCHOOL', 'MT', 'STARR BROWNING AIRSTRIP', 48.6, -113.117, 4656, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00114');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00115', 'BIG BEAR', 'CA', 'BIG BEAR CITY AIRPORT', 34.264, -116.854, 6749, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00115');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00116', 'WASHINGTON', 'MO', 'WASHINGTON REGIONAL AIRPORT', 38.583, -91, 489, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00116');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00117', 'WEAVERVILLE', 'CA', 'LONNIE POOL FIELD WEAVERVILLE AIRPORT', 40.747, -122.922, 2350, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00117');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00118', 'TILLAMOOK', 'OR', 'TILLAMOOK AIRPORT', 45.417, -123.817, 37, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00118');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00119', 'CHEHALIS', 'WA', 'CHEHALIS CENTRALIA AIRPORT', 46.683, -122.983, 178, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00119');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00120', 'CLAYPOOL HILL', 'VA', 'TAZEWELL COUNTY AIRPORT', 37.067, -81.8, 2651, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00120');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00121', 'VALENCIA WEST', 'AZ', 'RYAN FIELD AIRPORT', 32.15, -111.167, 2418, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00121');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00122', 'NEW CASTLE', 'PA', 'NEW CASTLE MUNICIPAL AIRPORT', 41.033, -80.417, 1073, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00122');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00123', 'ST MICHAEL', 'AK', 'ST MICHAEL AIRPORT', 63.49, -162.11, 92, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00123');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00124', 'CHESTER', 'MD', 'BAY BRIDGE AIRPORT', 38.967, -76.333, 17, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00124');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00125', 'WILLMAR', 'MN', 'WILLMAR MUNICIPAL AIRPORT JOHN L RICE FIELD', 45.117, -95.133, 1129, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00125');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00126', 'CALIFORNIA', 'MD', 'ST MARYS COUNTY REGIONAL AIRPORT', 38.317, -76.55, 142, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00126');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00127', 'BEEVILLE', 'TX', 'BEEVILLE MUNICIPAL AIRPORT', 28.35, -97.717, 270, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00127');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00128', 'HOMERVILLE', 'GA', 'HOMERVILLE AIRPORT', 31.056, -82.767, 188, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00128');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00129', 'HEREFORD', 'TX', 'HEREFORD MUNICIPAL AIRPORT', 34.85, -102.333, 3783, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00130', 'PLEASANTON', 'TX', 'PLEASANTON MUNICIPAL AIRPORT', 28.95, -98.517, 430, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00130');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00131', 'ALBANY', 'OH', 'OHIO UNIVERSITY AIRPORT SNYDER FIELD', 39.217, -82.233, 765, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00132', 'WAUTOMA', 'WI', 'WAUTOMA MUNICIPAL AIRPORT', 44.033, -89.3, 860, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00132');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00133', 'CLINTON', 'AR', 'CLINTON MUNICIPAL AIRPORT', 35.6, -92.45, 516, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00133');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00134', 'WICHITA FALLS', 'TX', 'KICKAPOO DOWNTOWN AIRPORT', 33.85, -98.483, 998, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00134');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00135', 'NOVATO', 'CA', 'GNOSS FIELD AIRPORT', 38.15, -122.55, 4, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00135');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00136', 'COATESVILLE', 'PA', 'CHESTER COUNTY GO CARLSON AIRPORT', 39.983, -75.867, 660, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00137', 'ANGEL FIRE', 'NM', 'ANGEL FIRE AIRPORT', 36.422, -105.29, 8380, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00137');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00138', 'HALEYVILLE', 'AL', 'POSEY FIELD AIRPORT', 34.267, -87.6, 932, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00138');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00139', 'MT VERNON', 'OH', 'KNOX COUNTY AIRPORT', 40.333, -82.517, 1191, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00139');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00140', 'FREMONT', 'MI', 'FREMONT MUNICIPAL AIRPORT', 43.433, -86, 831, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00140');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00141', 'INDEPENDENCE', 'KS', 'INDEPENDENT MUNICIPAL AIRPORT', 37.158, -95.778, 824, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00142', 'FRONTENAC', 'KS', 'ATKINSON MUNICPAL AIRPORT', 37.45, -94.733, 952, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00142');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00143', 'ST FRANCIS', 'KS', 'CHEYENNE COUNTY MUNICIPAL AIRPORT', 39.767, -101.8, 3413, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00143');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00144', 'JUNCTION CITY', 'KY', 'STUART POWELL FIELD AIRPORT', 37.578, -84.77, 1024, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00144');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00145', 'BEREA', 'KY', 'MADISON AIRPORT', 37.633, -84.333, 1001, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00145');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00146', 'MT STERLING', 'KY', 'MOUNT STERLING MONTGOMERY COUNTY AIRPORT', 38.067, -83.983, 1021, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00146');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00148', 'BOGALUSA', 'LA', 'GEORGE R CARR MEMORIAL AIR FIELD', 30.817, -89.867, 120, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00148');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00149', 'MONETT', 'MO', 'MONETT MUNICIPAL AIRPORT', 36.9, -94.017, 1316, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00149');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00150', 'GWINNER', 'ND', 'GWINNER ROGER MELROE FIELD AIRPORT', 46.217, -97.633, 1267, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00150');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00151', 'LYNDON', 'VT', 'CALEDONIA COUNTY AIRPORT', 44.567, -72.017, 1188, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00151');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00152', 'SWANTON', 'VT', 'FRANKLIN COUNTY STATE AIRPORT', 44.933, -73.1, 230, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00152');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00153', 'CHESTERFIELD', 'VA', 'CHESTERFIELD COUNTY AIRPORT', 37.4, -77.517, 237, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00153');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00154', 'CHESAPEAKE', 'VA', 'HAMPTON ROADS EXECUTIVE AIRPORT', 36.783, -76.45, 23, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00154');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00155', 'TAPPAHANNOCK', 'VA', 'TAPPAHANNOCK ESSEX COUNTY AIRPORT', 37.85, -76.883, 135, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00155');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00156', 'JASPER', 'AL', 'WALKER COUNTY AIRPORT BEVILL FIELD', 33.902, -87.314, 483, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00156');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00157', 'WALDEN', 'CO', 'WALDEN JACKSON COUNTY AIRPORT', 40.75, -106.267, 8157, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00157');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00158', 'DELTA', 'CO', 'BLAKE FIELD AIRPORT', 38.783, -108.067, 5194, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00158');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00159', 'SALIDA', 'CO', 'HARRIET ALEXANDER FIELD AIRPORT', 38.533, -106.05, 7527, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00159');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00160', 'VALMONT', 'CO', 'BOULDER MUNICIPAL AIRPORT', 40.033, -105.217, 5289, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00160');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00161', 'ERIE', 'CO', 'ERIE MUNICIPAL AIRPORT', 40.017, -105.05, 5132, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00161');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00162', 'FORT MORGAN', 'CO', 'FORT MORGAN MUNICIPAL AIRPORT', 40.333, -103.8, 4571, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00162');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00163', 'HOLYOKE', 'CO', 'HOLYOKE AIRPORT', 40.567, -102.267, 3731, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00163');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00164', 'LONGMONT', 'CO', 'VANCE BRAND AIRPORT', 40.167, -105.167, 5056, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00164');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00165', 'PAGOSA SPRINGS', 'CO', 'STEVENS FIELD AIRPORT', 37.283, -107.05, 7661, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00165');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00166', 'LAGO VISTA', 'TX', 'LAGO VISTA RUSTY ALLEN AIRPORT', 30.5, -97.967, 1231, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00166');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00167', 'AUBURN', 'IN', 'DE KALB COUNTY AIRPORT', 41.3, -85.067, 880, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00167');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00168', 'STERLING', 'CO', 'STERLING MUNICIPAL AIRPORT', 40.617, -103.267, 4039, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00168');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00169', 'CHESTER', 'CT', 'CHESTER AIRPORT', 41.384, -72.506, 417, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00169');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00170', 'EUREKA', 'NV', 'EUREKA AIRPORT', 39.6, -116, 5955, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00170');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00171', 'CARSON CITY', 'NV', 'CARSON AIRPORT', 39.183, -119.733, 4699, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00171');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00172', 'HUGO', 'OK', 'STAN STAMPER MUNICIPAL AIRPORT', 34.033, -95.533, 571, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00172');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00173', 'CARMEL', 'IN', 'INDIANAPOLIS EXECUTIVE AIRPORT', 40.031, -86.251, 922, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00173');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00174', 'DAVIS', 'CA', 'UNIVERSITY AIRPORT', 38.533, -121.783, 69, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00174');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00175', 'HENDERSON', 'TX', 'RUSK COUNTY AIRPORT', 32.15, -94.85, 443, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00175');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00176', 'BRECKENRIDGE', 'TX', 'STEPHENS COUNTY AIRPORT', 32.717, -98.892, 1283, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00176');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00177', 'CASTROVILLE', 'TX', 'CASTROVILLE MUNICIPAL AIRPORT', 29.85, -98.85, 771, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00177');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00178', 'LINDEN', 'NJ', 'LINDEN AIRPORT', 40.617, -74.25, 23, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00178');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00179', 'LONG PRAIRIE', 'MN', 'TODD FIELD AIRPORT', 45.9, -94.867, 1333, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00179');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00180', 'LAKE ELMO', 'MN', 'LAKE ELMO AIRPORT', 45, -92.85, 932, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00180');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00181', 'ZAPATA', 'TX', 'ZAPATA COUNTY AIRPORT', 26.967, -99.25, 424, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00181');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00182', 'ELBOW LAKE', 'MN', 'ELBOW LAKE MUNICIPAL PRIDE OF THE PRAIRIE AIRPORT', 45.986, -95.992, 1205, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00182');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00183', 'PLATTEVILLE', 'WI', 'PLATTEVILLE MUNICIPAL AIRPORT', 42.683, -90.45, 1024, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00183');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00184', 'ABBEVILLE', 'LA', 'ABBEVILLE CHRIS CRUSTA MEMORIAL AIRPORT', 29.976, -92.084, 50, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00184');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00185', 'SHAWANO', 'WI', 'SHAWANO MUNICIPAL AIRPORT', 44.783, -88.55, 814, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00185');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00186', 'FAIRHOPE', 'AL', 'H L SONNY CALLAHAN AIRPORT', 30.46, -87.877, 92, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00186');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00187', 'KNOX', 'IN', 'STARKE COUNTY AIRPORT', 41.333, -86.667, 683, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00187');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00188', 'HOUSTON', 'TX', 'HOUSTON DUNN HELISTOP', 29.717, -95.383, 227, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00188');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00189', 'NEWBERRY', 'SC', 'NEWBERRY COUNTY AIRPORT', 34.3, -81.633, 568, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00189');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00190', 'LAURENS', 'SC', 'LAURENS COUNTY AIRPORT', 34.5, -81.95, 696, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00190');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00191', 'MONCKS CORNER', 'SC', 'BERKELEY COUNTY AIRPORT', 33.183, -80.033, 73, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00191');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00192', 'CHESTER', 'SC', 'CHESTER CATAWBA REGIONAL AIRPORT', 34.783, -81.2, 657, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00192');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00193', 'AIKEN', 'SC', 'AIKEN MUNICIPAL AIRPORT', 33.65, -81.683, 529, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00193');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00194', 'BARNWELL', 'SC', 'BARNWELL REGIONAL AIRPORT', 33.25, -81.383, 247, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00194');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00195', 'CAMDEN', 'SC', 'WOODWARD FIELD AIRPORT', 34.283, -80.567, 302, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00195');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00196', 'KINGSTREE', 'SC', 'WILLIAMSBURG REGIONAL AIRPORT', 33.717, -79.85, 66, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00196');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00197', 'CHERAW', 'SC', 'CHERAW MUNICIPAL LYNCH BELLINGER FIELD', 34.717, -79.95, 240, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00197');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00198', 'CHARLESTON', 'SC', 'CHARLESTON EXECUTIVE AIRPORT', 32.701, -80.003, 17, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00198');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00199', 'LANCASTER', 'SC', 'LANCASTER COUNTY MC WHIRTER FIELD AIRPORT', 34.717, -80.85, 486, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00199');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00200', 'MARION', 'SC', 'MARION COUNTY AIRPORT', 34.181, -79.335, 92, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00200');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00201', 'WALTERBORO', 'SC', 'LOWCOUNTRY REGIONAL AIRPORT', 32.917, -80.633, 102, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00201');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00202', 'SUMTER', 'SC', 'SUMTER AIRPORT', 34, -80.367, 181, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00202');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00203', 'BEAUFORT', 'SC', 'BEAUFORT COUNTY AIRPORT', 32.4121, -80.6343, 125, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00203');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00204', 'CONWAY', 'SC', 'CONWAY HORRY COUNTY AIRPORT', 33.828, -79.122, 33, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00204');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00205', 'LINCOLN', 'CA', 'LINCOLN REGIONAL AIRPORT KARL HARDER FIELD', 38.909, -121.351, 122, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00205');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00206', 'COLUMBIA', 'CA', 'COLUMBIA AIRPORT', 38.033, -120.417, 2120, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00206');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00207', 'BRANSON', 'MO', 'BRANSON AIRPORT', 36.533, -93.2, 1303, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00207');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00208', 'BROOKSHIRE', 'TX', 'HOUSTON EXECUTIVE AIRPORT', 29.8, -95.9, 168, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00208');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00209', 'BOWIE', 'TX', 'BOWIE MUNICIPAL AIRPORT', 33.6, -97.783, 1103, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00209');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00210', 'ELIZABETHTON', 'TN', 'ELIZABETHTON MUNICIPAL AIRPORT', 36.367, -82.167, 1595, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00210');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00211', 'MADISON', 'SD', 'MADISON MUNICIPAL AIRPORT', 44.016, -97.086, 1716, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00211');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00212', 'BLACKWELL', 'OK', 'BLACKWELL TONKAWA MUNICIPAL AIRPORT', 36.75, -97.35, 1031, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00212');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00213', 'PAMPA', 'TX', 'PAMPA MESA VISTA AIRPORT', 35.883, -101.033, 2773, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00213');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00214', 'STIGLER', 'OK', 'STIGLER REGIONAL AIRPORT', 35.283, -95.1, 601, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00214');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00215', 'YUKON', 'OK', 'CLARENCE E PAGE MUNICIPAL AIRPORT', 35.483, -97.817, 1355, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00215');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00216', 'MIDLAND', 'MI', 'JACK BARSTOW AIRPORT', 43.663, -84.261, 637, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00216');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00217', 'BENNETTSVILLE', 'SC', 'MARLBORO COUNTY JETPORT H E AVENT FIELD', 34.617, -79.733, 148, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00217');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00218', 'SUMMERVILLE', 'SC', 'SUMMERVILLE AIRPORT', 33.063, -80.279, 56, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00218');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00219', 'HARTSVILLE', 'SC', 'HARTSVILLE REGIONAL AIRPORT', 34.4, -80.117, 365, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00219');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00220', 'WINNSBORO', 'SC', 'FAIRFIELD COUNTY AIRPORT', 34.315, -81.109, 578, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00220');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00221', 'WILLIAMS', 'AZ', 'H A CLARK MEMORIAL FIELD AIRPORT', 35.3, -112.2, 6677, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00221');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00222', 'CLINTON', 'MO', 'CLINTON MEMORIAL AIRPORT', 38.35, -93.683, 824, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00222');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00223', 'ARCOLA', 'TX', 'HOUSTON SOUTHWEST AIRPORT', 29.5, -95.477, 69, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00223');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00224', 'BEND', 'OR', 'BEND MUNICIPAL AIRPORT', 44.095, -121.2, 3462, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00224');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00225', 'HORSESHOE BAY', 'TX', 'HORSESHOE BAY RESORT AIRPORT', 30.533, -98.367, 1093, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00225');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00226', 'BUCKEYE', 'AZ', 'BUCKEYE MUNICIPAL AIRPORT', 33.417, -112.683, 1021, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00226');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00227', 'PLACERVILLE', 'CA', 'PLACERVILLE AIRPORT', 38.717, -120.75, 2583, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00227');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00228', 'HALF MOON BAY', 'CA', 'HALF MOON BAY AIRPORT', 37.513, -122.501, 66, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00228');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00229', 'LAMPASAS', 'TX', 'LAMPASAS AIRPORT', 31.106, -98.196, 1214, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00229');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00230', 'PFLUGERVILLE', 'TX', 'AUSTIN EXECUTIVE AIRPORT', 30.395, -97.567, 617, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00230');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00231', 'HALIFAX', 'NC', 'HALIFAX NORTHAMPTON REGIONAL AIRPORT', 36.33, -77.635, 145, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00231');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00232', 'MARYSVILLE', 'OH', 'UNION COUNTY AIRPORT', 40.225, -83.352, 1021, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00232');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00234', 'CORNING', 'AR', 'CORNING MUNICIPAL AIRPORT', 36.4, -90.65, 292, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00234');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00235', 'CRYSTAL RIVER', 'FL', 'CRYSTAL RIVER AIRPORT', 28.867, -82.567, 10, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00235');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00236', 'MIDDLETON', 'WI', 'MIDDLETON MUNICIPAL MOREY FIELD AIRPORT', 43.117, -89.533, 929, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00236');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00237', 'HINCKLEY', 'MN', 'FIELD OF DREAMS AIRPORT', 46.023, -92.895, 1021, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00237');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00238', 'SAUK CENTRE', 'MN', 'SAUK CENTRE MUNICIPAL AIRPORT', 45.707, -94.933, 1244, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00238');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00239', 'OZONA', 'TX', 'OZONA MUNICIPAL AIRPORT', 30.735, -101.203, 2382, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00239');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00240', 'IONIA', 'MI', 'IONIA COUNTY AIRPORT', 42.938, -85.061, 817, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00240');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00241', 'VIROQUA', 'WI', 'VIROQUA MUNICIPAL AIRPORT', 43.579, -90.913, 1293, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00241');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00242', 'INDEPENDENCE OIL PLATFORM', 'LA', 'INDEPENDENCE OIL PLATFORM', 28.083, -87.983, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00242');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00243', 'MAGNOLIA OIL PLATFORM', 'LA', 'MAGNOLIA OIL PLATFORM', 27.2, -92.2, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00243');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00244', 'SHIP SHOAL OIL PLATFORM', 'LA', 'SHIP SHOAL OIL PLATFORM', 28.6, -91.2, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00244');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00245', 'INNOVATOR OIL PLATFORM', 'MS', 'INNOVATOR OIL PLATFORM', 28.217, -89.617, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00245');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00246', 'EAST BREAKS OIL PLATFORM', 'TX', 'EAST BREAKS OIL PLATFORM', 27.817, -94.317, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00246');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00247', 'GUNNISON OIL PLATFORM', 'TX', 'GUNNISON OIL PLATFORM', 27.3, -93.533, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00247');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00248', 'JESUP', 'GA', 'JESUP WAYNE COUNTY AIRPORT', 31.554, -81.883, 109, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00248');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00249', 'MT ZION', 'GA', 'WEST GEORGIA REGIONAL AIRPORT O V GRAY FIELD', 33.633, -85.15, 1162, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00249');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00250', 'UNIONVILLE', 'GA', 'HENRY TIFT MYERS AIRPORT', 31.429, -83.489, 355, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00250');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00251', 'VERSAILLES', 'OH', 'DARKE COUNTY AIRPORT', 40.204, -84.532, 1008, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00251');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00252', 'DALLAS', 'GA', 'PAULDING NORTHWEST ATLANTA AIRPORT', 33.912, -84.941, 1290, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00252');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00253', 'MISSISSIPPI CANYON 311 A OIL PLATFORM', 'LA', 'MISSISSIPPI CANYON 311 A', 28.643, -89.794, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00253');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00254', 'EAST CAMERON 278 OIL PLATFORM', 'LA', 'EAST CAMERON 278 OIL PLATFORM', 28.433, -92.883, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00254');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00255', 'SABINE OIL PLATFORM', 'LA', 'SABINE OIL PLATFORM', 29.483, -93.633, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00255');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00256', 'SOUTH MARSH 268 OIL PLATFORM', 'LA', 'SOUTH MARSH 268 OIL PLATFORM', 29.117, -91.867, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00256');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00257', 'VERMILLION 26 OIL PLATFORM', 'LA', 'VERMILLION 26 OIL PLATFORM', 29.467, -92.367, 735, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00257');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00258', 'BELEN', 'NM', 'ALEXANDER MUNICIPAL AIRPORT', 34.645, -106.834, 5194, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00258');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00259', 'GALVESTON 424 OIL PLATFORM', 'TX', 'GALVESTON 424 OIL PLATFORM', 28.577, -94.977, 1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00259');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00260', 'HIGH ISLAND 376 OIL PLATFORM', 'TX', 'HIGH ISLAND 376', 27.962, -93.671, 1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00260');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00261', 'MUSTANG ISLAND 31 OIL PLATFORM', 'TX', 'MUSTANG ISLAND 31', 27.283, -96.733, 1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00261');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00262', 'MUSTANG ISLAND A85A OIL PLATFORM', 'TX', 'MUSTANG ISLAND A85A', 27.733, -96.183, 1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00262');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00263', 'SCENIC OAKS', 'TX', 'BOERNE STAGE FIELD AIRPORT', 29.724, -98.695, 1385, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00263');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00264', 'NAMPA', 'ID', 'NAMPA MUNICIPAL AIRPORT', 43.581, -116.523, 2537, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00264');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00265', 'ROCHESTER', 'IN', 'FULTON COUNTY AIRPORT', 41.066, -86.182, 791, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00265');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00266', 'CARRINGTON', 'ND', 'CARRINGTON MUNICIPAL AIRPORT', 47.451, -99.151, 1608, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00266');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00267', 'THOMASVILLE', 'GA', 'THOMASVILLE REGIONAL AIRPORT', 30.902, -83.881, 263, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00267');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00268', 'MINDEN', 'LA', 'MINDEN AIRPORT', 32.646, -93.298, 279, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00268');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00269', 'BOULDER CITY', 'NV', 'BOULDER MUNICIPAL AIRPORT', 35.947, -114.861, 2202, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00269');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00270', 'CARRIZO SPRINGS', 'TX', 'FAITH RANCH AIRPORT', 28.209, -100.019, 775, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00270');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00271', 'LIBERTY', 'SC', 'PICKENS COUNTY AIRPORT', 34.817, -82.7, 1014, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00271');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00272', 'BOOMVANG SPAR OIL PLATFORM', 'NULL', 'BOOMVANG SPUR OIL PLATFORM', 27.35, -94.633, 332, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00272');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00273', 'OXFORD', 'GA', 'COVINGTON MUNICIPAL AIRPORT', 33.632, -83.85, 794, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00273');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00274', 'PENNINGTON GAP', 'VA', 'LEE COUNTY AIRPORT', 36.654, -83.218, 1411, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00274');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00275', 'STARKVILLE', 'MS', 'GEORGE M BRYAN AIRPORT', 33.433, -88.849, 332, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00275');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00276', 'COLEMAN', 'TX', 'COLEMAN MUNICIPAL AIRPORT', 31.841, -99.404, 1697, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00276');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00277', 'COURTLAND', 'AL', 'COURTLAND AIRPORT', 34.659, -87.349, 588, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00277');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00278', 'CLINTON', 'MS', 'JOHN BELL WILLIAMS AIRPORT', 32.304, -90.411, 247, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00278');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00279', 'RENO', 'NV', 'RENO STEAD AIRPORT', 39.667, -119.876, 5053, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00279');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00280', 'FALCON', 'CO', 'MEADOW LAKE AIRPORT', 38.946, -104.57, 6874, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00280');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00281', 'ROLLA', 'ND', 'ROLLA MUNICIPAL AIRPORT', 48.884, -99.621, 1821, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00281');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00282', 'BEACH', 'ND', 'BEACH AIRPORT', 46.925, -103.982, 2756, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00282');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00283', 'CAVALIER', 'ND', 'CAVALIER MUNICIPAL AIRPORT', 48.784, -97.632, 893, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00283');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00284', 'SCOTTSBORO', 'AL', 'SCOTTSBORO MUNICIPAL AIRPORT WORD FIELD', 34.689, -86.006, 650, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00284');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00285', 'CANDO', 'ND', 'CANDO MUNICIPAL AIRPORT', 48.48, -99.236, 1477, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00285');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00286', 'BROCKWAY', 'WI', 'BLACK RIVER FALLS AREA AIRPORT', 44.251, -90.855, 837, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00286');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00287', 'CROSBY', 'ND', 'CROSBY MUNICIPAL AIRPORT', 48.929, -103.297, 1949, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00287');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00288', 'LANGDON', 'ND', 'ROBERTSON FIELD AIRPORT', 48.753, -98.393, 1608, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00288');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00289', 'TIOGA', 'ND', 'TIOGA MUNICIPAL AIRPORT', 48.381, -102.898, 2267, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00289');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00290', 'TARBORO', 'NC', 'TARBORO EDGECOMBE AIRPORT', 35.937, -77.547, 53, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00290');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00291', 'GRAFTON', 'ND', 'HUTSON FIELD AIRPORT', 48.405, -97.371, 824, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00291');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00292', 'HAZEN', 'ND', 'MERCER COUNTY REGIONAL AIRPORT', 47.29, -101.581, 1815, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00292');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00293', 'RUGBY', 'ND', 'RUGBY MUNICIPAL AIRPORT', 48.39, -100.024, 1549, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00293');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00294', 'WATFORD CITY', 'ND', 'WATFORD CITY MUNICIPAL AIRPORT', 47.796, -103.254, 2110, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00294');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00295', 'BOONVILLE', 'MO', 'JESSE VIERTEL MEMORIAL AIRPORT', 38.947, -92.683, 716, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00295');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00296', 'MANDAN', 'ND', 'MANDAN MUNICIPAL AIRPORT', 46.768, -100.894, 1946, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00296');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00297', 'MANITOWISH WATERS', 'WI', 'MANITOWISH WATERS AIRPORT', 46.117, -89.883, 1611, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00297');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00298', 'LAPEER', 'MI', 'DUPONT LAPEER AIRPORT', 43.067, -83.267, 834, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00298');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00299', 'NORTH DELAND', 'FL', 'DELAND MUNICIPAL AIRPORT SIDNEY H TAYLOR FIELD', 29.067, -81.283, 79, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00299');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00300', 'STANLEY', 'ND', 'STANLEY MUNICIPAL AIRPORT', 48.301, -102.406, 2245, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00300');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00301', 'VALLEY CITY', 'ND', 'BARNES COUNTY MUNICIPAL AIRPORT', 46.942, -98.018, 1401, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00301');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00302', 'VIOSCA KNOLL 768 OIL PLATFORM', 'AL', 'VIOSCA KNOLL 768 OIL PLATFORM', 29.233, -87.783, 174, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00302');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00303', 'SALSA OIL PLATFORM', 'LA', 'SALSA', 27.833, -91.983, 99, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00303');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00304', 'EUGENE ISLAND OIL PLATFORM', 'LA', 'EUGENE ISLAND OIL PLATFORM', 28.633, -91.483, 92, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00304');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00305', 'EAST CAMERON 47 OIL PLATFORM', 'LA', 'EAST CAMERON 47 OIL PLATFORM', 29.433, -92.983, 96, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00305');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00306', 'HIGH ISLAND 179 OIL PLATFORM', 'LA', 'HIGH ISLAND 179 OIL PLATFORM', 29.183, -94.517, 247, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00306');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00307', 'VALENTINE', 'SD', 'MAIN PASS 140 B', 43.033, -100.617, 2589, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00307');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00308', 'NORTH PADRE ISLAND OIL PLATFORM', 'TX', 'NORTH PADRE ISLAND', 26.833, -96.933, 83, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00308');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00309', 'WEST CAMERON 368 A OIL PLATFORM', 'TX', 'WEST CAMERON 368 A OIL PLATFORM', 28.917, -93.3, 112, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00309');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00310', 'BONNERS FERRY', 'ID', 'BOUNDARY COUNTY AIRPORT', 48.726, -116.295, 2333, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00310');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00311', 'CORINTH', 'MS', 'ROSCOE TURNER AIRPORT', 34.915, -88.604, 427, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00311');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00312', 'ATLANTIS OIL PLATFORM', 'LA', 'ATLANTIS OIL PLATFORM', 27.2, -90.033, 165, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00312');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00313', 'BEAUMONT', 'TX', 'BEAUMONT MUNICIPAL AIRPORT', 30.071, -94.216, 33, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00313');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00314', 'CLARKSDALE', 'MS', 'FLETCHER FIELD AIRPORT', 34.3, -90.512, 174, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00314');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00315', 'DELAWARE', 'OH', 'DELAWARE MUNICIPAL JIM MOORE FIELD AIRPORT', 40.28, -83.115, 945, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00315');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00316', 'CUMBERLAND', 'WI', 'CUMERLAND MUNICIPAL AIRPORT', 45.506, -91.981, 1241, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00316');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00317', 'BROOKHAVEN', 'MS', 'BROOKHAVEN LINCOLN COUNTY AIRPORT', 31.606, -90.409, 489, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00317');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00318', 'LINTON', 'ND', 'LINTON MUNICIPAL AIRPORT', 46.218, -100.245, 1779, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00318');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00319', 'WRAY', 'CO', 'WRAY MUNICIPAL AIRPORT', 40.1, -102.233, 3678, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00319');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00320', 'PETALUMA', 'CA', 'PETALUMA MUNICIPAL AIRPORT', 38.25, -122.6, 89, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00320');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00321', 'OAKES', 'ND', 'OAKES MUNICIPAL AIRPORT', 46.167, -98.083, 1336, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00321');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00322', 'HARVEY', 'ND', 'HARVY MUNICIPAL AIRPORT', 47.783, -99.933, 1605, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00322');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00323', 'WAHOO', 'NE', 'WAHOO MUNICIPAL AIRPORT', 41.233, -96.6, 1224, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00323');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00324', 'BELLAFONTAINE', 'OH', 'BELLEFONTAINE REGIONAL AIRPORT', 40.367, -83.817, 1123, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00324');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00325', 'QUINCY', 'FL', 'QUINCY MUNICIPAL AIRPORT', 30.598, -84.557, 69, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00325');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00326', 'FERNANDINA BEACH', 'FL', 'FERNANDINA BEACH MUNICIPAL AIRPORT', 30.617, -81.467, 17, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00326');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00327', 'COOPERSTOWN', 'ND', 'COOPERSTOWN MUNICIPAL AIRPORT', 47.423, -98.106, 1424, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00327');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00328', 'AMERICUS', 'GA', 'JIMMY CARTER REGIONAL AIRPORT', 32.11, -84.189, 468, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00328');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00329', 'CAMDENTON', 'MO', 'CAMDENTON MEMORIAL AIRPORT', 37.974, -92.691, 1063, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00329');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00330', 'SWAINSBORO', 'GA', 'EAST GEORGIA REGIONAL AIRPORT', 32.609, -82.37, 329, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00330');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00331', 'MAIN PASS 289 C OIL PLATFORM', 'LA', 'MAIN PASS 289 C OIL PLATFORM', 29.248, -88.441, 378, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00331');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00332', 'VERMILION 331 OIL PLATFORM', 'NULL', 'VERMILION 331 OIL PLATFORM', 28.27, -92.264, 296, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00332');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00333', 'PORT FOURCHON', 'LA', 'PORT FORCHON HELIPORT', 29.117, -90.2, 99, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00333');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00334', 'LOYALL', 'KY', 'TUCKER GUTHRIE MEMORIAL AIRPORT', 36.859, -83.358, 1552, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00334');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00335', 'BUTLER', 'GA', 'BUTLER MUNICIPAL AIRPORT', 32.567, -84.251, 667, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00335');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00336', 'LOGANSPORT', 'IN', 'LOGANSPORT CASS COUNTY AIRPORT', 40.711, -86.375, 739, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00336');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00337', 'EASTMAN', 'GA', 'HEART OF GEORGIA REGIONAL AIRPORT', 32.214, -83.128, 306, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00337');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00338', 'TYNDALL AFB', 'FL', 'TYNDALL DRONE RUNWAY', 30.033, -85.533, 17, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00338');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00339', 'GRIFFIN', 'GA', 'GRIFFIN SPALDING COUNTY AIRPORT', 33.227, -84.275, 959, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00339');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00340', 'SILER CITY', 'NC', 'SILER CITY MUNICIPAL AIRPORT', 35.704, -79.504, 614, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00340');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00341', 'TEN SLEEP', 'WY', 'TEN SLEEP', 44.15, -107.167, 8993, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00341');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00342', 'SOUTH PASS', 'WY', 'SOUTH PASS', 42.517, -108.783, 8491, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00342');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00343', 'ROCHESTER', 'MN', 'ST MARYS HOSPITAL', 44.017, -92.483, 1165, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00343');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00344', 'WINCHESTER', 'TN', 'WINCHESTER MUNICIPAL AIRPORT', 35.178, -86.066, 978, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00344');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00345', 'LORDSBURG', 'NM', 'LORDSBURG MUNICIPAL AIRPORT', 32.333, -108.691, 4279, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00345');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00346', 'TRINITY CENTER', 'CA', 'TRINITY CENTER AIRPORT', 40.983, -122.694, 2389, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00346');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00347', 'LA PORTE', 'IN', 'LA PORTE MUNICIPAL AIRPORT', 41.572, -86.735, 811, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00347');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00348', 'TULLAHOMA', 'TN', 'TULLAHOMA REGIONAL AIRPORT WILLIAM NORTHERN FIELD', 35.38, -86.246, 1083, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00348');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00349', 'ANDREWS', 'TX', 'ANDREWS COUNTY AIRPORT', 32.331, -102.53, 3173, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00349');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00350', 'FITZGERALD', 'GA', 'FITZGERALD MUNICIPAL AIRPORT', 31.684, -83.27, 365, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00350');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00351', 'CAMERON', 'TX', 'CAMERON MUNICIPAL AIRPORT', 30.879, -96.971, 404, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00351');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00352', 'HAZLEHURST', 'GA', 'HAZLEHURST AIRPORT', 31.885, -82.647, 256, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00352');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00353', 'BOYSEN', 'WY', 'BOYSEN THERMOPOL', 43.467, -108.389, 7300, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00353');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00354', 'EXCELSIOR SPRINGS', 'MO', 'MIDWEST NATIONAL AIR CENTER AIRPORT', 39.332, -94.31, 778, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00354');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00355', 'BALL GROUND', 'GA', 'CHEROKEE COUNTY AIRPORT', 34.317, -84.417, 1221, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00355');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00356', 'CAMBRIDGE', 'MD', 'CAMBRIDGE DORCHESTER AIRPORT', 38.533, -76.033, 20, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00356');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00357', 'HAMILTON', 'TX', 'HAMILTON MUNICIPAL AIRPORT', 31.666, -98.149, 1300, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00357');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00358', 'COLLEGE PARK', 'MD', 'COLLEGE PARK AP', 38.981, -76.922, 50, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00358');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00359', 'SULPHUR', 'LA', 'SOUTHLAND FIELD AIRPORT', 30.131, -93.376, 10, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00359');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00360', 'OKEECHOBEE', 'FL', 'OKEECHOBEE COUNTY AIRPORT', 27.267, -80.85, 33, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00360');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00361', 'GLASGOW', 'KY', 'GLASGOW MUNICIPAL AIRPORT', 37.033, -85.95, 716, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00361');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00362', 'GATESVILLE', 'TX', 'GATESVILLE MUNICIPAL AIRPORT', 31.417, -97.8, 906, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00362');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00363', 'WARRENSBURG', 'MO', 'SKYHAVEN AIRPORT', 38.783, -93.8, 798, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00363');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00364', 'WEATHERFORD', 'OK', 'THOMAS P STAFFORD AIRPORT', 35.55, -98.667, 1605, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00364');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00365', 'ARANSAS PASS', 'TX', 'T P MC CAMPBELL AIRPORT', 27.917, -97.2, 17, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00365');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00366', 'CLAXTON', 'GA', 'CLAXTON EVANS COUNTY AIRPORT', 32.2, -81.87, 112, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00366');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00367', 'BEALETON', 'VA', 'WARRENTON FAUQUIER AIRPORT', 38.586, -77.711, 338, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00367');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00368', 'WALHALLA', 'ND', 'WALHALLA MUNICIPAL AIRPORT', 48.941, -97.903, 952, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00368');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00369', 'OCEANSIDE', 'CA', 'MCOLF CAMP PENDLETON RED BEACH AIRPORT', 33.286, -117.456, 89, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00369');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00370', 'PINEY ISLAND', 'NC', 'PINEY ISLAND', 35.02, -76.46, 17, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00370');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00371', 'MANNING', 'SC', 'SANTEE COOPER REGIONAL AIRPORT', 33.587, -80.209, 102, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00371');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00372', 'SPARTA', 'TN', 'UPPER CUMBERLAND REGIONAL AIRPORT', 36.056, -85.531, 1024, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00372');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00373', 'EAST CAMDEN', 'AR', 'HARRELL FIELD AIRPORT', 33.617, -92.767, 132, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00373');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00374', 'PAYSON', 'AZ', 'PAYSON AIRPORT', 34.257, -111.339, 5158, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00374');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00375', 'SEDONA', 'AZ', 'SEDONA AIRPORT', 34.85, -111.783, 4827, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00375');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00376', 'MT WERNER', 'CO', 'MOUNT WERNER', 40.46, -106.76, 10634, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00376');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00377', 'INVERNESS', 'FL', 'INVERNESS AIRPORT', 28.817, -82.317, 50, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00377');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00378', 'CLEVELAND', 'TX', 'CLEVELAND MUNICIPAL AIRPORT', 30.356, -95.008, 151, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00378');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00379', 'ALLENTOWN', 'PA', 'ALLENTOWN QUEEN CITY MUNICIPAL AIRPORT', 40.567, -75.483, 401, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00379');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00380', 'RAVENNA', 'OH', 'PORTAGE COUNTY AIRPORT', 41.217, -81.25, 1198, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00380');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00381', 'JENNINGS', 'LA', 'JENNINGS AIRPORT', 30.243, -92.673, 23, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00381');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00382', 'TOCCOA', 'GA', 'TOCCOA RG LETOURNEAU FIELD AIRPORT', 34.594, -83.296, 995, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00382');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00383', 'WADESBORO', 'NC', 'ANSON COUNTY AIRPORT JEFF CLOUD FIELD', 35.017, -80.083, 299, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00383');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00384', 'SOUTH BEND', 'IN', 'MICHIGAN CITY MUNICIPAL AIRPORT PHILLIPS FIELD', 41.703, -86.282, 657, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00384');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00385', 'BUENA VISTA BALD MTN', 'CO', 'BALD MOUNTAIN COTTONWOOD PASS', 38.783, -106.217, 9827, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00385');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00386', 'SPEARFISH', 'SD', 'BLACK HILLS AIRPORT CLYDE ICE FIELD', 44.483, -103.783, 3931, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00386');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00387', 'CANYON CITY', 'OR', 'GRANT COUNTY REGIONAL AIRPORT OGILVIE FIELD', 44.4, -118.967, 3698, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00387');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00388', 'RENSSELAER', 'IN', 'JASPER COUNTY AIRPORT', 40.817, -87.05, 696, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00388');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00389', 'ABERDEEN', 'MS', 'MONROE COUNTY AIRPORT', 33.873, -88.49, 227, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00389');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00390', 'MT PLEASANT', 'SC', 'MT PLEASANT REGIONAL AIRPORT FASION FIELD', 32.9, -79.783, 14, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00390');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00391', 'GEORGETOWN', 'SC', 'GEORGETOWN COUNTY AIRPORT', 33.317, -79.317, 40, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00391');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00392', 'HOLLISTER', 'CA', 'HOLLISTER MUNICIPAL AIRPORT', 36.9, -121.417, 237, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00392');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00393', 'TELLER', 'AK', 'TELLER AIRPORT', 65.233, -166.35, 292, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00393');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00394', 'JEFFERSON', 'GA', 'JACKSON COUNTY AIRPORT', 34.147, -83.561, 952, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00394');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00395', 'SAN MARTIN', 'CA', 'SOUTH COUNTY AIRPORT OF SANTA CLARA COUNTY', 37.082, -121.597, 283, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00395');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00396', 'MCKINLEYVILLE', 'CA', 'ROHNERVILLE AIRPORT', 40.554, -124.133, 391, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00396');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00397', 'GRASS VALLEY', 'CA', 'NEVADA COUNTY AIR PARK', 39.224, -121.003, 3153, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00397');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00398', 'PONCE', 'NULL', 'MERCEDITA AIRPORT', 18, -66.55, 30, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00398');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00399', 'SIMPSON BAY', 'NULL', 'PRINCESS JULIANA INTERNATIONAL AIRPORT', 18.04583, -63.11472, 14, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00399');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00400', 'ORANJESTAD', 'NULL', 'F D ROOSEVELT AIRPORT', 17.49833, -62.98028, 132, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00400');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00401', 'POINTE A PITRE', 'NULL', 'POINTE A PITRE INTERNATIONAL AIRPORT', 16.267, -61.6, 27, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00402', 'MARIGOT', 'NULL', 'MELVILLE HALL AIRPORT', 15.533, -61.3, 46, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00402');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00403', 'CANEFIELD', 'NULL', 'CANEFIELD AIRPORT', 15.333, -61.383, 17, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00403');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00404', 'LAMENTIN', 'NULL', 'MARTINIQUE CESAIRE INTERNATIONAL AIRPORT', 14.6, -61, 23, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00404');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00405', 'SANSOUCI', 'NULL', 'GEORGE F L CHARLES AIRPORT', 14.017, -61, 7, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00405');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00406', 'VIEUX FORT', 'NULL', 'HEWANORRA INTERNATIONAL AIRPORT', 13.75, -60.95, 33, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00406');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00407', 'ARNOS VALE', 'NULL', 'E T JOSHUA AIRPORT', 13.133, -61.2, 43, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00407');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00408', 'CHARNOCKS', 'NULL', 'GRANTLEY ADAMS INTERNATIONAL AIRPORT', 13.067, -59.483, 171, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00408');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00409', 'GRAND BEACH', 'NULL', 'MAURICE BISHOP INTERNATIONAL AIRPORT', 12, -61.783, 23, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00409');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00410', 'BON ACCORD VILLAGE', 'NULL', 'ARTHUR NAPOLEON RAYMOND ROBINSON INTERNATIONAL AIRPORT', 11.15, -60.833, 40, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00410');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00411', 'ORANJESTAD', 'NULL', 'QUEEN BEATRIX INTERNATIONAL AIRPORT', 12.5, -70.017, 60, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00411');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00412', 'WILLEMSTAD', 'NULL', 'HATO INTERNATIONAL AIRPORT', 12.2, -68.967, 30, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00412');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00413', 'KRALENDIJK', 'NULL', 'FLAMINGO INTERNATIONAL AIRPORT', 12.15, -68.283, 23, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00413');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00414', 'PEGASUS FIELD', 'NULL', 'PEGASUS FIELD', -77.95, 166.517, 17, NULL, 11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00414');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00415', 'STARKE', 'FL', 'KEYSTONE AIRPARK', 29.84475, -82.04752, 197, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00415');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00416', 'LURAY', 'VA', 'LURAY CAVERNS AIRPORT', 38.66722, -78.50111, 903, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00416');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00417', 'COLSTRIP', 'MT', 'COLSRTIP AIRPORT', 45.85283, -106.70927, 3429, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00417');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00418', 'SOUTH PADRE ISLAND', 'TX', 'SOUTH PADRE ISLAND HELIPORT', 26.067, -97.15, 14, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00418');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00419', 'BERTHOUD PASS', 'CO', 'BERTHOUD PASS', 39.8, -105.766, 13495, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00419');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00420', 'BOOTHVILLE-VENICE', 'LA', 'VENICE HELIPORT', 29.35, -89.433, 1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00420');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00421', 'MADISONVILLE', 'KY', 'MADISONVILLE MUNICIPAL AIRPORT', 37.35, -87.4, 408, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00421');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00422', 'WILKERSON PASS', 'CO', 'WILKERSON PASS', 39.05, -105.516, 11280, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00422');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00423', 'DEQUINCY', 'LA', 'DE QUINCY INDUSTRIAL AIRPARK', 30.44105, -93.47361, 84, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00423');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00424', 'SUNLIGHT', 'CO', 'SUNLIGHT', 39.43333, -107.38333, 10604, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00424');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00425', 'MIDDLEBURY', 'VT', 'MIDDLEBURY STATE AIRPORT', 43.98547, -73.09547, 489, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00425');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00426', 'PINE BLUFFS', 'WY', 'PINE BLUFFS MUNICIPAL AIRPORT', 41.15247, -104.13475, 5152, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00426');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00428', 'ARKADELPHIA', 'AR', 'DEXTER B FLORENCE MEMORIAL FIELD AIRPORT', 34.0998, -93.066, 183, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00428');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00429', 'NATURITA', 'CO', 'HOPKINS FIELD AIRPORT', 38.23875, -108.56327, 5940, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00429');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00430', 'ANGOLA', 'IN', 'TRI STATE STEUBEN COUNTY AIRPORT', 41.63968, -85.08349, 996, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00430');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00431', 'MARSHALL', 'TX', 'HARRISON COUNTY AIRPORT', 32.5205, -94.3077, 357, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00431');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00432', 'GRAND ISLE', 'LA', 'GRAND ISLE SAWRS', 29.25, -89.966, 7, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00432');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00433', 'BRIDGEPORT SONORA JUNCTION', 'CA', 'BRIDGEPORT SONORA JUNCTION', 38.3557, -119.519, 6749, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00433');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00434', 'SCOTTSBLUFF', 'NE', 'SCOTTSBLUFF', 41.86666, -103.58333, 3947, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00434');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00435', 'MATAGORDA BAY', 'TX', 'BRAZOS 451 OILP', 28.5, -95.716, 112, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00435');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00436', 'BLAIR', 'NE', 'BLAIR MUNICIPAL AIRPORT', 41.41167, -96.10861, 1299, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00436');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00437', 'MURRAY', 'KY', 'KYLE OAKLEY FIELD AIRPORT', 36.66458, -88.37277, 578, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00437');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00438', 'BRYAN', 'TX', 'COULTER FIELD AIRPORT', 30.71569, -96.33136, 368, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00438');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00439', 'CORDELE', 'GA', 'CRISP COUNTY CORDELE AIRPORT', 31.98883, -83.77391, 311, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00439');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00440', 'COLLINSVILLE', 'AL', 'CULLMAN REGIONAL AIRPORT FOLSOM FIELD', 34.26869, -85.85802, 970, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00440');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00441', 'WASHINGTON', 'IN', 'DAVIESS COUNTY AIRPORT', 38.70042, -87.12973, 470, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00441');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00442', 'WEST BAY', 'MS', 'WEST DELTA 27A', 29.11666, -89.55, 119, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00442');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00443', 'DUBOIS', 'WY', 'DUBOIS MUNICIPAL AIRPORT', 43.54836, -109.69025, 7297, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00443');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00444', 'DIXON', 'WY', 'DIXON AIRPORT', 41.03744, -107.49252, 6549, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00444');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00445', 'DEMOPOLIS', 'AL', 'DEMOPOLIS MUNICIPAL AIRPORT', 32.46383, -87.95405, 112, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00445');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00446', 'NEWCASTLE', 'WY', 'MONDELL FIELD AIRPORT', 43.8854, -104.3179, 4176, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00446');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00447', 'ATHENS', 'TX', 'ATHENS MUNICIPAL AIRPORT', 32.16384, -95.82835, 444, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00447');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00448', 'ROCKWALL', 'TX', 'RALPH M HALL ROCKWALL MUNICIPAL AIRPORT', 32.93059, -96.43548, 575, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00448');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00449', 'LYMAN', 'WY', 'FORT BRIDGER AIRPORT', 41.39333, -110.40597, 7039, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00449');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00450', 'WATKINS', 'CO', 'FRONT RANGE AIRPORT', 39.7842, -104.5376, 5513, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00450');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00451', 'BRANSON WEST', 'MO', 'BRANSON WEST MUNICIPAL EMERSON FIELD AIRPORT', 36.6985, -93.4022, 1349, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00451');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00452', 'GRANGEVILLE', 'ID', 'IDAHO COUNTY AIRPORT', 45.94255, -116.12341, 3314, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00452');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00453', 'GREEN CANYON 338 OIL PLATFORM', 'LA', 'GREEN CANYON 338', 27.63, -90.45, 191, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00453');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00454', 'OFFSHORE', 'LA', 'OFFSHORE', 28.18333, -88.48333, 142, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00454');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00455', 'HANNIBAL', 'MO', 'HANNIBAL REGIONAL AIRPORT', 39.72516, -91.44386, 770, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00455');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00456', 'ALAMINOS CANYON', 'TX', 'ALAMINOS CANYON', 26.93333, -94.68333, 83, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00456');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00457', 'NEW ROADS', 'LA', 'FALSE RIVER REGIONAL AIRPORT', 30.7183, -91.4786, 40, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00457');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00458', 'BATAVIA', 'OH', 'CLERMONT COUNTY AIRPORT', 39.07838, -84.21022, 843, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00458');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00459', 'MISS CANYON 474', 'LA', 'MISS CANYON 474', 28.51666, -88.28333, 83, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00459');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00460', 'GONZALES', 'LA', 'LOUISIANA REGIONAL AIRPORT', 30.17272, -90.94061, 15, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00460');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00461', 'MEXIA', 'TX', 'MEXIA LIMESTONE COUNTY AIRPORT', 31.64117, -96.51445, 545, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00461');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00462', 'BAYVILLE', 'NJ', 'OCEAN COUNTY AIRPORT', 39.9275, -74.2923, 82, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00462');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00463', 'MT PLEASANT', 'TN', 'MAURY COUNTY AIRPORT', 35.55438, -87.17913, 682, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00463');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00464', 'OCONTO', 'WI', 'J DOUGLAS BAKE MEMORIAL AIRPORT', 44.87405, -87.90977, 605, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00464');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00465', 'OLEAN', 'NY', 'CATTARAUGUS COUNTY OLEAN AIRPORT', 42.24122, -78.37136, 2136, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00465');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00466', 'OPELOUSAS', 'LA', 'ST LANDRY PARISH AIRPORT AHART FIELD', 30.55838, -92.09938, 76, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00466');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00467', 'SAND SPRINGS', 'OK', 'WILLIAM R POUGE MUNICIPAL AIRPORT', 36.17527, -96.15183, 893, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00467');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00468', 'PORT CLINTON', 'OH', 'CARL R KELLER FIELD AIRPORT', 41.51627, -82.86948, 590, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00468');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00469', 'PUYALLUP', 'WA', 'PIERCE COUNTY AIRPORT THUN FIELD', 47.10391, -122.28719, 539, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00469');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00470', 'PLATTSMOUTH', 'NE', 'PLATTSMOUTH MUNICIPAL AIRPORT', 40.94836, -95.91735, 1205, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00470');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00471', 'PRESIDIO', 'TX', 'PRESIDIO LELY INTERNATIONAL AIRPORT', 29.63421, -104.36149, 2940, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00471');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00472', 'BISHKEH', 'NULL', 'KRYGYZSTAN US MIL BISHKEK MANAS', 43.06666, 74.46666, 2090, NULL, 6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00472');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00473', 'ABU NAHLAH', 'NULL', 'QATAR US MIL AL UDEID', 25.11666, 51.31666, 132, NULL, 3
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00473');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00474', 'CLEVELAND', 'MS', 'CLEVELAND MUNICIPAL AIRPORT', 33.86113, -90.75788, 140, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00474');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00475', 'SARATOGA', 'WY', 'SHIVELY FIELD AIRPORT', 41.44352, -106.82752, 7016, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00475');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00476', 'PRESTONSBURG', 'KY', 'BIG SANDY REGIONAL AIRPORT', 37.75102, -82.63669, 1222, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00476');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00477', 'GONZALES', 'TX', 'ROGER M DREYER MEMORIAL AIRPORT', 29.5291, -97.4643, 355, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00477');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00479', 'TEHACHAPI', 'CA', 'TEHACHAPI MUNICIPAL AIRPORT', 35.1349, -118.4393, 4001, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00479');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00480', 'EAGLE RANGE', 'UT', 'EAGLE RANGE WSO', 41.05, -113.06, 4239, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00480');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00481', 'FILLMORE', 'UT', 'GRANITE PEAK FILLMORE ARPT', 38.95813, -112.36313, 4985, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00481');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00482', 'VENICE', 'FL', 'VENICE MUNICIPAL AIRPORT', 27.07161, -82.44033, 19, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00482');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00483', 'NEW ORLEANS', 'LA', 'NEW ORLEANS SUPERDOME HELIPORT', 29.95268, -90.08273, 30, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00483');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00484', 'HULETT', 'WY', 'HULETT MUNICIPAL AIRPORT', 44.66286, -104.56783, 4265, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00484');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00485', 'BUNNELL', 'FL', 'FLAGLER COUNTY AIRPORT', 29.46738, -81.20633, 33, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00485');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00486', 'ZEPHYRHILLS', 'FL', 'ZEPHYRHILLS MUNICIPAL AIRPORT', 28.22813, -82.15592, 90, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00486');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00487', 'FUA AMOTU', 'NULL', 'FUA AMOTU TONGA', -21.23, -175.15, 125, NULL, 13
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00487');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00488', 'WELLINGTON', 'NULL', 'WELLINGTON INTL', -41.3, 174.8, 23, NULL, 12
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00488');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00489', 'ANCHORAGE', 'AK', 'BRYANT ARMY AIRFIELD HELIPORT', 61.2658, -149.6533, 388, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00489');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00490', 'SHUNGNAK', 'AK', 'SHUNGNAK AIRPORT', 66.88808, -157.16241, 206, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00490');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00491', 'JASPER', 'TN', 'JASPER MARION COUNTY BROWN FIELD ARPT JASPER NDB', 35.06066, -85.5853, 642, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00491');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00492', 'MAJURO', 'MH', 'MARSHALL ISLANDS INTERNATIONAL AIRPORT', 7.06472, 171.27194, 6, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00492');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00493', 'TAFUNSAK', 'FM', 'KOSRAE', 5.35444, 162.95333, 8.6, NULL, 12
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00493');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '00494', 'MIRAMAR', 'PR', 'FERNANDO LUIS RIBAS DOMINICCI AIRPORT', 18.45677, -66.09847, 10, NULL, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='00494');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03011', 'TELLURIDE', 'CO', 'TELLURIDE REGIONAL AIRPORT', 37.95, -107.9, 9091, 9078, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03011');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03012', 'TAOS', 'NM', 'TAOS REGIONAL AIRPORT', 36.45, -105.66667, 7091, 7091, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03012');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03013', 'LAMAR', 'CO', 'LAMAR MUNICIPAL AIRPORT', 38.07, -102.68806, 3704, 3703, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03013');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03014', 'TORREON', 'NM', 'TORREON', 35.7991, -107.1813, 6700, 6909, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03014');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03016', 'RIFLE', 'CO', 'GARFIELD CO REGIONAL ARPT', 39.52778, -107.71972, 5522, 5544, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03016');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03017', 'DENVER', 'CO', 'DENVER INTERNATIONAL AIRPORT', 39.8328, -104.6575, 5414, 5431, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03017');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03024', 'BORGER', 'TX', 'HUTCHINSON COUNTY AIRPORT', 35.695, -101.395, 3054, 3054, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03024');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03026', 'BURLINGTON', 'CO', 'KIT CARSON COUNTY AIRPORT', 39.24472, -102.28417, 4192, 4217, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03026');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03027', 'CLINES CORNERS', 'NM', 'CLINES CORNERS', 35.00278, -105.66278, 7086, 7086, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03027');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03028', 'SPRINGFIELD', 'CO', 'COMANCHE NATIONAL GRASSLAND', 37.28333, -102.61389, 4383, 4380, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03028');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03029', 'WINDOW ROCK', 'AZ', 'WINDOW ROCK AIRPORT', 35.6575, -109.06139, 6739, 6739, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03029');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03030', 'GUYMON', 'OK', 'GUYMON MUNICIPAL AIRPORT', 36.68167, -101.50528, 3123, 3112, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03030');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03031', 'ODESSA', 'TX', 'ODESSA-SCHLEMEYER FLD ARPT', 31.92056, -102.38667, 3001, 3001, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03031');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03032', 'DRYDEN', 'TX', 'TERRELL COUNTY AIRPORT', 30.04806, -102.21306, 2322, 2301, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03032');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03033', 'MORIARTY', 'NM', 'MORIARTY AIRPORT', 34.98472, -106, 6199, 6199, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03033');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03034', 'ALBUQUERQUE', 'NM', 'DOUBLE EAGLE II AIRPORT', 35.145, -106.795, 5837, 5837, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03034');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03035', 'ARTESIA', 'NM', 'ARTESIA MUNICIPAL AIRPORT', 32.8525, -104.4675, 3541, 3541, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03035');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03036', 'BLANDING', 'UT', 'BLANDING MUNICIPAL AIRPORT', 37.58278, -109.48306, 5865, 5865, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03036');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03037', 'COLBY', 'KS', 'SHALTZ FIELD AIRPORT', 39.4275, -101.04639, 3186, 3186, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03037');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03038', 'COPPER MOUNTAIN', 'CO', 'COPPER MOUNTAIN', 39.46667, -106.15, 12074, 12074, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03038');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03039', 'WOLF CREEK PASS', 'CO', 'WOLF CREEK PASS AWOS-3 AIRPORT', 37.45, -106.8, 11791, 11791, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03039');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03040', 'MONUMENT HILL', 'CO', 'MONUMENT HILL AWOS-3 ARPT', 39.21667, -104.63333, 7060, 7060, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03040');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03041', 'MONARCH PASS', 'CO', 'MONARCH PASS AWOS-3 ARPT', 38.48333, -106.31667, 12031, 12031, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03041');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03042', 'LA VETA PASS', 'CO', 'LA VETA PASS AWOS-3 ARPT', 37.5, -105.16667, 10217, 10217, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03042');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03043', 'ATWOOD', 'KS', 'ATWOOD-RAWLNS CO CTY CO AP', 39.84, -101.04194, 2991, 2991, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03043');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03044', 'BIG SPRING', 'TX', 'BIG SRNG MCMHN-WRKLE ARPT', 32.2125, -101.52139, 2573, 2573, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03044');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03045', 'PERRYTON', 'TX', 'PERRYTON OCHILTREE CO ARPT', 36.41389, -100.74972, 2918, 2918, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03045');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03047', 'MONAHANS', 'TX', 'MONAHANS 6 ENE', 31.6219, -102.8071, 2724, 2724, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03047');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03048', 'SOCORRO', 'NM', 'SOCORRO 20 N', 34.3557, -106.8859, 4847, 4847, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03048');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03049', 'ALPINE', 'TX', 'ALPINE-CASPARIS MUNI ARPT', 30.38333, -103.68333, 4513, 4315, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03049');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03050', 'SNYDER', 'TX', 'WINSTON FIELD AIRPORT', 32.69333, -100.95056, 2430, 2430, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03050');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03051', 'LEOTI', 'KS', 'MARK HOARD MEMORIAL ARPT', 38.45694, -101.35333, 3303, 3303, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03051');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03052', 'PAMPA', 'TX', 'PERRY LEFORS FIELD AIRPORT', 35.61306, -100.99639, 3245, 3245, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03052');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03053', 'SANTA TERESA', 'NM', 'DONA ANA COUNTY AIRPORT AT SANTA TERESA', 31.88333, -106.71722, 4112, 4112, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03053');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03054', 'MULESHOE', 'TX', 'MULESHOE 19 S', 33.9557, -102.774, 3742, 2742, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03054');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03055', 'GOODWELL', 'OK', 'GOODWELL 2 E', 36.5993, -101.595, 3266, 3266, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03055');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03056', 'SEMINOLE', 'TX', 'GAINES COUNTY AIRPORT', 32.67528, -102.65444, 3315, 3315, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03056');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03058', 'PECOS', 'TX', 'PECOS MUNICIPAL AIRPORT', 31.3825, -103.51083, 2613, 2613, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03058');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03059', 'SONORA', 'TX', 'SONORA MUNICIPAL AIRPORT', 30.58583, -100.64861, 2140, 2140, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03059');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03060', 'MONTROSE', 'CO', 'MONTROSE 11 ENE', 38.544, -107.6928, 8402, 8402, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03060');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03061', 'CORTEZ', 'CO', 'CORTEZ 8 SE', 37.2553, -108.5035, 8034, 834, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03061');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03062', 'LOS ALAMOS', 'NM', 'LOS ALAMOS 13 W', 35.8584, -106.5214, 8716, 8716, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03062');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03063', 'LA JUNTA', 'CO', 'LA JUNTA 17 WSW', 37.8639, -103.8224, 4386, 4386, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03063');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03064', 'BUENA VISTA AP', 'CO', 'CENTRAL COLORADO REGIONAL AP', 38.6975, -106.07, 7946, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03064');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03065', 'DENVER', 'CO', 'ATCT, ROCKY MOUNTAIN METRO', 39.9, -105.11667, 5595, 5670, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03065');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03067', 'OAKLEY', 'KS', 'OAKLEY 19 SSW', 38.8701, -100.9627, 2870, 2870, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03067');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03068', 'BULLSEYE', 'CO', 'BULLSEYE AUXILIARY AIRFIELD USAFA', 38.76667, -104.3, 6030, 6030, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03068');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03069', 'SAGUACHE', 'CO', 'SAGUACHE MUNICIPAL AIRPORT', 38.09722, -106.16889, 7826, 7826, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03069');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03070', 'DUMAS', 'TX', 'MOORE COUNTY AIRPORT', 35.85806, -102.01306, 3705, 3705, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03070');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03071', 'MIDLAND', 'TX', 'MIDLAND AIRPARK', 32.03639, -102.10111, 2803, 2803, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03071');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03072', 'BRONTE', 'TX', 'BRONTE 11 NNE', 32.0408, -100.2495, 1997, 1997, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03072');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03073', 'CROCKETT', 'TX', 'CROCKETT HOUSTON COUNTY AP', 31.30694, -95.40389, 348, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03073');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03074', 'LAS CRUCES', 'NM', 'LAS CRUCES 20 N', 32.6137, -106.7414, 4327, 4327, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03074');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03075', 'DULCE', 'NM', 'DULCE 1 NW', 36.9352, -107.0003, 6806, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03075');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03076', 'GRAND JUNCTION', 'CO', 'GRAND JUNCTION 9 W', 39.1018, -108.7353, 5806, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03076');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03077', 'HOLBROOK', 'AZ', 'HOLBROOK 17 ESE', 34.8221, -109.8927, 5613, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03077');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03078', 'EADS', 'CO', 'EADS 16 ENE', 38.5443, -102.5025, 3967, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03078');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03079', 'SAGUACHE', 'CO', 'SAGUACHE 2 WNW', 38.0989, -106.1709, 7823, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03079');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03080', 'RESERVE', 'NM', 'RESERVE 1 W', 33.7149, -108.7771, 5842, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03080');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03081', 'TROPIC', 'UT', 'TROPIC 9 SE', 37.5174, -111.9775, 5895, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03081');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03082', 'CARRIZOZO', 'NM', 'CARRIZOZO 1 W', 33.6476, -105.8945, 5366, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03082');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03083', 'STRATTON', 'CO', 'STRATTON 24 N', 39.6553, -102.6217, 4212, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03083');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03084', 'CENTER A', 'CO', 'CENTER A 4 SSW', 37.7068, -106.1444, 7678, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03084');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03085', 'BOWIE', 'AZ', 'BOWIE 23 SSE', 32.0095, -109.3891, 5133, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03085');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03086', 'SPRINGFIELD', 'CO', 'SPRINGFIELD 6 WSW', 37.3813, -102.7194, 4557, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03086');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03087', 'SANTA FE', 'NM', 'SANTA FE 20 WNW', 35.8245, -106.3178, 7258, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03087');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03088', 'WOODLAND PARK', 'CO', 'WOODLAND PARK 14 WSW', 38.9105, -105.265, 8535, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03088');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03089', 'ROCKY FORD', 'CO', 'ROCKY FORD 1 ESE', 38.0386, -103.6948, 4170, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03089');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03090', 'TAOS', 'NM', 'TAOS 27 NW', 36.6519, -105.9721, 8151, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03090');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03091', 'KIM', 'CO', 'KIM 9 WSW', 37.2168, -103.5042, 5870, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03091');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03092', 'RATON', 'NM', 'RATON 26 ESE', 36.7779, -103.9818, 7232, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03092');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03093', 'GENOA', 'CO', 'GENOA 35 N', 39.7857, -103.517, 4764, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03093');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03094', 'CLAYTON', 'NM', 'CLAYTON 3 ENE', 36.4701, -103.1238, 4885, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03094');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03095', 'MILLS', 'NM', 'MILLS 6 WSW', 36.0609, -104.3552, 5866, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03095');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03096', 'RIFLE', 'CO', 'RIFLE 23 NW', 39.7608, -108.1268, 7550, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03096');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03097', 'MOUNTAINAIR', 'NM', 'MOUNTAINAIR 2 WSW', 34.517, -106.2702, 6489, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03097');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03098', 'EAGLE', 'CO', 'EAGLE 13 SSE', 39.4827, -106.7337, 8605, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03098');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03099', 'CRAIG', 'CO', 'CRAIG 30 N', 40.948, -107.6082, 6518, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03099');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03101', 'SHOW LOW', 'AZ', 'SHOW LOW MUNICIPAL AIRPORT', 34.2639, -110.0075, 6411, 6415, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03101');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03102', 'ONTARIO', 'CA', 'ONTARIO INTERNATIONAL ARPT', 34.05611, -117.60028, 949, 905, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03102');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03103', 'FLAGSTAFF', 'AZ', 'FLAGSTAFF PULLIAM AIRPORT', 35.1441, -111.6663, 7003, 7018, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03103');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03104', 'PALM SPRINGS', 'CA', 'DESERT RESORTS RGNL ARPT', 33.62667, -116.15944, -118, -118, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03104');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03122', 'NULL', 'CA', 'TORRANCE AIRPORT', 33.8016, -118.3419, 90, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03122');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03124', 'FORT HUACHUCA/SIERRA VISTA', 'AZ', 'SIERRA VISTA MUNICIPAL-LIBBY AAF AIRPORT', 31.58833, -110.34417, 4719, 4698, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03124');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03131', 'SAN DIEGO', 'CA', 'MONTGOMERY FIELD AIRPORT', 32.81583, -117.13944, 417, 423, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03144', 'IMPERIAL', 'CA', 'IMPERIAL COUNTY AIRPORT', 32.83417, -115.57861, -58, -56, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03144');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03145', 'YUMA', 'AZ', 'YUMA MCAS', 32.65, -114.61667, 213, 213, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03145');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03148', 'GILA BEND', 'AZ', 'GILA BEND AF AUX AIRPORT', 32.88333, -112.71667, 846, 858, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03148');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03154', 'CAMP PENDLETON', 'CA', 'MARINE CORPS AIR STATION', 33.3, -117.35, 75, 78, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03154');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03159', 'LANCASTER', 'CA', 'GENERAL WILLIAM J. FOX AIRFIELD AIRPORT', 34.7411, -118.2116, 2338, 2339, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03159');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03160', 'MERCURY', 'NV', 'DESERT ROCK AIRPORT', 36.6206, -116.0278, 3230, 3310, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03160');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03162', 'PAGE', 'AZ', 'PAGE MUNICIPAL AIRPORT', 36.92611, -111.44778, 4310, 4310, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03162');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03163', 'CALIENTE', 'NV', 'CALIENTE AIRPORT', 37.61222, -114.52639, 4374, 4373, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03163');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03164', 'CAMPO', 'CA', 'CAMPO', 32.6233, -116.4728, 2630, 2615, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03164');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03165', 'EL MONTE', 'CA', 'EL MONTE AIRPORT', 34.08333, -118.03333, 296, 296, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03165');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03166', 'FULLERTON', 'CA', 'FULLERTON MUNICIPAL ARPT', 33.87194, -117.97889, 96, 96, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03166');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03167', 'HAWTHORNE', 'CA', 'NRTHORP FLD/HATHRN MUNI AP', 33.92278, -118.33417, 63, 63, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03167');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03170', 'EUREKA', 'NV', 'EUREKA', 39.6013, -116.0055, 5936, 5936, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03170');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03171', 'RIVERSIDE', 'CA', 'RIVERSIDE MUNICIPAL ARPT', 33.95194, -117.43861, 804.5, 831, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03171');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03174', 'TORRANCE', 'CA', 'ZAMPERINI FIELD AIRPORT', 33.80338, -118.33961, 97, 103, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03174');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03177', 'CARLSBAD', 'CA', 'MCCLELLAN-PALOMAR AIRPORT', 33.12806, -117.27944, 328, 328, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03177');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03178', 'SAN DIEGO', 'CA', 'BROWN FIELD MUNICIPAL ARPT', 32.57222, -116.97944, 515, 524, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03178');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03179', 'CHINO', 'CA', 'CHINO AIRPORT', 33.97528, -117.63611, 650, 681, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03179');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03180', 'LA VERNE', 'CA', 'BRACKETT FIELD AIRPORT', 34.1, -117.78333, 1011, 1011, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03180');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03181', 'MAMMOTH LAKES', 'CA', 'MAMMOTH YOSEMITE AIRPORT', 37.63333, -118.85, 7128, 7128, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03181');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03182', 'BICYCLE LAKE FORT IRWIN', 'CA', 'BICYCLE LAKE FORT IRWIN AAF', 35.28333, -116.63333, 248, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03182');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03183', 'MOJAVE', 'CA', 'MOJAVE AIRPORT', 35.06667, -118.15, 2787, 2791, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03183');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03184', 'PHOENIX', 'AZ', 'PHOENIX DEER VALLEY ARPT', 33.68833, -112.08167, 1455, 1476, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03184');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03185', 'MESA', 'AZ', 'FALCON FIELD AIRPORT', 33.46667, -111.73333, 1380, 1394, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03185');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03186', 'GOODYEAR', 'AZ', 'PHOENIX GOODYEAR AIRPORT', 33.41667, -112.38333, 968, 968, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03186');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03192', 'SCOTTSDALE', 'AZ', 'SCOTTSDALE AIRPORT', 33.62278, -111.91056, 1473, 1510, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03192');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03195', 'GRAND CANYON', 'AZ', 'GRAND CANYON NATL PARK APT', 35.94611, -112.15472, 6606, 6606, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03195');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03196', 'NOGALES', 'AZ', 'NOGALES INTERNATIONAL ARPT', 31.42083, -110.84583, 3932, 3932, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03196');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03701', 'NORFOLK', 'VA', 'CHESAPEAKE REGIONAL ARPT', 36.66556, -76.32056, 20, 20, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03701');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03702', 'KENANSVILLE', 'NC', 'DUPLIN COUNTY AIRPORT', 35, -77.98167, 137, 137, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03702');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03703', 'EDENTON', 'NC', 'NORTHEASTERN REGIONAL ARPT', 36.0275, -76.56694, 20, 20, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03703');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03704', 'EMPORIA', 'VA', 'EMPORIA-GRENVLE RGNL ARPT', 36.68667, -77.48278, 127, 127, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03705', 'ELIZABETHTOWN', 'NC', 'CURTIS L BROWN JR FLD ARPT', 34.60167, -78.57806, 133, 133, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03705');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03706', 'FREDRICKSBURG', 'VA', 'SHANNON AIRPORT', 38.26667, -77.44917, 85, 85, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03706');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03707', 'FARMVILLE', 'VA', 'FARMVILLE REGIONAL AIRPORT', 37.3575, -78.43778, 417, 417, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03707');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03708', 'GOLDSBORO', 'NC', 'GOLDSBORO-WAYNE MUNI ARPT', 35.46028, -77.96472, 134, 134, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03708');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03709', 'ASHEBORO', 'NC', 'ASHEBORO MUNICIPAL AIRPORT', 35.65361, -79.895, 673, 673, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03709');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03710', 'MANASSAS', 'VA', 'MANSAS RGNL/H P DAVS FD AP', 38.72139, -77.51528, 192, 192, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03710');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03711', 'OXFORD', 'NC', 'HENDERSON-OXFORD AIRPORT', 36.36139, -78.52889, 527, 527, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03711');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03712', 'ERWIN', 'NC', 'HARNETT COUNTY AIRPORT', 35.37861, -78.73361, 198, 198, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03712');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03713', 'SMITHFIELD', 'NC', 'JOHNSTON COUNTY AIRPORT', 35.54083, -78.39028, 165, 165, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03713');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03714', 'LEESBURG', 'VA', 'LESSBURG EXECUTIVE AIRPORT', 39.07778, -77.5575, 389, 389, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03714');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03715', 'LOUISA', 'VA', 'LOUISA CO/FREEMAN FLD ARPT', 38.00972, -77.97, 493, 493, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03715');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03716', 'MELFA', 'VA', 'ACCOMACK COUNTY AIRPORT', 37.64667, -75.76083, 47, 47, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03716');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03717', 'WINCHESTER', 'VA', 'WINCHESTER REGIONAL ARPT', 39.14333, -78.14444, 727, 727, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03717');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03718', 'ORANGE', 'VA', 'ORANGE COUNTY AIRPORT', 38.24694, -78.04556, 469, 469, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03718');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03719', 'SUFFOLK', 'VA', 'SUFFOLK MUNICIPAL AIRPORT', 36.68222, -76.60167, 72, 72, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03719');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03720', 'PINEHURST/SOUTHERN P', 'NC', 'MOORE COUNTY AIRPORT', 35.23722, -79.39111, 461, 461, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03720');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03721', 'OAK ISLAND', 'NC', 'BRUNSWICK COUNTY AIRPORT', 33.92917, -78.07472, 25, 25, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03721');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03722', 'ROXBORO', 'NC', 'PERSON COUNTY AIRPORT', 36.28472, -78.98417, 609, 609, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03722');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03723', 'SANFORD', 'NC', 'SANFORD-LEE COUNTY RGNL ARPT', 35.58222, -79.10111, 247, 247, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03723');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03724', 'WESTMINISTER', 'MD', 'CARROLL CO RGNL/JJACK B POAGE FIELD AIRPORT', 39.60806, -77.0075, 789, 789, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03724');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03725', 'PETERSBURG', 'WV', 'GRANT COUNT AIRPORT', 38.99417, -79.14417, 960, 960, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03725');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03726', 'WILDWOOD', 'NJ', 'CAPE MAY COUNTY AIRPORT', 39.00833, -74.90806, 23, 23, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03726');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03727', 'CLINTON', 'NC', 'SAMPSON COUNTY AIRPORT', 34.97556, -78.36444, 148, 148, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03727');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03728', 'MCCLELLANVILLE', 'SC', 'MCCLELLANVILLE 7 NE', 33.1532, -79.3637, 9, 9, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03728');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03730', 'WILLIAMSBURG', 'VA', 'WILLIAMSBURG-JAMESTOWN APT', 37.23917, -76.71611, 49, 49, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03730');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03731', 'LOUISBURG', 'NC', 'FRANKLIN COUNTY AIRPORT', 36.02333, -78.33028, 369, 369, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03731');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03732', 'CUMBERLAND', 'MD', 'GREATER CUMBERLAND REGIONAL AIRPORT', 39.6, -78.76667, 775, 775, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03732');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03733', 'ELKINS', 'WV', 'ELKINS 21 ENE', 39.013, -79.4743, 3390, 3390, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03734', 'WEST POINT', 'VA', 'MIDDLE PENINSULA REGIONAL AIRPORT', 37.52111, -76.76472, 24, 24, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03734');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03735', 'STAFFORD', 'VA', 'STAFFORD REGIONAL AIRPORT', 38.39806, -77.45556, 212, 212, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03735');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03736', 'KILL DEVIL HILLS', 'NC', 'FIRST FLIGHT AIRPORT', 36.01833, -75.67139, 13, 13, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03736');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03737', 'CURRITUCK', 'NC', 'CURRITUCK COUNTY AIRPORT', 36.39889, -76.01639, 16, 16, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03738', 'ROCKINGHAM', 'NC', 'RICHMOND COUNTY AIRPORT', 34.89111, -79.75917, 358, 358, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03738');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03739', 'CAPE CHARLES', 'VA', 'CAPE CHARLES 5 ENE', 37.2907, -75.927, 29, 29, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03739');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03741', 'WASHINGTON', 'NC', 'WARREN FIELD AIRPORT', 35.57028, -77.04972, 38, 38, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03741');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03742', 'DARLINGTON', 'SC', 'DARLINGTON COUNTY JETPORT AIRPORT', 34.46583, -79.89028, 192, 192, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03742');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03749', 'CAMP DAVID', 'MD', 'CAMP DAVID', 39.645, -77.468, 1847, 1842, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03749');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03755', 'OAKLAND', 'MD', 'GARRETT COUNTY AIRPORT', 39.58028, -79.33944, 2933, 2933, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03755');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03756', 'EASTON', 'MD', 'EASTON/NEWNAM FIELD AIRPORT', 38.80417, -76.06889, 72, 72, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03756');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03757', 'ST INIGOES', 'MD', 'WEBSTER NAVAL OUTLYING FIELD', 38.14167, -76.42917, 21, 21, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03757');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03758', 'DURHAM', 'NC', 'DURHAM 11 W', 35.9705, -79.0931, 562, 562, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03758');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03759', 'CHARLOTTESVILLE', 'VA', 'CHARLOTTESVILLE 2 SSE', 37.9975, -78.4656, 1177, 1177, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03759');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03761', 'AVONDALE', 'PA', 'AVONDALE 2 N', 39.8593, -75.7861, 400, 400, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03761');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03763', 'REIDSVILLE', 'NC', 'ROCKINGHAM COUNTY NC SHILOH AIRPORT', 36.43722, -79.85111, 693, 693, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03763');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03802', 'CLARKSBURG', 'WV', 'HARRISON/MARION RGNL ARPT', 39.29556, -80.22889, 1203, 1203, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03804', 'PARKERSBURG', 'WV', 'MID-OHIO VALLEY REGIONAL AIRPORT', 39.2, -81.27, 831, 864, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03804');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03809', 'DYERSBURG', 'TN', 'DYERSBURG MUNICIPAL ARPT', 36.0002, -89.4094, 300, 338, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03810', 'HICKORY', 'NC', 'HICKORY REGIONAL AIRPORT', 35.7425, -81.3819, 1143, 1188, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03811', 'JACKSON', 'TN', 'MCKELLAR-SIPES RGNL ARPT', 35.593, -88.9167, 433, 422, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03811');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03812', 'ASHEVILLE', 'NC', 'ASHEVILLE REGIONAL AIRPORT', 35.43194, -82.5375, 2117, 2170, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03812');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03813', 'MACON', 'GA', 'MIDDLE GEORGIA REGIONAL AIRPORT', 32.6847, -83.6527, 343, 362, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03816', 'PADUCAH', 'KY', 'BARKLEY REGIONAL AIRPORT', 37.0563, -88.7744, 413, 413, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03818', 'MARIANNA', 'FL', 'MARIANNA MUNICIPAL AIRPORT', 30.83556, -85.18389, 113, 113, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03820', 'AUGUSTA', 'GA', 'AUGUSTA REGIONAL AT BUSH FIELD AP', 33.3644, -81.9633, 132, 148, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03821', 'LAGRANGE', 'GA', 'LAGRANGE-CALLAWAY AIRPORT', 33.01667, -85.06667, 682, 693, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03822', 'SAVANNAH', 'GA', 'SAVANNAH/HILTON HEAD INTL AIRPORT', 32.13, -81.21, 46, 51, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03832', 'WINDER', 'GA', 'WINDER-BORROW AIRPORT', 33.98278, -83.66806, 943, 943, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03832');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03841', 'OAK RIDGE', 'TN', 'ATDL', 36.0028, -84.2486, 905, 880, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03847', 'CROSSVILLE', 'TN', 'CROSSVILLE MEMORIAL -WHITSON FIELD APT', 35.9509, -85.0813, 1867, 1870, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03849', 'LONDON', 'KY', 'LDON-CRBN APT-MGEE FLD APT', 37.08722, -84.07694, 1188, 1211, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03850', 'FORT RUCKER/OZARK', 'AL', 'CAIRNS ARMY AIRFIELD (FORT RUCKER)', 31.26667, -85.71667, 301, 301, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03852', 'MARY ESTHER', 'FL', 'HURLBURT FIELD AIRPORT', 30.41667, -86.68333, 38, 38, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03853', 'MAYPORT', 'FL', 'MAYPORT NAF', 30.4, -81.41667, 16, 14, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03855', 'PENSACOLA', 'FL', 'PENSACOLA NAS', 30.35, -87.31667, 28, 30, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03856', 'HUNTSVILLE', 'AL', 'HUNTSVILLE INTL/C.T.JONES FIELD AIRPORT', 34.64389, -86.78611, 624, 644, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03856');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03858', 'EASTOVER', 'SC', 'MCENTIRE AIR NATIONAL GUARD STATION', 33.96667, -80.8, 254, 252, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03859', 'BLUEFIELD', 'WV', 'MERCER COUNTY AIRPORT', 37.2958, -81.2077, 2870, 2867, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03859');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03860', 'HUNTINGTON', 'WV', 'TRI-STATE/M.J.FERGUSON FIELD AIRPORT', 38.365, -82.555, 824, 838, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03860');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03865', 'MARION', 'IL', 'WILLIAMSON COUNTY RGNL APT', 37.75, -89, 460, 472, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03865');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03866', 'MERIDIAN', 'MS', 'MERIDIAN NAS/MC CAIN FD AP', 32.55, -88.56667, 271, 317, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03866');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03868', 'TERRE HAUTE', 'IN', 'TER HUTE INTL-HULMAN FD AP', 39.45194, -87.30889, 575, 593, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03870', 'GREER', 'SC', 'GREENVILLE-SPARTANBURG INTL AIRPORT', 34.8842, -82.2209, 943, 971, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03872', 'BECKLEY', 'WV', 'RALEIGH COUNTY MEMORIAL AIRPORT', 37.7836, -81.123, 2514, 2514, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03872');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03875', 'FORT STEWART (HINESVILLE)', 'GA', 'WRIGHT AAF AIRPORT', 31.88333, -81.56667, 45, 46, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03875');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03878', 'TROY', 'AL', 'TROY MUNICIPAL AIRPORT', 31.86056, -86.01222, 397, 397, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03878');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03879', 'SALEM', 'IL', 'SALEM-LECKRONE AIRPORT', 38.65, -88.96667, 570, 573, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03879');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03881', 'CENTREVILLE-BRENT', 'AL', 'NWS METEOROLOGICAL OBSY', 32.9, -87.25, 453, 453, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03881');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03882', 'PANAMA CITY', 'FL', 'PANAMA CITY-BAY CO. INT AP', 30.21194, -85.68278, 21, 18, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03883', 'BIRMINGHAM C.O.', 'AL', '11 WEST OXMOOR RD', 33.46667, -86.83333, 744, 712, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03887', 'DECATUR', 'IL', 'DECATUR AIRPORT', 39.83444, -88.86556, 675, 679, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03887');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03888', 'ATLANTA', 'GA', 'FULTON CO-BROWN FLD ARPT', 33.77917, -84.52139, 840, 841, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03888');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03889', 'JACKSON', 'KY', 'JULIAN CARROLL AIRPORT', 37.59139, -83.31444, 1365, 1357, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03892', 'AUBURN', 'AL', 'ABRN-OPLKA R G PITTS ARPT', 32.61611, -85.43333, 776, 776, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03892');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03893', 'BLOOMINGTON', 'IN', 'MONROE COUNTY AIRPORT', 39.13333, -86.61667, 844, 847, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03894', 'CLARKSVILLE', 'TN', 'OUTLAW FIELD AIRPORT', 36.62389, -87.41944, 560, 550, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03896', 'GADSDEN', 'AL', 'GADSDEN MUNICIPAL AIRPORT', 33.96667, -86.08333, 569, 569, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03901', 'LONGVIEW', 'TX', 'EAST TEXAS REGIONAL ARPT', 32.38472, -94.71167, 365, 373, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03902', 'FORT HOOD/KILLEEN', 'TX', 'ROBERT GRAY AFF AIRPORT', 31.06667, -97.83333, 1015, 1015, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03904', 'COLLEGE STATION', 'TX', 'EASTERWOOD FIELD AIRPORT', 30.58917, -96.36472, 305, 328, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03908', 'HAMMOND', 'LA', 'HAMMOND MUNICIPAL AIRPORT', 30.52083, -90.4175, 44, 44, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03914', 'CASA GRANDE', 'AZ', 'CASA GRANDE MUNICIPAL ARPT', 32.95, -111.76667, 1462, 1463, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03914');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03918', 'FLIPPIN', 'AR', 'MARION COUNTY RGNL AIRPORT', 36.29083, -92.59, 719, 719, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03919', 'SALINA', 'KS', 'SALINA MUNICIPAL AIRPORT', 38.8, -97.65, 1269, 1282, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03923', 'WICHITA', 'KS', 'MCCONNELL AFB AIRPORT', 37.61667, -97.26667, 1371, 1371, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03923');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03927', 'DALLAS-FORT WORTH', 'TX', 'DALLAS/FT WORTH INTERNATIONAL AP', 32.8978, -97.0189, 560, 596, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03928', 'WICHITA', 'KS', 'WICHITA EISENHOWER NATIONAL', 37.6475, -97.43, 1321, 1340, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03929', 'KANSAS CITY', 'MO', 'RICHARD GEBAUR', 38.84389, -94.56028, 1056, 1090, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03930', 'JACKSONVILLE', 'AR', 'LITTLE ROCK AFB AIRPORT', 34.91667, -92.15, 311, 311, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03931', 'FORT POLK', 'LA', 'POLK ARMY AIRFIELD', 31.05, -93.18333, 330, 330, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03932', 'CLINTON', 'OK', 'CLINTON-SHERMAN AIRPORT', 35.3568, -99.2042, 1922, 1955, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03933', 'FORT HOOD (KILLEEN)', 'TX', 'HOOD AAF AIRPORT', 31.13333, -97.71667, 924, 875, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03935', 'CAPE GIRARDEAU', 'MO', 'CAPE GIRARDEAU RGNL ARPT', 37.2252, -89.5705, 336, 352, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03936', 'MANHATTAN', 'KS', 'MANHATTAN REGIONAL AIRPORT', 39.1346, -96.6788, 1056, 1070, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03936');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03937', 'LAKE CHARLES', 'LA', 'LAKE CHARLES REGIONAL AIRPORT', 30.12472, -93.22833, 9, 9, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03938', 'FORT LEONARD WOOD', 'MO', 'WAYNESVILLE REGIONAL AIRPORT AT FORNEY FIELD', 37.75, -92.15, 1159, 1159, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03938');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03940', 'JACKSON', 'MS', 'JACKSON INTERNATIONAL AIRPORT', 32.3205, -90.0777, 330, 331, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03945', 'COLUMBIA', 'MO', 'COLUMBIA REGIONAL AIRPORT', 38.8169, -92.2183, 893, 898, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03945');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03947', 'KANSAS CITY', 'MO', 'KANSAS CITY INTERNATIONAL AIRPORT', 39.2972, -94.7306, 1005, 1025, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03948', 'NORMAN', 'OK', 'UNIVERSITY OF OKLAHOMA WESTHEIMER AIRPORT', 35.25, -97.46667, 1182, 1182, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03948');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03949', 'FORT POLK', 'LA', 'PEASON RIDGE', 31.4, -93.28333, 365, 363, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03949');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03950', 'LAWTON', 'OK', 'LAWTON-FORT SILL RGNL ARPT', 34.5584, -98.4172, 1069, 1108, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03952', 'NORTH LITTLE ROCK', 'AR', 'NORTH LITTLE ROCK AIRPORT', 34.8353, -92.2596, 563, 563, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03952');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03953', 'JONESBORO', 'AR', 'JONESBORO MUNICIPAL ARPT', 35.83111, -90.64639, 262, 274, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03954', 'OKLAHOMA CITY', 'OK', 'WILEY POST AIRPORT', 35.53417, -97.64694, 1297, 1299, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03954');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03957', 'MEDICINE LODGE', 'KS', 'MEDICINE LODGE', 37.2839, -98.5528, 1535, 1540, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03958', 'ALTON/ST LOUIS', 'IL', 'ST LOUIS REGIONAL AIRPORT', 38.88333, -90.05, 543, 544, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03959', 'BARTLESVILLE', 'OK', 'BARTLESVILLE MUNICIPAL AIRPORT', 36.7683, -96.0261, 715, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03960', 'CAHOKIA/ST.LOUIS', 'IL', 'ST LOUIS DOWNTOWN AIRPORT', 38.57139, -90.15722, 413, 413, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03961', 'NATCHEZ', 'MS', 'HRDY-ANRES FD NTCHZ-ADAMS COUNTY AIRPORT', 31.61667, -91.28333, 272, 272, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03962', 'HOT SPRINGS', 'AR', 'MEMORIAL FIELD AIRPORT', 34.29, -93.06, 535, 555, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03963', 'JEFFERSON CITY', 'MO', 'JEFFERSON CITY MEMO ARPT', 38.59111, -92.15583, 573, 573, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03963');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03965', 'STILLWATER', 'OK', 'STILLWATER REGIONAL ARPT', 36.1624, -97.0894, 984, 984, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03965');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03966', 'ST LOUIS', 'MO', 'SPIRIT OF ST LOUIS AIRPORT', 38.65722, -90.65583, 462, 462, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03967', 'OLATHE', 'KS', 'JOHNSON CO EXECUTIVE ARPT', 38.85, -94.73917, 1070, 1072, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03968', 'HAYS', 'KS', 'HAYS REGIONAL AIRPORT', 38.85, -99.26667, 1998, 1998, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03969', 'STEPHENVILLE', 'TX', 'CLARK FIELD MUNI AIRPORT', 32.21528, -98.1775, 1321, 1321, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03969');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03970', 'DALLAS', 'TX', 'ADDISON AIRPORT', 32.96861, -96.83639, 644, 644, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03970');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03971', 'DALLAS', 'TX', 'DALLAS EXECUTIVE AIRPORT', 32.68083, -96.86806, 658, 671, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03972', 'KILLEEN', 'TX', 'KILLEEN MUNICIPAL AIRPORT', 31.08333, -97.68333, 841, 848, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03973', 'SPRINGDALE', 'AR', 'SPRINGDALE MUNICIPAL ARPT', 36.16667, -94.11667, 1353, 1353, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03974', 'WICHITA', 'KS', 'COLONEL JAMES JABARA ARPT', 37.74611, -97.22111, 1421, 1420, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03974');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03975', 'POPLAR BLUFF', 'MO', 'POPLAR BUFF MUNICIAL ARPT', 36.7725, -90.32472, 327, 328, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03976', 'DE RIDDER', 'LA', 'BEAUREGARD REGIONAL AIRPORT', 30.83333, -93.33333, 204, 202, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03977', 'ELK CITY', 'OK', 'ELK CITY MUNICIPAL AP', 35.43333, -99.4, 2002, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03978', 'BENTONVILLE', 'AR', 'BENTONVILLE MUNI/LOUIS M THADEN FIELD AIRPORT', 36.35, -94.21667, 1296, 1296, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03981', 'FREDERICK', 'OK', 'FREDERICK MUNICIPAL AIRPT', 34.21, -98.59, 1255, 1255, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03981');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03985', 'FORT WORTH', 'TX', 'FORT WORTH SPINKS AIRPORT', 32.565, -97.30806, 700, 700, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03991', 'DENTON', 'TX', 'DENTON MUNICIPAL AIRPORT', 33.20611, -97.19889, 642, 642, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03994', 'SEDALIA', 'MO', 'SEDALIA MEMORIAL AIRPORT', 38.70417, -93.18333, 900, 905, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03996', 'TALLULAH/VICKSBURG', 'LA', 'VICKSBURG/TALLULAH RGNL AP', 32.35, -91.02778, 86, 86, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03997', 'LAWRENCE', 'KS', 'LAWRENCE MUNICIPAL AIRPORT', 39.00778, -95.21167, 832, 832, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03998', 'PARSONS', 'KS', 'TRI-CITY AIRPORT', 37.32778, -95.50417, 869, 899, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03998');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '03999', 'BURNET', 'TX', 'BURNET MUNICIPAL CRADOCK FIELD AIRPORT', 30.7406, -98.2354, 1288, 1288, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='03999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04109', 'ELK CITY', 'ID', 'ELK CITY', 46.14417, -115.59639, 1480, 1480, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04109');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04110', 'JEROME', 'ID', 'JEROME COUNTY AIRPORT', 42.72667, -114.45639, 4048, 4048, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04110');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04111', 'EVANSTON', 'WY', 'EVAN-UINTA CO BURNS FLD AP', 41.27306, -111.03056, 7163, 7144, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04111');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04112', 'STANLEY', 'ID', 'STANLEY RANGER STATION', 44.17083, -114.92722, 6495, 6495, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04112');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04113', 'HERMISTON', 'OR', 'HERMISTON MUNICIPAL ARPT', 45.82583, -119.26111, 641, 641, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04113');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04114', 'CHALLIS', 'ID', 'CHALLIS AIRPORT', 44.52278, -114.215, 5033, 5040, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04114');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04116', 'SALT LAKE CITY', 'UT', 'SALT LAKE CITY MUNI 2 ARPT', 40.61944, -111.99278, 4603, 4603, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04116');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04125', 'JOHN DAY', 'OR', 'JOHN DAY 35 WNW', 44.556, -119.6459, 2245, 2245, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04125');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04126', 'ARCO', 'ID', 'ARCO 17 SW', 43.4621, -113.556, 5920, 5920, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04126');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04127', 'MURPHY', 'ID', 'MURPHY 10 W', 43.2044, -116.7505, 3950, 3950, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04127');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04128', 'RILEY', 'OR', 'RILEY 10 WSW', 43.4711, -119.6917, 4583, 4587, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04128');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04129', 'SANDPOINT', 'ID', 'SANDPOINT AIRPORT', 48.29944, -116.56, 2127, 2127, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04130', 'ST. MARY', 'MT', 'ST. MARY 1 SSW', 48.7412, -113.433, 4555, 4555, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04130');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04131', 'MOOSE', 'WY', 'MOOSE 1 NNE', 43.6615, -110.712, 6466, 6466, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04133', 'HEBER', 'UT', 'HEBER CITY MUNI ARPT-RUSS MCDONALD FIELD', 40.48194, -111.42889, 5637, 5637, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04133');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04134', 'DUGWAY PROVING GROUND', 'UT', 'GRANITE PEAK DUGWAY PROVING GROUND', 40.16056, -113.35083, 4300, 4300, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04134');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04135', 'DRIGGS', 'ID', 'DRIGGS-REED MEMORIAL AIRPORT', 43.7425, -111.0975, 6229, 6229, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04135');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04136', 'SPOKANE', 'WA', 'SPOKANE 17 SSW', 47.4174, -117.5264, 2267, 2267, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04137', 'DILLON', 'MT', 'DILLON 18 WSW', 45.158, -113.0057, 5971, 5971, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04137');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04138', 'BRIGHAM CITY', 'UT', 'BRIGHAM CITY 28 WNW', 41.6163, -112.5437, 4951, 4951, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04138');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04139', 'DENIO', 'NV', 'DENIO 52 WSW', 41.8484, -119.6357, 6500, 6500, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04139');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04140', 'LEWISTOWN', 'MT', 'LEWISTOWN 42 WSW', 46.8847, -110.2895, 5070, 5070, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04140');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04141', 'COOS BAY', 'OR', 'COOS BAY 8 SW', 43.2718, -124.3186, 12, 12, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04143', 'PROVO', 'UT', 'PROVO 22 E', 40.2816, -111.2398, 7808, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04143');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04201', 'SCAPPOOSE', 'OR', 'SCAPPOOSE INDUS AIRPK ARPT', 45.77278, -122.86111, 55, 55, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04201');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04204', 'SHELTER COVE', 'CA', 'SHELTER COVE AIRPORT', 40.03306, -124.06778, 69, 69, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04204');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04205', 'ARLINGTON', 'WA', 'ARLINGTON MUNICIPAL ARPT', 48.16056, -122.15889, 137, 137, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04205');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04221', 'ROCHE HARBOR', 'WA', 'ROCHE HARBOR SEAPLANE BASE', 48.60806, -123.15944, 0, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04221');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04222', 'REDDING', 'CA', 'REDDING 12 WNW', 40.6507, -122.6068, 1418, 1418, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04222');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04223', 'DARRINGTON', 'WA', 'DARRINGTON 21 NNE', 48.5405, -121.446, 407, 407, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04223');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04224', 'EASTSOUND', 'WA', 'ORCAS ISLAND AIRPORT', 48.70833, -122.91056, 31, 31, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04224');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04236', 'CORVALLIS', 'OR', 'CORVALLIS 10 SSW', 44.4185, -123.3257, 312, 312, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04236');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04237', 'QUINAULT', 'WA', 'QUINAULT 4 NE', 47.5139, -123.812, 286, 286, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04237');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04720', 'JAMESTOWN', 'NY', 'CHAUTAUQUA CO/JAMESTOWN AP', 42.15, -79.25, 1723, 1723, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04720');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04724', 'NIAGARA FALLS', 'NY', 'NIAGARA FALLS INTL AIRPORT', 43.10833, -78.93806, 585, 587, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04724');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04725', 'BINGHAMTON', 'NY', 'GREATER BINGHAMTON/E A LINK FIELD AP', 42.2068, -75.98, 1595, 1638, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04725');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04726', 'JOHNSTOWN', 'PA', 'J MURTHA JTWN-CMBRA CO APT', 40.31611, -78.83389, 2284, 2284, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04726');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04741', 'SCHENECTADY', 'NY', 'SCHENECTADY COUNTY AIRPORT', 42.85, -73.95, 378, 378, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04741');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04743', 'PORTSMOUTH', 'NH', 'PEASE INTERNATIONAL TRADEPORT AIRPORT', 43.08333, -70.81667, 100, 100, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04743');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04751', 'BRADFORD', 'PA', 'BRADFORD REGIONAL AIRPORT', 41.8, -78.6333, 2142, 2150, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04751');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04780', 'FITCHBURG', 'MA', 'FITCHBURG MUNICIPAL ARP', 42.55194, -71.75583, 348, 348, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04780');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04781', 'ISLIP', 'NY', 'LONG ISLAND MAC ARTHUR AIRPORT', 40.79389, -73.10167, 84, 99, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04781');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04787', 'DU BOIS', 'PA', 'DU BOIS-JEFFERSON CO ARPT', 41.17833, -78.89889, 1814, 1824, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04787');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04789', 'MONTGOMERY', 'NY', 'ORANGE COUNTY AIRPORT', 41.50917, -74.265, 365, 365, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04789');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04803', 'RHINELANDER', 'WI', 'RHINELANDER-ONEIDA CO ARPT', 45.6314, -89.4823, 1624, 1624, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04804', 'COLUMBUS', 'OH', 'OHIO STATE UNIVERSITY ARPT', 40.07806, -83.07806, 905, 905, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04804');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04805', 'CLEVELAND', 'OH', 'CUYAHOGA COUNTY AIRPORT', 41.56667, -81.48333, 879, 879, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04805');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04806', 'ELKHART', 'IN', 'ELKHART MUNICIPAL AIRPORT', 41.71667, -85.98333, 778, 778, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04807', 'GARY', 'IN', 'GARY/CHICAGO AIRPORT', 41.61667, -87.41667, 591, 592, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04807');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04808', 'CHICAGO/AURORA', 'IL', 'AURORA MUNICIPAL AIRPORT', 41.77, -88.48139, 710, 714, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04813', 'BEAVER FALLS', 'PA', 'BEAVER COUNTY AIRPORT', 40.76667, -80.4, 1252, 1253, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04824', 'STURGEON BAY', 'WI', 'DOOR CO CHERRYLAND AIRPORT', 44.85, -87.41667, 724, 724, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04825', 'APPLETON', 'WI', 'OUTAGAMIE CO RGNL AIRPORT', 44.26667, -88.51667, 917, 918, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04826', 'WISCONSIN RAPIDS', 'WI', 'ALEXANDER FD S WOOD CO APT', 44.35917, -89.83694, 1019, 1021, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04827', 'WASHINGTON', 'PA', 'WASHINGTON COUNTY AIRPORT', 40.13333, -80.28333, 1185, 1184, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04828', 'ANDERSON', 'IN', 'ANRSN MUNI DRNGTON FLD AP', 40.11667, -85.61667, 900, 919, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04828');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04829', 'SAGINAW', 'MI', 'SAGNAW CO H.W. BROWNE ARPT', 43.43333, -83.86667, 600, 600, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04829');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04831', 'CHICAGO/ROMEOVILLE', 'IL', 'LEWIS UNIVERSITY AIRPORT', 41.6041, -88.085, 640, 673, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04831');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04836', 'FRENCHVILLE', 'ME', 'NRN AROOSTOOK REGIONAL APT', 47.28556, -68.31333, 988, 1000, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04836');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04838', 'CHICAGO/PROSPECT HGTS/WHEELI', 'IL', 'PALWAUKEE MUNICIPAL ARPT', 42.12083, -87.90472, 636, 647, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04839', 'HOLLAND', 'MI', 'TULIP CITY AIRPORT', 42.74611, -86.09667, 689, 688, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04840', 'FOND DU LAC', 'WI', 'FOND DU LAC COUNTY AIRPORT', 43.76944, -88.49083, 807, 807, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04840');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04841', 'SHEBOYGAN', 'WI', 'SHEBOYGAN CO MEMO AIRPORT', 43.76944, -87.85056, 746, 749, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04842', 'WOOSTER', 'OH', 'WAYNE COUNTY AIRPORT', 40.87306, -81.88667, 1137, 1137, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04843', 'MEADVILLE', 'PA', 'PORT MEADVILLE AIRPORT', 41.62639, -80.215, 1400, 1400, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04843');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04845', 'KENOSHA', 'WI', 'KENOSHA REGIONAL AIRPORT', 42.595, -87.93806, 743, 743, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04845');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04846', 'VALPARAISO', 'IN', 'PORTER COUNTY MUNICIPAL AIRPORT', 41.4525, -87.00583, 770, 770, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04847', 'ADRIAN', 'MI', 'ADRIAN LENAWEE COUNTY ARPT', 41.86778, -84.07944, 796, 798, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04848', 'TOLEDO', 'OH', 'METCALF FIELD AIRPORT', 41.56306, -83.47639, 622, 624, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04848');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04849', 'LORAIN/ELYRIA', 'OH', 'LORAIN COUNTY RGNL AIRPORT', 41.34611, -82.17944, 794, 794, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04850', 'LIMA', 'OH', 'LIMA ALLEN COUNTY AIRPORT', 40.7075, -84.02722, 975, 975, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04851', 'DEFIANCE', 'OH', 'DEFIANCE MEMORIAL AIRPORT', 41.3375, -84.42889, 707, 708, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04851');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04852', 'NEW PHILADELPHIA', 'OH', 'HARRY CLEVER FIELD AIRPORT', 40.47194, -81.42361, 895, 895, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04853', 'CLEVELAND', 'OH', 'CLEVELAND BURKE LAKEFNT AP', 41.5175, -81.68361, 584, 584, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04854', 'GAYLORD', 'MI', 'OTSEGO COUNTY AIRPORT', 45.01333, -84.70139, 1335, 1328, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04854');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04855', 'MARION', 'OH', 'MARION MUNICIPAL AIRPORT', 40.61611, -83.06361, 994, 990, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04856', 'SEUL CHOIX POINT', 'MI', 'SEUL CHOIX POINT', 45.95, -86.22972, 584, 584, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04856');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04857', 'ASHTABULA', 'OH', 'ASHTABULA COUNTY AIRPORT', 41.77806, -80.69583, 924, 924, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04857');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04858', 'NEWARK', 'OH', 'NEWARK-HEATH AIRPORT', 40.02278, -82.4625, 884, 884, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04862', 'LINCOLN', 'IL', 'LOGAN COUNTY AIRPORT', 40.15833, -89.33472, 597, 597, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04862');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04863', 'BELLAIRE', 'MI', 'ANTRIM COUNTY AIRPORT', 44.98639, -85.20306, 620, 623, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04863');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04864', 'ANTIGO', 'WI', 'LANGLADE COUNTY AIRPORT', 45.15417, -89.11056, 1522, 1522, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04864');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04865', 'MINOCQUA-WOODRUFF', 'WI', 'LKLND/NOBL F LEE MEM FD AP', 45.92778, -89.73083, 1630, 1630, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04865');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04866', 'BURLINGTON', 'WI', 'BURLINGTON MUNICIPAL ARPT', 42.69, -88.30361, 779, 779, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04866');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04867', 'MORRIS', 'IL', 'MORS MUNI-J.R. WSBRN FD AP', 41.42528, -88.41861, 584, 584, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04867');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04868', 'LACON', 'IL', 'MARSHALL COUNTY AIRPORT', 41.01917, -89.38639, 568, 568, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04869', 'SAULT STE MARIE', 'MI', 'CHIPPEWA COUNTY INTL ARPT', 46.25, -84.46667, 799, 800, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04869');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04870', 'CHARLEVOIX', 'MI', 'CHARLEVOIX MUNICIPAL ARPT', 45.30472, -85.27472, 646, 670, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04871', 'DE KALB', 'IL', 'DE KALB TAYLOR MUNI ARPT', 41.93167, -88.70806, 915, 915, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04872', 'LAMBERTVILLE', 'MI', 'TOLEDO SUBURBAN AIRPORT', 41.73583, -83.65528, 669, 669, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04872');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04873', 'MONROE', 'WI', 'MONROE MUNICIPAL AIRPORT', 42.61472, -89.59056, 1085, 1085, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04873');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04874', 'NEWBERRY', 'MI', 'LUCE COUNTY AIRPORT', 46.31111, -85.45722, 923, 869, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04874');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04875', 'WEST BEND', 'WI', 'WEST BEND MUNICIPAL ARPT', 43.41667, -88.13333, 884, 887, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04875');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04876', 'FREEPORT', 'IL', 'ALBERTUS AIRPORT', 42.24611, -89.58222, 859, 859, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04876');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04877', 'CHARLOTTE', 'MI', 'FITCH H BEACH AIRPORT', 42.57444, -84.81139, 878, 891, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04877');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04878', 'GRAYLING', 'MI', 'GRAYLING ARMY AIRFIELD', 44.68028, -84.72861, 1149, 1158, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04878');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04879', 'CHICAGO', 'IL', 'LANSING MUNICIPAL AP', 41.53972, -87.53194, 616, 616, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04879');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04880', 'KANKAKEE', 'IL', 'GREATER KANKAKEE AIRPORT', 41.12139, -87.84611, 629, 1980, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04880');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04881', 'MANISTIQUE', 'MI', 'SCHOOLCRAFT COUNTY AIRPORT', 45.98333, -86.18333, 686, 685, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04881');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04882', 'HILLSDALE', 'MI', 'HILLSDALE MUNICIPAL ARPT', 41.92111, -84.58556, 1182, 1182, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04883', 'LUDINGTON', 'MI', 'MASON COUNTY AIRPORT', 43.9625, -86.40778, 640, 645, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04884', 'HARBOR SPRINGS', 'MI', 'HARBOR SPRINGS AIRPORT', 45.42556, -84.91333, 677, 686, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04884');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04885', 'MOUNT PLEASANT', 'MI', 'MOUNT PLEASANT MUNI ARPT', 43.62167, -84.73722, 755, 755, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04885');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04886', 'COLDWATER', 'MI', 'BRANCH COUNTY MEMO AIRPORT', 41.93333, -85.0525, 959, 959, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04887', 'HOWELL', 'MI', 'LIVINGSTON COUNTY AIRPORT', 42.62944, -83.98417, 944, 960, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04887');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04888', 'PORT HURON', 'MI', 'ST CLAIR COUNTY INTL ARPT', 42.91083, -82.52861, 640, 650, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04888');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04889', 'PONTIAC', 'IL', 'PONTIAC MUNICIPAL AIRPORT', 40.92361, -88.62528, 659, 659, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04890', 'ROCHELLE', 'IL', 'ROCHELLE MUNI ARPT-KORITZ FIELD AIRPORT', 41.89278, -89.07806, 781, 781, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04890');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04891', 'MERRILL', 'WI', 'MERRILL MUNICIPAL AIRPORT', 45.19917, -89.71139, 1317, 1317, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04892', 'BEAVER ISLAND', 'MI', 'BEAVER ISLAND AIRPORT', 45.68889, -85.56611, 666, 669, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04892');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04893', 'CHEBOYGAN', 'MI', 'CHEBOYGAN COUNTY AIRPORT', 45.65361, -84.51917, 635, 641, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04894', 'STERLING/ROCKFALLS', 'IL', 'WHITESIDE CO ARPT-JOS H BITTOR F FLD ARPT', 41.74278, -89.67611, 647, 647, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04895', 'STEVENS POINT', 'WI', 'STEVENS POINT MUNI ARPT', 44.55, -89.53333, 1110, 1110, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04896', 'RANTOUL', 'IL', 'RANTOUL NATL AVN CNTR-F ELLIOTT FIELD AIRPORT', 40.29333, -88.14222, 737, 737, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04897', 'WAUKESHA', 'WI', 'WAUKESHA COUNTY AIRPORT', 43.04083, -88.23694, 911, 911, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04898', 'JUNEAU', 'WI', 'DODGE COUNTY AIRPORT', 43.42639, -88.70306, 936, 936, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04899', 'PERU', 'IL', 'ILLINOIS VALLEY RGNL-WALTER DUNCAN FLD ARPT', 41.35167, -89.15306, 654, 654, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04899');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04901', 'AURORA', 'NE', 'ARRA MUNI-AL POTTER FLD AP', 40.89306, -97.99722, 1799, 1804, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04902', 'AUSTIN', 'MN', 'AUSTIN MUNICIPAL AIRPORT', 43.66472, -92.93333, 1233, 1233, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04903', 'WASHINGTON', 'IA', 'WASHINGTON MUNICIPAL ARPT', 41.27583, -91.67333, 754, 754, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04903');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04904', 'ALGONA', 'IA', 'ALGONA MUNICIPAL AIRPORT', 43.07778, -94.27194, 1219, 1219, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04905', 'SILVER BAY', 'MN', 'SILVER BAY MUNICIPAL ARPT', 47.24889, -91.41556, 1089, 1089, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04905');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04906', 'BOONE', 'IA', 'BOONE MUNICIPAL AIRPORT', 42.04944, -93.8475, 1160, 1160, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04906');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04907', 'CLARION', 'IA', 'CLARION MUNICIPAL AIRPORT', 42.74167, -93.75889, 1162, 1162, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04907');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04908', 'COUNCIL BLUFFS', 'IA', 'COUNCIL BLUFFS MUNI ARPT', 41.25944, -95.75972, 1253, 1253, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04909', 'CAMBRIDGE', 'MN', 'CAMBRIDGE MUNICIPAL ARPT', 45.55944, -93.265, 945, 945, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04910', 'CARROLL', 'IA', 'ARTHUR N. NEU AIRPORT', 42.04611, -94.78889, 1204, 1204, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04911', 'GRAND MARAIS', 'MN', 'GRAND MARAIS/COOK CO ARPT', 47.83806, -90.38306, 1798, 1798, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04912', 'CROOKSTON', 'MN', 'CRKSTN MUNI KRKWOD FLD APT', 47.84167, -96.62139, 899, 899, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04912');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04913', 'CHARITON', 'IA', 'CHARITON MUNICIPAL AIRPORT', 41.01944, -93.35944, 1050, 1050, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04913');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04914', 'CLOQUET', 'MN', 'CLOQUET CARLTON CO AIRPORT', 46.7025, -92.50417, 1279, 1280, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04914');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04915', 'CRESTON', 'IA', 'CRESTON MUNICIPAL AIRPORT', 41.00722, -94.36306, 1294, 1294, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04915');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04916', 'DECORAH', 'IA', 'DECORAH MUNICIPAL AIRPORT', 43.27528, -91.73917, 1157, 1157, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04916');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04917', 'DENISON', 'IA', 'DENISON MUNICIPAL AIRPORT', 41.98639, -95.38056, 1276, 1276, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04917');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04918', 'MADISON', 'MN', 'MDSON-LAC QUI PARLE CO APT', 44.98611, -96.04333, 1082, 1082, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04919', 'DULUTH', 'MN', 'SKY HARBOR AIRPORT', 46.72167, -92.04333, 610, 610, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04920', 'WEBSTER CITY', 'IA', 'WEBSTER CITY MUNI AIRPORT', 42.43639, -93.86861, 1121, 1121, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04920');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04921', 'KEOKUK', 'IA', 'KEOKUK MUNICIPAL AIRPORT', 40.45972, -91.42833, 671, 671, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04921');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04922', 'WHEATON', 'MN', 'WHEATON MUNICIPAL AIRPORT', 45.77972, -96.54472, 1025, 1027, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04922');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04923', 'EVELETH', 'MN', 'EVELETH-VIRGINIA MUNI ARPT', 47.42417, -92.4975, 1379, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04923');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04924', 'FREMONT', 'NE', 'FREMONT MUNICIPAL AIRPORT', 41.44889, -96.52, 1203, 1203, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04924');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04925', 'FAIRFIELD', 'IA', 'FAIRFIELD MUNICIPAL ARPT', 41.05306, -91.97889, 799, 799, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04926', 'FLAG ISLAND', 'MN', 'FLAG ISLAND', 49.31833, -94.90278, 1080, 1080, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04927', 'PRESTON', 'MN', 'FILLMORE COUNTY AIRPORT', 43.67667, -92.17972, 1276, 1276, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04928', 'BIGFORK', 'MN', 'BIGFORK MUNICIPAL AIRPORT', 47.78278, -93.65, 1349, 1349, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04929', 'FOSSTON', 'MN', 'FOSSTON MUNICIPAL AIRPORT', 47.59278, -95.77528, 1277, 1273, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04930', 'FORT MADISON', 'IA', 'FORT MADISON MUNICIPAL APT', 40.65917, -91.32667, 724, 724, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04931', 'GLENWOOD', 'MN', 'GLENWOOD MUNICIPAL AIRPORT', 45.64389, -95.32028, 1393, 1393, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04932', 'GLENCOE', 'MN', 'GLENCOE MUNICIPAL AIRPORT', 44.75583, -94.08111, 992, 992, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04933', 'HUTCHINSON', 'MN', 'HTCHSON MUNI-BUTLR FLD APT', 44.85889, -94.38167, 1060, 1060, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04934', 'HALLOCK', 'MN', 'HALLOCK MUNICIPAL AIRPORT', 48.7525, -96.94278, 819, 819, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04934');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04935', 'HOLDREGE', 'NE', 'BREWSTER FIELD AIRPORT', 40.45, -99.33917, 2304, 2309, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04936', 'HARLAN', 'IA', 'HARLAN MUNICIPAL AIRPORT', 41.58417, -95.33944, 1231, 1231, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04936');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04937', 'CLARINDA', 'IA', 'SCHENCK FIELD AIRPORT', 40.72167, -95.02639, 992, 992, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04938', 'ANKENY', 'IA', 'ANKENY REGIONAL AIRPORT', 41.69111, -93.56611, 910, 910, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04938');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04939', 'MOORHEAD', 'MN', 'MOORHEAD MUNICIPAL AIRPORT', 46.83917, -96.66306, 917, 917, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04939');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04940', 'MORA', 'MN', 'MORA MUNICIPAL AIRPORT', 45.88583, -93.27167, 1012, 1012, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04941', 'LITCHFIELD', 'MN', 'LITCHFIELD MUNICIPAL ARPT', 45.14694, -94.50722, 1140, 1140, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04941');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04942', 'LE MARS', 'IA', 'LE MARS MUNICIPAL AIRPORT', 42.77778, -96.19361, 1196, 1196, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04943', 'MINNEAPOLIS', 'MN', 'AIRLAKE AIRPORT', 44.62778, -93.22806, 960, 960, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04944', 'LITTLE FALLS', 'MN', 'LITL FALS/MRSON CO-LNBRG FIELD AIRPORT', 45.94917, -94.34694, 1122, 1122, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04944');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04945', 'MAPLE LAKE', 'MN', 'MAPLE LAKE MUNICIPAL ARPT', 45.23583, -93.98556, 1028, 1028, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04945');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04946', 'JACKSON', 'MN', 'JACKSON MUNICIPAL AIRPORT', 43.65, -94.98639, 1446, 1446, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04946');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04947', 'MOUNT PLEASANT', 'IA', 'MOUNT PLEASANT MUNI ARPT', 40.94667, -91.51111, 734, 734, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04948', 'MORRIS', 'MN', 'MORRIS MUNICIPAL AIRPORT', 45.56639, -95.9675, 1130, 1130, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04948');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04949', 'MACOMB', 'IL', 'MACOMB MUNICIPAL AIRPORT', 40.52028, -90.65222, 707, 707, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04949');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04950', 'MUSCATINE', 'IA', 'MUSCATINE MUNICIPIAL ARPT', 41.36667, -91.15, 547, 548, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04951', 'MONTEVIDEO', 'MN', 'MONTIVIDEO-CHIPPEWA CO APT', 44.96889, -95.71, 1034, 1034, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04951');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04952', 'WINDOM', 'MN', 'WINDOM MUNICIPAL AIRPORT', 43.91333, -95.10917, 1410, 1410, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04952');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04953', 'MONTICELLO', 'IA', 'MONTICELLO REGIONAL ARPT', 42.22389, -91.16556, 849, 849, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04954', 'MOOSE LAKE', 'MN', 'MOOSE LAKE CARLTON CO ARPT', 46.41861, -92.80444, 1076, 1076, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04954');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04955', 'OELWEIN', 'IA', 'OELWEIN MUNICIPAL AIRPORT', 42.68083, -91.97444, 1076, 1076, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04955');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04956', 'WINONA', 'MN', 'WIOA MUNI-MAX CONRAD FD AP', 44.07694, -91.70806, 656, 656, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04956');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04957', 'O''NEILL', 'NE', 'THE O''NEILL MUNI-JOHN L BAKER FIELD AIRPORT', 42.46972, -98.68806, 2031, 2031, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04958', 'ORR', 'MN', 'ORR REGIONAL AIRPORT', 48.01583, -92.85583, 1311, 1311, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04959', 'ORANGE CITY', 'IA', 'ORANGE CITY MUNICIPAL ARPT', 42.99, -96.06278, 1414, 1414, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04960', 'OLIVIA', 'MN', 'OLIVIA REGIONAL AIRPORT', 44.77861, -95.03278, 1076, 1076, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04961', 'OWATONNA', 'MN', 'OWATONNA DEGNER RGNL ARPT', 44.12333, -93.26056, 1146, 1146, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04962', 'KNOXVILLE', 'IA', 'KNOXVILLE MUNICIPAL ARPT', 41.29861, -93.11361, 926, 926, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04963', 'PRAIRIE DU CHIEN', 'WI', 'PRAIRIE DU CHIEN MUNI ARPT', 43.01917, -91.12361, 661, 661, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04963');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04964', 'PELLA', 'IA', 'PELLA MUNICIPAL AIRPORT', 41.4, -92.94583, 885, 885, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04964');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04965', 'PIPESTONE', 'MN', 'PIPESTONE MUNICIPAL ARPT', 43.98306, -96.30028, 1736, 1736, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04965');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04966', 'RED OAK', 'IA', 'RED OAK MUNICIPAL AIRPORT', 41.01028, -95.25972, 1044, 1044, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04967', 'RED WING', 'MN', 'RED WING REGIONAL AIRPORT', 44.58917, -92.48472, 780, 780, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04968', 'NEW RICHMOND', 'WI', 'NEW RICHMOND MUNICIPAL APT', 45.15, -92.53333, 996, 996, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04969', 'RUSH CITY', 'MN', 'RUSH CITY REGIONAL AIRPORT', 45.69778, -92.95278, 923, 923, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04969');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04970', 'ROSEAU', 'MN', 'ROSEAU MUNI/RUDY BILLBERG FLD ARPT', 48.85583, -95.69694, 1059, 1059, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04970');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04971', 'WARROAD', 'MN', 'WARROAD INTL-SWEDE CARLSON FLD ARPT', 48.94139, -95.34833, 1074, 1074, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04972', 'SIREN', 'WI', 'BURNETT COUNTY AIRPORT', 45.8225, -92.3725, 989, 989, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04973', 'SHENANDOAH', 'IA', 'SHENANDOAH MUNICIPAL ARPT', 40.75139, -95.41333, 971, 971, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04974', 'SOUTH ST PAUL', 'MN', 'SOUTH ST PAUL MUNI-RICHARD E FLEMING FLD ARPT', 44.85694, -93.03278, 820, 220, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04974');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04975', 'SHELDON', 'IA', 'SHELDON MUNICIPAL AIRPORT', 43.20833, -95.83333, 1419, 1419, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04976', 'STORM LAKE', 'IA', 'STORM LAKE MUNICIPAL ARPT', 42.59694, -95.24056, 1488, 1488, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04977', 'NEWTON', 'IA', 'NEWTON MUNICIPAL AIRPORT', 41.67417, -93.02167, 953, 953, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04978', 'DODGE CENTER', 'MN', 'DODGE CENTER ARPT', 44.01778, -92.83139, 1305, 1305, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04979', 'TWO HARBORS', 'MN', 'RICHARD B HELGESON AIRPORT', 47.04917, -91.745, 1080, 1080, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04979');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04980', 'NEW ULM', 'MN', 'NEW ULM MUNICIPAL AIRPORT', 44.31944, -94.50222, 1011, 1011, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04980');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04981', 'VINTON', 'IA', 'VINTON VETERANS MEMORIAL AIRPARK AIRPORT', 42.21861, -92.02583, 845, 845, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04981');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04982', 'ORTHONVILLE', 'MN', 'ORTHONVILLE MUNI-MARTINSON FIELD AIRPORT', 45.30556, -96.42417, 1100, 1100, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04982');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04983', 'WASKISH', 'MN', 'WASKISH MUNICIPAL AIRPORT', 48.15389, -94.51667, 1181, 1181, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04983');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04984', 'LONGVILLE', 'MN', 'LONGVILLE MUNICIPAL ARPT', 46.99, -94.20389, 1334, 1334, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04984');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04989', 'INDEPENDENCE', 'IA', 'INDEPENDENCE MUNICIPAL APT', 42.45333, -91.9475, 978, 978, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04990', 'SIOUX FALLS', 'SD', 'SIOUX FALLS 14 NNE', 43.7346, -96.6222, 1594, 1594, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04992', 'OMAHA', 'NE', 'MILLARD AIRPORT', 41.19611, -96.11194, 1049, 1051, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04993', 'NEBRASKA CITY', 'NE', 'NEBRASKA CITY MUNI AIRPORT', 40.60639, -95.86361, 1153, 1162, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04994', 'GOODRIDGE', 'MN', 'GOODRIDGE 12 NNW', 48.3055, -95.8744, 1150, 1150, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04995', 'MENOMONIE', 'WI', 'MENOMONIE MUNICIPAL AIRPORT-SCORE FIELD', 44.89222, -91.86778, 895, 895, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04995');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04996', 'SAVANNA', 'IL', 'TRI-TOWNSHIP AIRPORT', 42.04583, -90.10806, 616, 616, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04997', 'MCGREGOR', 'MN', 'ISEDOR IVERSON AIRPORT', 46.61889, -93.30972, 1228, 1228, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04998', 'HEBRON', 'NE', 'HEBRON MUNICIPAL AIRPORT', 40.14917, -97.58667, 1473, 1466, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04998');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '04999', 'GRANITE FALLS', 'MN', 'GRANITE FALLS MUNI ARPT/LENZEN-ROE MEMORIAL FIELD', 44.75333, -95.55611, 1047, 1047, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='04999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11603', 'AGUADILLA', 'PR', 'RAFAEL HERNANDEZ AIRPORT', 18.4977, -67.1294, 218, 237, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11603');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11624', 'CHRISTIANSTED', 'VI', 'HENRY E ROHLSEN AIRPORT', 17.6997, -64.8125, 61, 55, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11624');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11630', 'ROOSEVELT ROADS', 'PR', 'NAVAL STATION ROOSEVELT ROADS, CEIBA', 18.255, -65.64083, 33, 38, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11630');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11634', 'PIARCO', 'NULL', 'PIARCO INTL AP', 10.58333, -61.35, 40, NULL, 4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11634');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11640', 'CHARLOTTE AMALIE', 'VI', 'CYRIL E KING AIRPORT', 18.3363, -64.98, 20, 11, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11640');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11641', 'SAN JUAN', 'PR', 'LUIS MUNOZ MARIN INTERNATIONAL AP', 18.4325, -66.01083, 9, 8, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11641');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11653', 'MAYAQUEZ', 'PR', 'EUGENIO MARIA DE HOSTOS AIRPORT', 18.25389, -67.14861, 38, 28, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11653');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11706', 'GUANTANAMO', 'NULL', 'GUANTANAMO BAY NAVAL AIR STATION', 19.9, -75.15, 131, 56, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11706');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '11813', 'GRAND CAYMAN', 'NULL', 'OWEN ROBERTS AIRPORT', 19.3, -81.36667, 10, 6, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='11813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12803', 'BOCA RATON', 'FL', 'BOCA RATON AIRPORT', 26.37833, -80.1075, 13, 13, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12809', 'BARTOW', 'FL', 'BARTOW MUNICIPAL AIRPORT', 27.95, -81.78333, 125, 125, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12810', 'TAMPA', 'FL', 'MAC DILL AFB AIRPORT', 27.85, -82.51667, 14, 14, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12812', 'PUNTA GORDA', 'FL', 'CHARLOTTE COUNTY AIRPORT', 26.91722, -81.99139, 25, 25, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12812');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12815', 'ORLANDO', 'FL', 'ORLANDO INTERNATIONAL AIRPORT', 28.4339, -81.325, 90, 106, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12816', 'GAINESVILLE', 'FL', 'GAINESVILLE REGIONAL AIRPORT', 29.6919, -82.2755, 123, 165, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12818', 'BROOKSVILLE', 'FL', 'HERNANDO COUNTY AIRPORT', 28.47361, -82.45444, 77, 77, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12819', 'LEESBURG', 'FL', 'LEESBURG REGIONAL AIRPORT', 28.82083, -81.80972, 77, 77, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12826', 'HOMESTEAD', 'FL', 'HOMESTEAD AFB AIRPORT', 25.48333, -80.38333, 5, 7, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12832', 'APALACHICOLA', 'FL', 'APALACHICOLA MUNI AIRPORT', 29.73333, -85.03333, 19, 19, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12832');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12833', 'CROSS CITY', 'FL', 'CROSS CITY AIRPORT', 29.63333, -83.10528, 38, 42, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12834', 'DAYTONA BEACH', 'FL', 'DAYTONA BEACH INTERNATIONAL AIRPORT', 29.1828, -81.0483, 31, 41, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12834');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12835', 'FORT MYERS', 'FL', 'PAGE FIELD AIRPORT', 26.585, -81.8614, 15, 18, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12835');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12836', 'KEY WEST', 'FL', 'KEY WEST INTERNATIONAL AIRPORT', 24.555, -81.75222, 4, 21, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12836');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12838', 'MELBOURNE', 'FL', 'MELBOURNE INTL AP', 28.1011, -80.6439, 27, 27, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12839', 'MIAMI', 'FL', 'MIAMI INTERNATIONAL AIRPORT', 25.7905, -80.3163, 29, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12841', 'ORLANDO', 'FL', 'EXECUTIVE AIRPORT', 28.54528, -81.33306, 108, 112, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12842', 'TAMPA', 'FL', 'TAMPA INTERNATIONAL AIRPORT', 27.96194, -82.5403, 19, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12843', 'VERO BEACH', 'FL', 'VERO BEACH MUNICIPAL AIRPORT', 27.651, -80.4199, 28, 28, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12843');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12844', 'WEST PALM BEACH', 'FL', 'PALM BEACH INTERNATIONAL AIRPORT', 26.6847, -80.0994, 19, 21, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12844');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12849', 'FORT LAUDERDALE', 'FL', 'FT LAUD/HOLLYWOOD INTL APT', 26.0719, -80.1536, 11, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12850', 'KEY WEST', 'FL', 'KEY WEST NAS', 24.58333, -81.68333, 6, 24, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12854', 'ORLANDO', 'FL', 'ORLANDO SANFORD AIRPORT', 28.77972, -81.24361, 55, 55, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12854');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12861', 'OCALA', 'FL', 'OCALA INTERNATIONAL AIRPORT-JIM TAYLOR FIELD', 29.16667, -82.23333, 87, 89, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12861');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12867', 'COCOA BEACH', 'FL', 'PATRICK AFB AIRPORT', 28.23333, -80.6, 8, 9, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12867');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12868', 'COCOA BEACH', 'FL', 'COCOA BEACH CAPE KENNEDY AFS', 28.48333, -80.56667, 10, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12871', 'SARASOTA/BRADENTON', 'FL', 'SARASOTA/BRADENTON INTL AP', 27.40139, -82.55861, 28, 27, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12873', 'ST PETERSBURG/CLEARWATER', 'FL', 'ST PETE-CLWTR INTL AIRPORT', 27.91056, -82.6875, 11, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12873');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12876', 'WINTER HAVEN', 'FL', 'WINTER HAVEN''S GILBERT APT', 28.06222, -81.75417, 146, 146, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12876');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12882', 'MIAMI', 'FL', 'OPA LOCKA AIRPORT', 25.90694, -80.28028, 10, 9, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12883', 'LAKELAND', 'FL', 'LAKELAND LINDER REGIONAL AIRPORT', 28, -82.05, 142, 142, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12884', 'BOOTHVILLE', 'LA', 'BOOTHVILLE', 29.333, -89.4075, 3, 1, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12884');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12885', 'FORT LAUDERDALE', 'FL', 'FT LAUDER EXECUTIVE ARPT', 26.19694, -80.17083, 14, 14, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12885');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12886', 'TITUSVILLE', 'FL', 'KENNEDY SPACE CENTER', 28.61667, -80.68333, 10, 10, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12888', 'MIAMI', 'FL', 'KENDALL-TAMIAMI EXEC ARPT', 25.6475, -80.43306, 10, 10, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12888');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12889', 'ASTOR', 'FL', 'BOMBING RANGE DETACHMENT ASTOR', 29.14306, -81.63278, 92, 35, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12894', 'FORT MYERS', 'FL', 'SW FLORIDA INTN AIRPORT', 26.53611, -81.755, 31, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12895', 'FORT PIERCE', 'FL', 'ST LUCIE COUNTY INTL ARPT', 27.49806, -80.37667, 24, 26, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12896', 'MARATHON', 'FL', 'THE FL KEYS MARATHON ARPT', 24.72583, -81.05167, 8, 7, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12897', 'NAPLES', 'FL', 'NAPLES MUNICIPAL AIRPORT', 26.1522, -81.7752, 9, 23, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12898', 'TITUSVILLE', 'FL', 'SPACE COAST REGIONAL AIRPORT', 28.51667, -80.8, 35, 34, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12904', 'HARLINGEN', 'TX', 'VALLEY INTERNATIONAL ARPT', 26.22806, -97.65417, 34, 35, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12906', 'HOUSTON', 'TX', 'ELLINGTON FIELD AIRPORT', 29.61667, -95.16667, 32, 32, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12906');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12907', 'LAREDO', 'TX', 'LAREDO INTERNATIONAL AIRPORT', 27.53333, -99.46667, 494, 508, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12907');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12909', 'SAN ANTONIO', 'TX', 'LACKLAND AIR FORCE BASE (KELLY FIELD ANNEX)', 29.38333, -98.58333, 690, 691, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12911', 'UNIVERSAL CITY', 'TX', 'RANDOLPH AFB AIRPORT', 29.5325, -98.2623, 728, 762, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12912', 'VICTORIA', 'TX', 'VICTORIA REGIONAL AIRPORT', 28.8614, -96.9303, 115, 117, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12912');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12916', 'NEW ORLEANS', 'LA', 'LOUIS ARMSTRONG NEW ORLEANS INTL AP', 29.9933, -90.2511, 4, 20, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12916');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12917', 'BEAUMONT/PORT ARTHUR', 'TX', 'SOUTHEAST TEXAS REGIONAL AIRPORT', 29.95056, -94.02056, 16, 16, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12917');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12918', 'HOUSTON', 'TX', 'WILLIAM P. HOBBY AIRPORT', 29.63806, -95.28194, 44, 47, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12919', 'BROWNSVILLE', 'TX', 'BROWNSVILLE/S PADRE ISLAND INTL AP', 25.9141, -97.423, 24, 22, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12921', 'SAN ANTONIO', 'TX', 'SAN ANTONIO INTERNATIONAL AIRPORT', 29.5443, -98.4839, 789, 809, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12921');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12923', 'GALVESTON', 'TX', 'SCHOLES INTL AT GLSTON APT', 29.2733, -94.8592, 5, 9, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12923');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12924', 'CORPUS CHRISTI', 'TX', 'CORPUS CHRISTI INTERNATIONAL AIRPORT', 27.7742, -97.5122, 44, 44, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12924');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12925', 'CHASE FIELD BEEVILLE', 'TX', 'CHASE NAVAL AIR STATION', 28.36667, -97.66667, 66, 190, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12926', 'CORPUS CHRISTI', 'TX', 'CORPUS CHRISTI NAS', 27.68333, -97.28333, 18, 19, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12927', 'HOUMA', 'LA', 'HOUMA-TERREBONNE AIRPORT', 29.56639, -90.66028, 9, 10, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12928', 'KINGSVILLE', 'TX', 'KINGSVILLE NAS', 27.5, -97.81667, 50, 58, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12930', 'NEW ORLEANS AUDUBON', 'LA', 'NEW ORLEANS AUDUBON GOLF SITE (AUDUBON PARK)', 29.9166, -90.1302, 20, 25, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12932', 'ALICE', 'TX', 'ALICE INTERNATIONAL ARPT', 27.74111, -98.02472, 173, 180, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12935', 'PALACIOS', 'TX', 'PALACIOS MUNICIPAL AIRPORT', 28.72472, -96.25361, 12, 15, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12936', 'PATTERSON', 'LA', 'HARRY P WILLIAMS MEMO ARPT', 29.71667, -91.33333, 9, 9, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12936');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12945', 'HOUSTON', 'TX', 'DOWNTOWN', 29.76667, -95.36667, 52, 42, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12945');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12946', 'CORPUS CHRISTI', 'TX', 'CABANISS FIELD NAVAL OUTLYING FIELD', 27.7, -97.43333, 30, 30, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12946');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12947', 'COTULLA', 'TX', 'COTULLA-LA SALLE CO ARPT', 28.45667, -99.21833, 476, 479, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12953', 'WHARTON', 'TX', 'WHARTON REGIONAL AIRPORT', 29.26611, -96.0075, 100, 100, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12957', 'PORT ISABEL', 'TX', 'PORT ISABEL-CAMERON CO APT', 26.16583, -97.34583, 19, 19, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12958', 'NEW ORLEANS', 'LA', 'NEW ORLEANS NAS', 29.81667, -90.01667, 2, 1, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12959', 'MC ALLEN', 'TX', 'MC ALLEN MILLER INTL ARPT', 26.18389, -98.25389, 100, 112, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12960', 'HOUSTON', 'TX', 'G BUSH INTERCONTINENTAL AP/HOUSTON AP', 29.98, -95.36, 95, 105, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12961', 'KERRVILLE', 'TX', 'KRVL MUNI/LUIS SHRER FD AP', 29.98333, -99.08333, 1617, 1617, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12962', 'HONDO', 'TX', 'HONDO MUNICIPAL AIRPORT', 29.3601, -99.1742, 920, 930, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12968', 'SALT POINT', 'LA', 'SALT POINT', 29.56222, -91.52556, 4, 2, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12970', 'SAN ANTONIO', 'TX', 'STINSON MINICIPAL AIRPORT', 29.3389, -98.472, 571, 577, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12970');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12971', 'NEW BRAUNFELS', 'TX', 'NEW BRAUNFELS MUNICIPAL AP', 29.7089, -98.0458, 645, 645, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12972', 'ROCKPORT', 'TX', 'ARANSAS COUNTY AIRPORT', 28.08361, -97.04639, 22, 19, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12973', 'WALDRON', 'TX', 'OUTLYING LANDING FIELD', 27.63333, -97.31667, 25, 25, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12974', 'ORANGE GROVE', 'TX', 'NAVAL AUXILIARY LANDING FD', 27.9, -98.05, 257, 257, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12974');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12975', 'HOUSTON', 'TX', 'CLOVER FIELD AIRPORT', 29.51889, -95.24167, 44, 44, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12976', 'ANGLETON/LAKE JACKSON', 'TX', 'BRAZORIA COUNTY AIRPORT', 29.10972, -95.46194, 25, 24, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12977', 'HOUSTON', 'TX', 'SUGAR LAND REGIONAL ARPT', 29.62194, -95.65667, 84, 82, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12978', 'LA GRANGE', 'TX', 'FAYETTE RGNL AIR CNTR ARP', 29.90778, -96.95, 324, 324, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12979', 'SAN MARCOS', 'TX', 'SAN MARCOS MUNICIPAL ARPT', 29.89083, -97.86444, 597, 597, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12979');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12980', 'WESLACO', 'TX', 'MID VALLEY AIRPORT', 26.1775, -97.97306, 70, 70, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12980');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12981', 'BAY CITY', 'TX', 'BAY CITY MUNICIPAL AIRPORT', 28.97306, -95.86333, 45, 45, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12981');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12982', 'FALFURRIAS', 'TX', 'BROOKS COUNTY AIRPORT', 27.20667, -98.12111, 112, 112, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12982');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12983', 'EDINBURG', 'TX', 'EDINBURG INTL AIRPORT', 26.44194, -98.12944, 80, 80, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12983');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12984', 'ROBSTOWN', 'TX', 'NUECES COUNTY ARIPORT', 27.77861, -97.69056, 79, 79, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12984');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12985', 'UVALDE', 'TX', 'GARNER FIELD AIRPORT', 29.21139, -99.74361, 942, 942, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12986', 'PORT LAVACA', 'TX', 'CALHOUN COUNTY AIRPORT', 28.65417, -96.68139, 30, 30, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12986');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12987', 'EDINBURG', 'TX', 'EDINBURG 17 NNE', 26.5258, -98.0633, 64, 64, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12987');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12988', 'LAKE PALOURD', 'LA', 'LAKE PALOURD BASE HELIPORT', 29.69306, -91.09861, 5, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12988');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12989', 'CAMERON', 'LA', 'CAMERON HELIPORT', 29.78333, -93.3, 3, 3, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12990', 'GRAND ISLE', 'LA', 'GRAND ISLE SEAPLANE BASE', 29.19194, -90.07528, 7, 7, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12991', 'SHIP SHOAL BLOCK 207A', 'LA', 'GULF OF MEXICO', 28.51667, -90.96667, 106, 106, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12992', 'SPOFFORD', 'TX', 'SPOFFORD AUXILARY AIRFIELD LAUGHLIN AFB', 29.12917, -100.47222, 965, 965, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12993', 'GALLIANO', 'LA', 'SOUTH LAFOURCHE AIRPORT', 29.44472, -90.26111, 1, 1, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12994', 'HEBBRONVILLE', 'TX', 'JIM HOGG COUNTY AIRPORT', 27.34944, -98.73694, 663, 663, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '12995', 'PORT ARANSAS', 'TX', 'MUSTANG BEACH AIRPORT', 27.81194, -97.08861, 5, 5, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='12995');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13025', 'ROTA', 'NULL', 'ROTA NAVAL AIR STATION', 36.65, -6.35, 84, 86, 1
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13025');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13301', 'CHILLICOTHE', 'MO', 'CHILLICOTHE 22 ENE', 39.8668, -93.147, 833, 833, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13301');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13601', 'BERMUDA', 'NULL', 'NAVAL ATLANTIC METEROLOGY & OCEANOGRAPHY FACILITY', 32.36667, -64.68333, 18, 11, -4
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13601');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13701', 'ABERDEEN PROVING GROUNDS', 'MD', 'PHILLIPS ARMY AIRFIELD', 39.4716, -76.1697, 57, 58, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13701');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13702', 'HAMPTON', 'VA', 'LANGLEY AFB AIRPORT', 37.08278, -76.36028, 10, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13702');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13705', 'CAMP SPRINGS', 'MD', 'ANDREWS AIR FORCE BASE AIRPORT', 38.81667, -76.86667, 282, 280, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13705');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13707', 'DOVER', 'DE', 'DOVER AFB AIRPORT', 39.13333, -75.46667, 28, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13707');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13713', 'GOLDSBORO', 'NC', 'SEYMOUR-JOHNSON AFB AIRPORT', 35.3444, -77.9647, 109, 110, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13713');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13714', 'FAYETTEVILLE', 'NC', 'POPE AFB AIRPORT', 35.17389, -79.00889, 218, 218, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13714');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13717', 'MYRTLE BEACH', 'SC', 'MYRTLE BEACH INTL AIRPORT', 33.68333, -78.93333, 25, 25, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13717');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13721', 'PATUXENT RIVER', 'MD', 'NAVAL AIR STATION', 38.3, -76.41667, 39, 40, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13721');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13722', 'RALEIGH/DURHAM', 'NC', 'RALEIGH-DURHAM INTERNATIONAL AP', 35.8923, -78.7819, 416, 437, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13722');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13723', 'GREENSBORO', 'NC', 'PIEDMONT TRIAD INTERNATIONAL AIRPORT', 36.0969, -79.9432, 890, 886, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13723');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13724', 'ATLANTIC CITY C.O.', 'NJ', 'ATLANTIC CITY MARINA', 39.3792, -74.4243, 10, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13724');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13728', 'DANVILLE', 'VA', 'DANVILLE REGIONAL AIRPORT', 36.5728, -79.3361, 571, 590, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13728');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13729', 'ELKINS', 'WV', 'ELKINS-RAND CO-J.RAND FIELD AIRPORT', 38.8853, -79.8528, 1979, 1979, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13729');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13730', 'FREDERICK', 'MD', 'FREDERICK MUNICIPAL ARPT', 39.41667, -77.38333, 303, 303, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13730');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13733', 'LYNCHBURG', 'VA', 'LYNCHBURG RGNL/PRESTON GLENN FLD AP', 37.3208, -79.2067, 940, 938, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13734', 'MARTINSBURG', 'WV', 'ETRN WV RGNL/SHPHERD FD AP', 39.4019, -77.9844, 534, 534, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13734');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13735', 'MILLVILLE', 'NJ', 'MILLVILLE MUNICIPAL ARPT', 39.3667, -75.0667, 70, 76, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13735');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13736', 'MORGANTOWN', 'WV', 'MGTN RGNL-W L B HART FD AP', 39.64278, -79.91639, 1240, 1253, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13736');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13737', 'NORFOLK', 'VA', 'NORFOLK INTERNATIONAL AIRPORT', 36.9033, -76.1922, 30, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13739', 'PHILADELPHIA', 'PA', 'PHILADELPHIA INTERNATIONAL AIRPORT', 39.8683, -75.2311, 10, 28, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13739');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13740', 'RICHMOND', 'VA', 'RICHMOND INTERNATIONAL AIRPORT', 37.505, -77.3202, 164, 163, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13740');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13741', 'ROANOKE', 'VA', 'ROANOKE REGIONAL/WOODRUM FIELD AP', 37.3169, -79.9741, 1175, 1176, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13741');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13743', 'WASHINGTON', 'VA', 'RONALD REAGAN WASHINGTON NATL AP', 38.8483, -77.0341, 10, 65, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13743');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13744', 'FLORENCE', 'SC', 'FLORENCE REGIONAL AIRPORT', 34.1852, -79.7238, 146, 151, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13744');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13748', 'WILMINGTON', 'NC', 'WILMINGTON INTERNATIONAL AIRPORT', 34.2675, -77.8997, 33, 38, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13748');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13750', 'NORFOLK', 'VA', 'NORFOLK NAS', 36.93746, -76.28926, 17, 16, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13750');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13752', 'ANNAPOLIS', 'MD', 'U.S. NAVAL ACADEMY', 38.98333, -76.46667, 8, 7, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13752');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13754', 'CHERRY POINT', 'NC', 'CHERRY POINT MCAS', 34.9, -76.88333, 29, 29, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13754');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13762', 'FENTRESS', 'VA', 'NAVAL AUXILIARY LANDING FIELD', 36.695, -76.13556, 16, 16, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13762');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13763', 'FRANKLIN', 'VA', 'FRANKLIN MUNICIPAL-JOHN BEVERLY ROSE AIRPORT', 36.69806, -76.90306, 41, 41, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13763');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13764', 'GEORGETOWN', 'DE', 'SUSSEX COUNTY AIRPORT', 38.68917, -75.35917, 51, 51, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13764');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13766', 'MANTEO', 'NC', 'DARE COUNTY REGIONAL AIRPORT', 35.91722, -75.7, 13, 14, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13766');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13769', 'OCEANA', 'VA', 'OCEANA NAS', 36.81667, -76.03333, 23, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13769');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13773', 'QUANTICO', 'VA', 'QUANTICO MCAF', 38.50362, -77.30503, 10, 12, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13773');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13776', 'LUMBERTON', 'NC', 'LUMBERTON MUNICIPAL ARPT', 34.608, -79.0591, 121, 126, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13776');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13777', 'BALTIMORE', 'MD', 'BALTIMORE CUSTOM HOUSE', 39.28333, -76.61667, 14, 19, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13777');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13781', 'WILMINGTON', 'DE', 'NEW CASTLE COUNTY AIRPORT', 39.6728, -75.6008, 79, 80, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13781');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13782', 'CHARLESTON C.O.', 'SC', 'CUSTOM HOUSE (CITY OFC)', 32.78, -79.9319, 10, 12, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13782');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13783', 'GREENVILLE', 'NC', 'PITT-GREENVILLE AIRPORT', 35.63333, -77.38333, 25, 27, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13783');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13786', 'ELIZABETH CITY', 'NC', 'ELZ CTY CG/AIR STN RGNL AP', 36.26056, -76.175, 13, 13, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13786');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13802', 'BELLEVILLE', 'IL', 'SCOTT AIR FORCE BASE/MIDAMERICA AIRPORT', 38.55, -89.85, 459, 459, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13803', 'COLUMBUS', 'IN', 'COLUMBUS MUNICIPAL AIRPORT', 39.26667, -85.9, 656, 656, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13806', 'FORT CAMPBELL/HOPKINSVILLE', 'KY', 'CAMPBELL AAF AIRPORT', 36.66667, -87.48333, 573, 573, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13807', 'FORT KNOX', 'KY', 'GODMAN AAF AIRPORT', 37.9, -85.96667, 755, 780, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13807');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13809', 'LAWRENCEVILLE', 'IL', 'LRNCVLL-VINCNES INTL ARPT', 38.76417, -87.60556, 429, 430, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13810', 'LOUISVILLE', 'KY', 'BOWMAN FIELD AIRPORT', 38.22806, -85.66361, 540, 556, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13812', 'COLUMBUS', 'OH', 'RICKENBACKER INTL AIRPORT', 39.81667, -82.93333, 744, 744, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13812');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13814', 'BLYTHEVILLE', 'AR', 'ARKANSAS INTERNATIONAL AIRPORT', 35.96667, -89.95, 254, 251, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13814');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13820', 'BILOXI', 'MS', 'KEESLER AIR FORCE BASE', 30.41667, -88.91667, 33, 34, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13821', 'MONTGOMERY', 'AL', 'MAXWELL AFB AIRPORT', 32.38333, -86.35, 171, 172, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13824', 'SAVANNAH', 'GA', 'HUNTER ARMY AIRFIELD', 32.01667, -81.13333, 41, 42, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13825', 'COLUMBUS', 'MS', 'COLUMBUS AFB AIRPORT', 33.65, -88.45, 219, 219, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13827', 'SMYRNA', 'TN', 'SMYRNA AIRPORT', 36.00889, -86.52, 543, 543, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13829', 'FORT BENNING (COLUMBUS)', 'GA', 'LAWSON AAF AIRPORT', 32.35, -85, 232, 232, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13829');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13833', 'HATTIESBURG', 'MS', 'BOBBY L CHAIN MUNI AIRPORT', 31.28194, -89.25306, 151, 151, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13837', 'AUGUSTA', 'GA', 'DANIEL FIELD AIRPORT', 33.46694, -82.03861, 423, 423, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13837');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13838', 'MOBILE', 'AL', 'MOBILE DOWNTOWN AIRPORT', 30.62639, -88.06806, 26, 26, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13839', 'DOTHAN', 'AL', 'DOTHAN REGIONAL AIRPORT', 31.3167, -85.45, 374, 353, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13840', 'DAYTON', 'OH', 'WRIGHT-PATTERSON AFB AIRPORT', 39.83333, -84.05, 823, 823, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13840');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13841', 'WILMINGTON', 'OH', 'AIRBORNE AIRPARK AIRPORT', 39.42028, -83.82167, 1077, 1071, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13846', 'PANAMA CITY', 'FL', 'TYNDALL AFB AIRPORT', 30.06667, -85.58333, 17, 17, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13849', 'SUMTER', 'SC', 'SHAW AIR FORCE BASE', 33.96667, -80.46667, 241, 242, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13857', 'VALDOSTA', 'GA', 'MOODY AFB AIRPORT', 30.96667, -83.2, 233, 235, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13857');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13858', 'VALPARAISO', 'FL', 'EGLIN AFB AIRPORT', 30.48333, -86.51667, 87, 87, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13860', 'WARNER ROBINS', 'GA', 'ROBINS AFB AIRPORT', 32.63333, -83.6, 294, 295, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13860');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13861', 'WAYCROSS', 'GA', 'WAYCROSS-WARE CO. AIRPORT', 31.25, -82.4, 142, 142, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13861');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13864', 'MARIETTA', 'GA', 'DOBBINS AIR RESERVE BASE AIRPORT', 33.91667, -84.51667, 1068, 1068, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13864');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13865', 'MERIDIAN', 'MS', 'KEY FIELD AIRPORT', 32.3347, -88.7442, 294, 310, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13865');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13866', 'CHARLESTON', 'WV', 'YEAGER AIRPORT', 38.3794, -81.59, 910, 982, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13866');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13868', 'DUBLIN', 'VA', 'NEW RIVER VALLEY AIRPORT', 37.13333, -80.68333, 2105, 2105, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13869', 'ALBANY', 'GA', 'SW GEORGIA REGIONAL ARPT', 31.53556, -84.19444, 190, 193, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13869');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13870', 'ALMA', 'GA', 'BACON COUNTY AIRPORT', 31.5358, -82.5067, 193, 206, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13871', 'ANNISTON', 'AL', 'ANNISTON METROPOLITAN ARPT', 33.5872, -85.8556, 594, 600, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13873', 'ATHENS', 'GA', 'ATHENS/BEN EPPS AIRPORT', 33.948, -83.3275, 785, 802, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13873');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13874', 'ATLANTA', 'GA', 'HARTSFIELD-JACKSON ATLANTA INTL AP', 33.6301, -84.4418, 1010, 1026, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13874');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13876', 'BIRMINGHAM', 'AL', 'BIRMINGHAM INTERNATIONAL AIRPORT', 33.56556, -86.745, 615, 630, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13876');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13877', 'BRISTOL/JHNSN CTY/KNGSPRT', 'TN', 'TRI-CITIES REGIONAL TN/VA AIRPORT', 36.4731, -82.4044, 1500, 1525, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13877');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13878', 'BRUNSWICK', 'GA', 'MALCOLM MC KINNON AIRPORT', 31.1522, -81.3908, 16, 24, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13878');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13880', 'CHARLESTON', 'SC', 'CHARLESTON AFB/INTERNATIONAL AIRPORT', 32.8986, -80.0402, 40, 48, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13880');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13881', 'CHARLOTTE', 'NC', 'CHARLOTTE/DOUGLAS INTERNATIONAL AP', 35.2236, -80.9552, 728, 769, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13881');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13882', 'CHATTANOOGA', 'TN', 'LOVELL FIELD AIRPORT', 35.0311, -85.2014, 671, 688, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13883', 'COLUMBIA', 'SC', 'COLUMBIA METROPOLITAN AIRPORT', 33.9419, -81.1181, 225, 225, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13884', 'CRESTVIEW', 'FL', 'BOB SIKES AIRPORT', 30.77972, -86.5225, 190, 185, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13884');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13886', 'GREENVILLE', 'SC', 'GREENVILLE DOWNTOWN ARPT', 34.84611, -82.34611, 1048, 1048, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13889', 'JACKSONVILLE', 'FL', 'JACKSONVILLE INTERNATIONAL AIRPORT', 30.495, -81.6936, 26, 32, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13891', 'KNOXVILLE', 'TN', 'MC GHEE TYSON AIRPORT', 35.8181, -83.9858, 962, 980, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13893', 'MEMPHIS', 'TN', 'MEMPHIS INTERNATIONAL AIRPORT', 35.0564, -89.9865, 254, 332, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13894', 'MOBILE', 'AL', 'MOBILE REGIONAL AIRPORT', 30.68833, -88.24556, 215, 221, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13895', 'MONTGOMERY', 'AL', 'MONTGOMERY RGNL (DANNELLY FD) AP', 32.2997, -86.4075, 202, 202, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13896', 'MUSCLE SHOALS', 'AL', 'NORTH WEST ALABAMA REGIONAL AIRPORT', 34.7441, -87.5997, 540, 562, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13897', 'NASHVILLE', 'TN', 'NASHVILLE INTERNATIONAL AIRPORT', 36.11889, -86.68917, 600, 605, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13899', 'PENSACOLA', 'FL', 'PENSACOLA REGIONAL AIRPORT', 30.47806, -87.18694, 112, 118, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13899');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13902', 'ALTUS', 'OK', 'ALTUS AFB AIRPORT', 34.65, -99.26667, 1382, 1382, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13904', 'AUSTIN/BERGSTROM', 'TX', 'AUSTIN-BERGSTROM INTL AIRPORT', 30.1831, -97.6799, 480, 495, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13909', 'ENID', 'OK', 'VANCE AIR FORCE BASE', 36.33333, -97.91667, 1306, 1307, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13910', 'ABILENE', 'TX', 'DYESS AIR FORCE BASE', 32.43333, -99.85, 1788, 1789, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13911', 'FORT WORTH', 'TX', 'FORT WORTH NAVAL AIR STATION JRB/CARSWELL FIELD', 32.76667, -97.45, 608, 650, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13917', 'NEWPORT', 'AR', 'NEWPORT MUNICIPAL AIRPORT', 35.63333, -91.16667, 239, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13917');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13919', 'OKLAHOMA CITY', 'OK', 'TINKER AFB AIRPORT', 35.41667, -97.38333, 1291, 1291, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13920', 'TOPEKA', 'KS', 'FORBES FIELD AIRPORT', 38.95028, -95.66389, 1067, 1079, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13920');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13925', 'STUTTGART', 'AR', 'STUTTGART MUNICIPAL ARPT', 34.60028, -91.57444, 224, 224, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13926', 'GREENVILLE', 'TX', 'MAJORS AIRPORT', 33.06778, -96.06528, 535, 535, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13927', 'JACKSON', 'MS', 'HAWKINS FIELD AIRPORT', 32.33667, -90.22139, 342, 342, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13930', 'KNOB NOSTER', 'MO', 'WHITEMAN AFB AIRPORT', 38.71667, -93.55, 870, 871, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13932', 'WINFIELD/ARKANSAS CITY', 'KS', 'STROTHER FIELD AIRPORT', 37.16806, -97.03694, 1150, 1160, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13935', 'ALEXANDRIA', 'LA', 'ESLER REGIONAL AIRPORT', 31.3953, -92.2907, 118, 118, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13939', 'GREENVILLE', 'MS', 'MID DELTA REGIONAL AIRPORT', 33.4825, -90.98528, 128, 129, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13939');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13940', 'GREAT BEND', 'KS', 'GREAT BEND MUNICIPAL ARPT', 38.35, -98.86667, 1880, 1886, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13941', 'LAKE CHARLES', 'LA', 'CHENNAULT INTERNATIONAL AP', 30.21028, -93.14333, 17, 17, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13941');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13942', 'MONROE', 'LA', 'MONROE REGIONAL AIRPORT', 32.5155, -92.0405, 79, 81, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13944', 'BOSSER CITY', 'LA', 'BARKSDALE AIR FORCE BASE', 32.5, -93.66667, 166, 166, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13944');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13945', 'FORT SILL', 'OK', 'HENRY POST AAF AIRPORT', 34.65, -98.4, 1189, 1188, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13945');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13947', 'FORT RILEY (JUNCTION CITY)', 'KS', 'MARSHALL ARMY AIRFIELD', 39.05, -96.76667, 1065, 1063, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13957', 'SHREVEPORT', 'LA', 'SHREVEPORT REGIONAL AIRPORT', 32.4472, -93.8244, 254, 259, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13958', 'AUSTIN/CITY', 'TX', 'AUSTIN-CAMP MABRY ARMY NATIONAL GUARD', 30.3208, -97.7604, 670, 658, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13959', 'WACO', 'TX', 'WACO REGIONAL AIRPORT', 31.61889, -97.22833, 500, 508, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13960', 'DALLAS', 'TX', 'DALLAS LOVE FIELD AIRPORT', 32.8519, -96.8555, 440, 488, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13961', 'FORT WORTH', 'TX', 'FT WORTH MEACHAM INTL ARPT', 32.81917, -97.36139, 687, 706, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13962', 'ABILENE', 'TX', 'ABILENE REGIONAL AIRPORT', 32.4105, -99.6822, 1790, 1790, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13963', 'LITTLE ROCK', 'AR', 'ADAMS FIELD AIRPORT', 34.7273, -92.2389, 258, 257, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13963');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13964', 'FORT SMITH', 'AR', 'FORT SMITH REGIONAL AIRPORT', 35.333, -94.3625, 449, 462, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13964');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13966', 'WICHITA FALLS', 'TX', 'SHEPPARD AFB/WICHITA FALLS MUNI AP', 33.9786, -98.4928, 1017, 1030, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13967', 'OKLAHOMA CITY', 'OK', 'WILL ROGERS WORLD AIRPORT', 35.3889, -97.6006, 1285, 1304, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13968', 'TULSA', 'OK', 'TULSA INTERNATIONAL AIRPORT', 36.1994, -95.8872, 650, 676, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13969', 'PONCA CITY', 'OK', 'PONCA CITY REGIONAL ARPT', 36.73667, -97.10194, 1000, 998, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13969');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13970', 'BATON ROUGE', 'LA', 'BATON ROUGE METRO RYAN FIELD AIRPORT', 30.5372, -91.1469, 64, 76, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13970');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13971', 'HARRISON', 'AR', 'BOONE COUNTY AIRPORT', 36.2668, -93.1566, 1374, 1385, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13972', 'TYLER', 'TX', 'TYLER POUNDS REGIONAL ARPT', 32.35417, -95.4025, 544, 551, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13973', 'JUNCTION', 'TX', 'KIMBLE COUNTY AIRPORT', 30.51083, -99.76639, 1749, 1749, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13975', 'GAGE', 'OK', 'GAGE AIRPORT', 36.2967, -99.7689, 2191, 2202, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13976', 'LAFAYETTE', 'LA', 'LAFAYETTE REGIONAL AIRPORT', 30.205, -91.9875, 38, 42, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13977', 'TEXARKANA', 'AR', 'TEXARKANA REGIONAL AIRPORT-WEBB FIELD', 33.4536, -94.0074, 361, 399, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13978', 'GREENWOOD', 'MS', 'GREENWOOD-LEFLORE AIRPORT', 33.4963, -90.0866, 133, 133, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13981', 'CHANUTE', 'KS', 'CHANUTE MARTIN JOHNSON APT', 37.67028, -95.48417, 985, 1011, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13981');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13984', 'CONCORDIA', 'KS', 'BLOSSER MUNICIPAL AIRPORT', 39.5514, -97.6508, 1469, 1484, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13984');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13985', 'DODGE CITY', 'KS', 'DODGE CITY REGIONAL AIRPORT', 37.7686, -99.9678, 2582, 2592, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13986', 'HUTCHINSON', 'KS', 'HUTCHINSON MUNICIPAL ARPT', 38.06528, -97.86056, 1543, 1542, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13986');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13987', 'JOPLIN', 'MO', 'JOPLIN REGIONAL AIRPORT', 37.1466, -94.5022, 980, 985, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13987');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13988', 'KANSAS CITY', 'MO', 'C.R. WHEELER DOWNTOWN ARPT', 39.1208, -94.5969, 742, 750, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13988');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13989', 'EMPORIA', 'KS', 'EMPORIA MUNICIPAL AIRPORT', 38.32917, -96.19472, 1196, 1206, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13993', 'ST JOSEPH', 'MO', 'ROSECRANS MEMORIAL AIRPORT', 39.7736, -94.9233, 818, 818, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13994', 'ST LOUIS', 'MO', 'LAMBERT-ST LOUIS INTERNATIONAL AP', 38.7525, -90.3736, 531, 710, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13995', 'SPRINGFIELD', 'MO', 'SPRINGFIELD-BRANSON REGIONAL AIRPORT', 37.2397, -93.3897, 1259, 1270, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13995');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13996', 'TOPEKA', 'KS', 'PHILIP BILLARD MUNICIPAL AIRPORT', 39.0725, -95.6261, 876, 885, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13997', 'ROLLA/VICHY', 'MO', 'ROLLA NATIONAL AIRPORT', 38.13111, -91.76833, 1127, 1137, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '13999', 'CLINTON', 'OK', 'CLINTON REGIONAL AIRPORT', 35.53827, -98.93278, 1616, 1615, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='13999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14604', 'PRESQUE ISLE', 'ME', 'NERN MAINE RGNL ARPT AT PRESQUE IS AIRPORT', 46.68333, -68.05, 534, 534, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14604');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14605', 'AUGUSTA', 'ME', 'AUGUSTA STATE AIRPORT', 44.3155, -69.7972, 351, 360, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14605');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14606', 'BANGOR', 'ME', 'BANGOR INTERNATIONAL ARPT', 44.7978, -68.8185, 148, 192, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14606');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14607', 'CARIBOU', 'ME', 'CARIBOU MUNICIPAL AIRPORT', 46.8705, -68.0173, 624, 624, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14607');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14609', 'HOULTON', 'ME', 'INTERNATIONAL AIRPORT', 46.1236, -67.7928, 476, 476, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14609');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14610', 'MILLINOCKET', 'ME', 'MILLINOCKET MUNICIPAL ARPT', 45.6477, -68.6925, 406, 402, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14610');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14611', 'BRUNSWICK', 'ME', 'NAVAL AIR STATION', 43.9, -69.93333, 70, 75, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14611');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14615', 'WATERVILLE', 'ME', 'WATERVILLE ROBERT LAFLEUR ARPT', 44.53333, -69.66667, 310, 333, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14615');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14616', 'BAR HARBOR', 'ME', 'HANCOCK CO-BAR HARBOR ARPT', 44.45, -68.36667, 88, 85, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14616');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14702', 'BEDFORD', 'MA', 'LAURENCE G HANSCOM FLD APT', 42.47, -71.28944, 133, 133, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14702');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14703', 'SPRINGFIELD/CHICOPEE', 'MA', 'WESTOVER AFB/METROPOLITAN AIRPORT', 42.2, -72.53333, 241, 245, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14703');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14704', 'FALMOUTH', 'MA', 'OTIS ANGB AIRPORT', 41.65, -70.51667, 130, 132, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14706', 'WRIGHTSTOWN', 'NJ', 'MCGUIRE AFB AIRPORT', 40.01667, -74.6, 131, 133, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14706');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14707', 'GROTON NEW LONDON', 'CT', 'GROTON-NEW LONDON AIRPORT', 41.3275, -72.04944, 10, 10, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14707');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14710', 'MANCHESTER', 'NH', 'MANCHESTER AIRPORT', 42.93333, -71.43833, 225, 232, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14710');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14711', 'HARRISBURG', 'PA', 'HARRISBURG INTERNATIONAL AIRPORT', 40.1962, -76.7724, 312, 312, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14711');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14712', 'READING', 'PA', 'RDNG RGNL/C A SPAATZ FD AP', 40.36667, -75.96667, 344, 353, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14712');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14714', 'NEWBURGH', 'NY', 'STEWART INTERNATIONAL AIRPORT', 41.5, -74.1, 491, 491, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14714');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14715', 'FORT DRUM', 'NY', 'WHEELER-SACK AAF AIRPORT', 44.05, -75.73333, 688, 691, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14715');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14719', 'WESTHAMPTON BEACH', 'NY', 'FRANCIS S GABRESKI AP', 40.84361, -72.63222, 67, 67, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14719');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14732', 'NEW YORK', 'NY', 'LA GUARDIA AIRPORT', 40.77944, -73.88028, 11, 31, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14732');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14733', 'BUFFALO', 'NY', 'BUFFALO NIAGARA INTERNATIONAL AP', 42.9408, -78.7358, 716, 705, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14734', 'NEWARK', 'NJ', 'NEWARK LIBERTY INTERNATIONAL AP', 40.6825, -74.16944, 7, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14734');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14735', 'ALBANY', 'NY', 'ALBANY INTERNATIONAL AIRPORT', 42.7431, -73.8092, 312, 292, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14735');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14736', 'ALTOONA', 'PA', 'ALTOONA-BLAIR COUNTY ARPT', 40.29639, -78.32028, 1480, 1469, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14736');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14737', 'ALLENTOWN', 'PA', 'LEHIGH VALLEY INTERNATIONAL AIRPORT', 40.65083, -75.4492, 390, 385, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14739', 'BOSTON', 'MA', 'GEN E L LOGAN INTERNATIONAL AIRPORT', 42.3606, -71.0106, 12, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14739');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14740', 'WINDSOR LOCKS', 'CT', 'BRADLEY INTERNATIONAL AIRPORT', 41.9381, -72.6825, 190, 179, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14740');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14742', 'BURLINGTON', 'VT', 'BURLINGTON INTERNATIONAL AIRPORT', 44.4683, -73.1499, 330, 340, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14742');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14745', 'CONCORD', 'NH', 'CONCORD MUNICIPAL AIRPORT', 43.1952, -71.5011, 346, 346, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14745');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14747', 'DUNKIRK', 'NY', 'CHAUTAUQUA CO/DUNKIRK ARPT', 42.49333, -79.27222, 666, 693, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14747');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14748', 'ELMIRA/CORNING', 'NY', 'ELMIRA/CORNING RGNL ARPT', 42.15944, -76.89194, 955, 954, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14748');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14750', 'GLENS FALLS', 'NY', 'FLOYD BENNETT MEMO AIRPORT', 43.35, -73.6167, 321, 333, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14750');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14751', 'HARRISBURG', 'PA', 'CAPITAL CITY AIRPORT', 40.21722, -76.85139, 340, 347, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14751');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14752', 'HARTFORD', 'CT', 'HARTFORD-BRAINARD AIRPORT', 41.73611, -72.65056, 19, 19, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14752');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14753', 'MILTON', 'MA', 'BLUE HILL OBSERVATORY', 42.2123, -71.1137, 625, 635, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14753');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14755', 'MT. WASHINGTON', 'NH', 'MT. WASHINGTON OBSERVATORY', 44.2669, -71.2997, 6271, 6267, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14755');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14756', 'NANTUCKET', 'MA', 'NANTUCKET MEMORIAL AIRPORT', 41.25306, -70.06083, 48, 47, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14756');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14757', 'POUGHKEEPSIE', 'NY', 'DUTCHESS COUNTY AIRPORT', 41.6266, -73.8842, 166, 162, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14757');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14758', 'NEW HAVEN', 'CT', 'TWEED-NEW HAVEN AIRPORT', 41.26389, -72.88722, 3, 13, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14758');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14761', 'PHILIPSBURG', 'PA', 'MID-STATE AIRPORT', 40.9, -78.08333, 1942, 1948, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14761');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14762', 'PITTSBURGH', 'PA', 'ALLEGHENY COUNTY AIRPORT', 40.35472, -79.92167, 1248, 1273, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14762');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14763', 'PITTSFIELD', 'MA', 'PITTSFIELD MUNICIPAL ARPT', 42.42722, -73.28917, 1194, 1194, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14763');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14764', 'PORTLAND', 'ME', 'PORTLAND INTERNATIONAL JETPORT AP', 43.6497, -70.3002, 45, 63, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14764');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14765', 'PROVIDENCE', 'RI', 'THEODORE F GREEN STATE AIRPORT', 41.7219, -71.4325, 60, 62, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14765');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14768', 'ROCHESTER', 'NY', 'GREATER ROCHESTER INTERNATIONAL AP', 43.1167, -77.6767, 539, 555, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14768');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14770', 'SELINSGROVE', 'PA', 'PENN VALLEY AIRPORT', 40.82056, -76.86417, 444, 450, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14770');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14771', 'SYRACUSE', 'NY', 'SYRACUSE HANCOCK INTERNATIONAL AP', 43.1111, -76.1038, 413, 417, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14771');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14775', 'WESTFIELD/SPRINGFIELD', 'MA', 'BARNES MUNICIPAL AIRPORT', 42.15778, -72.71611, 271, 271, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14775');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14777', 'WILKES-BARRE/SCRANTON', 'PA', 'WILKES-BARRE/SCRANTON INTL AIRPORT', 41.3336, -75.7269, 930, 962, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14777');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14778', 'WILLIAMSPORT', 'PA', 'WILLIAMSPORT REGIONAL AIRPORT', 41.2433, -76.9217, 520, 525, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14778');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14780', 'LAKEHURST', 'NJ', 'NAES/MAXFIELD FIELD', 40.03333, -74.35, 101, 103, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14780');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14787', 'NEWPORT', 'RI', 'NEWPORT STATE AIRPORT', 41.53333, -71.28333, 172, 172, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14787');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14790', 'SOUTH WEYMOUTH', 'MA', 'NAVAL AIR STATION', 42.15, -70.93333, 161, 162, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14790');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14792', 'TRENTON', 'NJ', 'TRENTON MERCER AIRPORT', 40.27694, -74.81583, 184, 213, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14792');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14793', 'WILLOW GROVE', 'PA', 'WILLOW GROVE NAS', 40.2, -75.15, 361, 362, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14793');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14794', 'WESTERLY', 'RI', 'WESTERLY STATE AIRPORT', 41.34972, -71.79889, 81, 81, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14794');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14804', 'MOUNT CLEMENS', 'MI', 'SELFRIDGE AIR NATIONAL GUARD BASE', 42.6083, -82.8183, 580, 580, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14804');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14808', 'OSCODA', 'MI', 'OSCODA-WURTSMITH AIRPORT', 44.45, -83.4, 633, 634, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14813', 'AKRON', 'OH', 'AKRON FULTON INTL AIRPORT', 41.0375, -81.46417, 1044, 1101, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14815', 'BATTLE CREEK', 'MI', 'W K KELLOGG AIRPORT', 42.3075, -85.25111, 928, 939, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14817', 'CADILLAC', 'MI', 'WEXFORD COUNTY AIRPORT', 44.28333, -85.41667, 1305, 1307, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14819', 'CHICAGO', 'IL', 'CHICAGO MIDWAY INTL ARPT', 41.78611, -87.75222, 612, 618, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14820', 'CLEVELAND', 'OH', 'CLEVELAND-HOPKINS INTERNATIONAL AP', 41.405, -81.85278, 770, 805, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14821', 'COLUMBUS', 'OH', 'PORT COLUMBUS INTERNATIONAL AIRPORT', 39.99139, -82.88083, 810, 816, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14822', 'DETROIT', 'MI', 'DETROIT CITY AIRPORT', 42.40917, -83.01, 626, 626, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14825', 'FINDLAY', 'OH', 'FINDLAY AIRPORT', 41.01361, -83.66861, 800, 812, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14826', 'FLINT', 'MI', 'BISHOP INTERNATIONAL AIRPORT', 42.9666, -83.7494, 770, 766, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14827', 'FORT WAYNE', 'IN', 'FORT WAYNE INTERNATIONAL AIRPORT', 40.9705, -85.2063, 791, 828, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14829', 'GOSHEN', 'IN', 'GOSHEN MUNICIPAL AIRPORT', 41.5333, -85.7833, 830, 827, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14829');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14833', 'JACKSON', 'MI', 'JAKSON CO-RYNOLDS FLD ARPT', 42.2667, -84.4667, 998, 1020, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14834', 'JOLIET', 'IL', 'JOLIET REGIONAL AIRPORT', 41.5, -88.16667, 582, 581, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14834');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14835', 'LAFAYETTE', 'IN', 'PURDUE UNIVERSITY AIRPORT', 40.41222, -86.93694, 599, 637, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14835');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14836', 'LANSING', 'MI', 'CAPITAL CITY AIRPORT', 42.78028, -84.57889, 841, 874, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14836');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14837', 'MADISON', 'WI', 'DANE CO REGIONAL-TRUAX FIELD AIRPORT', 43.1405, -89.3452, 866, 866, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14837');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14839', 'MILWAUKEE', 'WI', 'GENERAL MITCHELL INTERNATIONAL AP', 42.955, -87.9044, 670, 693, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14840', 'MUSKEGON', 'MI', 'MUSKEGON COUNTY AIRPORT', 43.17111, -86.23667, 625, 633, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14840');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14841', 'PELLSTON', 'MI', 'PELLSTON REGIONAL ARPT OF EMMET COUNTY ARPT', 45.5644, -84.7927, 705, 715, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14842', 'PEORIA', 'IL', 'GREATER PEORIA REGIONAL AIRPORT', 40.6675, -89.6839, 650, 662, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14845', 'SAGINAW', 'MI', 'MBS INTERNATIONAL AIRPORT', 43.53306, -84.07972, 660, 670, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14845');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14847', 'SAULT STE MARIE', 'MI', 'SAULT STE MARIE MUNI/SASN FIELD AP', 46.4794, -84.3572, 722, 724, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14848', 'SOUTH BEND', 'IN', 'SOUTH BEND REGIONAL AIRPORT', 41.7072, -86.3163, 773, 773, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14848');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14850', 'TRAVERSE CITY', 'MI', 'CHERRY CAPITAL AIRPORT', 44.74083, -85.5825, 618, 630, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14852', 'YOUNGSTOWN/WARREN', 'OH', 'YOUNGSTOWN-WARREN REGIONAL AIRPORT', 41.25444, -80.67389, 1180, 1186, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14853', 'DETROIT', 'MI', 'WILLOW RUN AIRPORT', 42.23333, -83.53333, 777, 716, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14855', 'GLENVIEW', 'IL', 'NAVAL AIR STATION', 42.08333, -87.83333, 646, 653, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14858', 'HANCOCK', 'MI', 'HOUGHTON COUNTY MEMO ARPT', 47.16861, -88.48889, 1095, 1079, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14860', 'ERIE', 'PA', 'ERIE INTL/T. RIDGE FIELD AIRPORT', 42.08, -80.1825, 730, 737, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14860');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14864', 'BIG RAPIDS', 'MI', 'ROBEN-HOOD ARPT', 43.71667, -85.5, 928, 990, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14864');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14880', 'CHICAGO/WAUKEGAN', 'IL', 'WAUKEGAN REGIONAL AIRPORT', 42.41667, -87.86667, 727, 727, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14880');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14886', 'Land O'' Lakes', 'WI', 'Kings Land O'' Lakes Airport', 46.15, -89.21667, 1704, 1074, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14891', 'MANSFIELD', 'OH', 'MANSFIELD LAHM REGIONAL AIRPORT', 40.82028, -82.51778, 1295, 1312, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14894', 'WHEELING', 'WV', 'WHEELING OHIO COUNTY ARPT', 40.17639, -80.64722, 1179, 1195, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14895', 'AKRON', 'OH', 'AKRON-CANTON REGIONAL AIRPORT', 40.91667, -81.43333, 1208, 1236, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14897', 'WAUSAU', 'WI', 'WAUSAU DOWNTOWN AIRPORT', 44.9288, -89.6277, 1200, 1196, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14898', 'GREEN BAY', 'WI', 'AUSTIN STRAUBEL INTERNATIONAL AP', 44.4794, -88.1366, 687, 702, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14905', 'KEARNEY', 'NE', 'KEARNEY MUNICIPAL AIRPORT', 40.73333, -99, 2130, 2129, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14905');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14910', 'ALEXANDRIA', 'MN', 'CHANDLER FIELD AIRPORT', 45.8679, -95.3941, 1416, 1431, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14913', 'DULUTH', 'MN', 'DULUTH INTERNATIONAL AIRPORT', 46.8369, -92.1833, 1433, 1417, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14913');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14914', 'FARGO', 'ND', 'HECTOR INTERNATIONAL AIRPORT', 46.92528, -96.81111, 900, 899, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14914');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14916', 'GRAND FORKS', 'ND', 'GRAND FORKS INTERNATIONAL AIRPORT', 47.9428, -97.1839, 842, 832, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14916');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14918', 'INTERNATIONAL FALLS', 'MN', 'FALLS INTERNATIONAL AIRPORT', 48.5614, -93.3981, 1183, 1183, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14919', 'JAMESTOWN', 'ND', 'JAMESTOWN REGIONAL AIRPORT', 46.9258, -98.6691, 1494, 1494, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14920', 'LA CROSSE', 'WI', 'LA CROSSE MUNICIPAL AIRPORT', 43.8788, -91.2527, 652, 656, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14920');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14921', 'LONE ROCK', 'WI', 'TRI-COUNTY REGIONAL ARPT', 43.21194, -90.18139, 717, 721, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14921');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14922', 'MINNEAPOLIS', 'MN', 'MINNEAPOLIS-ST PAUL INTERNATIONAL AP', 44.8831, -93.2289, 872, 838, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14922');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14923', 'MOLINE', 'IL', 'QUAD CITY INTERNATIONAL AIRPORT', 41.46528, -90.52333, 592, 594, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14923');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14925', 'ROCHESTER', 'MN', 'ROCHESTER INTERNATIONAL AIRPORT', 43.9041, -92.4916, 1304, 1320, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14926', 'ST CLOUD', 'MN', 'ST CLOUD REGIONAL AIRPORT', 45.5433, -94.0513, 1009, 1024, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14927', 'ST PAUL', 'MN', 'ST PAUL DWTWN HOLMAN FD AP', 44.93194, -93.05556, 700, 711, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14928', 'WILLMAR', 'MN', 'WLMR MUNI-J L RICE FD ARPT', 45.11667, -95.08333, 1129, 1129, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14929', 'ABERDEEN', 'SD', 'ABERDEEN REGIONAL AIRPORT', 45.4433, -98.413, 1297, 1300, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14930', 'ATLANTIC', 'IA', 'ATLANTIC MUNICIPAL AIRPORT', 41.40722, -95.04667, 1182, 1182, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14931', 'BURLINGTON', 'IA', 'SE IOWA REGIONAL AIRPORT', 40.78333, -91.12528, 692, 702, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14933', 'DES MOINES', 'IA', 'DES MOINES INTERNATIONAL AIRPORT', 41.5338, -93.653, 957, 963, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14935', 'GRAND ISLAND', 'NE', 'CENTRAL NEBRASKA REGIONAL AIRPORT', 40.9611, -98.3136, 1840, 1856, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14936', 'HURON', 'SD', 'HURON REGIONAL AIRPORT', 44.3981, -98.2231, 1280, 1289, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14936');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14937', 'IOWA CITY', 'IA', 'IOWA CITY MUNICIPAL ARPT', 41.63278, -91.54306, 650, 668, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14938', 'KIRKSVILLE', 'MO', 'KIRKSVILLE REGIONAL ARPT', 40.09722, -92.54333, 966, 965, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14938');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14939', 'LINCOLN', 'NE', 'LINCOLN MUNICIPAL AIRPORT', 40.8508, -96.7475, 1190, 1189, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14939');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14940', 'MASON CITY', 'IA', 'MASON CITY MUNICIPAL ARPT', 43.1544, -93.3269, 1225, 1225, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14941', 'NORFOLK', 'NE', 'KARL STEFAN MEMORIAL AIRPORT', 41.9855, -97.4352, 1551, 1551, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14941');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14942', 'OMAHA', 'NE', 'EPPLEY AIRFIELD AIRPORT', 41.3102, -95.8991, 982, 982, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14943', 'SIOUX CITY', 'IA', 'SIOUX GATEWAY/COL BUD DAY FIELD AP', 42.3913, -96.3791, 1095, 1103, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14944', 'SIOUX FALLS', 'SD', 'JOE FOSS FIELD AIRPORT', 43.5778, -96.7539, 1428, 1427, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14944');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14946', 'WATERTOWN', 'SD', 'WATERTOWN MUNICIPAL ARPT', 44.9047, -97.1494, 1748, 1740, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14946');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14949', 'OMAHA', 'NE', 'OFFUTT AFB AIRPORT', 41.11667, -95.91667, 1047, 1048, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14949');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14950', 'OTTUMWA', 'IA', 'OTTUMWA INDUSTRIAL AIRPORT', 41.1077, -92.4466, 842, 845, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14954', 'MANKATO', 'MN', 'MANKATO MUNICIPAL AIRPORT', 44.21667, -93.91667, 1021, 1020, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14954');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14958', 'BEMIDJI', 'MN', 'BEMIDJI-BELTRAMI CO ARPT', 47.5, -94.93333, 1392, 1390, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14966', 'CHARLES CITY', 'IA', 'CHARLES CITY MUNICIPAL APT', 43.0604, -92.6717, 993, 1125, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14972', 'SPENCER', 'IA', 'SPENCER MUNICIPAL AIRPORT', 43.16444, -95.20167, 1338, 1338, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14976', 'GRINNELL', 'IA', 'GRINNELL REGIONAL AIRPORT', 41.71667, -92.7, 1008, 1008, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14989', 'YORK', 'NE', 'YORK MUNICIPAL AIRPORT', 40.89417, -97.62583, 1664, 1670, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14990', 'CEDAR RAPIDS', 'IA', 'THE EASTERN IOWA AIRPORT', 41.8833, -91.7166, 868, 871, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14991', 'EAU CLAIRE', 'WI', 'CHIPPEWA VALLEY RGNL ARPT', 44.8665, -91.4879, 885, 895, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14992', 'REDWOOD FALLS', 'MN', 'REDWOOD FALLS MUNI AIRPORT', 44.5483, -95.0804, 1021, 1030, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '14994', 'LEXINGTON', 'NE', 'JIM KELLY FIELD AIRPORT', 40.78861, -99.77083, 2405, 2413, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='14994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '16201', 'KEFLAVIK', 'NULL', 'KEFLAVIK NAVAL AIR STATION', 63.96667, -22.6, 171, 171, 0
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='16201');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21504', 'HILO', 'HI', 'HILO INTERNATIONAL AIRPORT', 19.7191, -155.053, 38, 36, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21504');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21508', 'CAMP POHAKULOA', 'HI', 'BRADSHAW ARMY AIRFIELD', 19.76, -155.55361, 5900, 5900, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21508');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21510', 'KAILUA/KONA', 'HI', 'KONA INTL AT KEAHOLE ARPT', 19.73556, -156.04889, 43, 43, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21510');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21514', 'MAUNA LOA', 'HI', 'MAUNA LOA 5 NNE', 19.5354, -155.576, 11179, 11179, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21514');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21515', 'HILO', 'HI', 'HILO 5 S', 19.645, -155.0827, 622, 622, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21515');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '21603', 'JOHNSTON ISLAND, JOHNSTON ATOLL', 'UM', 'JOHNSTON ATOLL AIRPORT', 16.73333, -169.51667, 10, 17, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='21603');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22001', 'DEL RIO', 'TX', 'LAUGHLIN AFB AIRPORT', 29.36667, -100.78333, 1082, 1081, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22001');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22010', 'DEL RIO', 'TX', 'DEL RIO INTERNATIONAL AIRPORT', 29.3784, -100.927, 999, 1027, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22010');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22016', 'PANTHER JUNCTION', 'TX', 'PANTHER JUNCTION 2 N', 29.3483, -103.2093, 3494, 3494, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22016');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22501', 'KEKAHA, KAUAI', 'HI', 'BARKING SANDS PACIFIC MISSILE RANGE FAC ARPT', 22.03333, -159.78333, 23, 12, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22501');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22508', 'WAHIAWA', 'HI', 'WHEELER ARMY AIRFIELD', 21.48722, -158.02806, 846, 837, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22508');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22514', 'OAHU', 'HI', 'BARBERS POINT', 21.31667, -158.06667, 50, 33, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22514');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22516', 'KAHULUI', 'HI', 'KAHULUI AIRPORT', 20.89972, -156.42861, 51, 50, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22516');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22519', 'KANEOHE', 'HI', 'KANEOHE MCAS', 21.45045, -157.76794, 24, 18, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22519');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22521', 'HONOLULU', 'HI', 'HONOLULU INTERNATIONAL AIRPORT', 21.32389, -157.92944, 7, 15, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22521');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22524', 'LANAI CITY', 'HI', 'LANAI AIRPORT', 20.7895, -156.9485, 1300, 1308, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22524');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22534', 'KAUNAKAKAI', 'HI', 'MOLOKAI AIRPORT', 21.1545, -157.0961, 443, 444, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22534');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22536', 'LIHUE', 'HI', 'LIHUE AIRPORT', 21.98389, -159.34056, 100, 148, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22536');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22551', 'KAPOLEI', 'HI', 'KALAELOA AIRPORT (JOHN RODGERS FIELD)', 21.31667, -158.06667, 33, 33, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22551');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22552', 'LAHAINA', 'HI', 'KAPTALUA AIRPORT', 20.9625, -156.6753, 240, 256, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22552');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '22701', 'SAND ISLAND, MIDWAY ISLAND', 'UM', 'HENDERSON FIELD AIRPORT', 28.2, -177.38333, 18, 13, -11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='22701');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23002', 'ALAMOGORDO', 'NM', 'HOLLOMAN AFB AIRPORT', 32.85, -106.1, 4158, 4093, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23002');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23007', 'CHILDRESS', 'TX', 'CHILDRESS MUNICIPAL ARPT', 34.4272, -100.2831, 1951, 1952, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23007');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23008', 'CLOVIS', 'NM', 'CANNON AFB AIRPORT', 34.38333, -103.31667, 4295, 4295, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23008');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23009', 'ROSWELL', 'NM', 'ROSWELL INTERNATIONAL AIR CENTER AP', 33.3075, -104.5083, 3649, 3669, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23009');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23020', 'LIBERAL', 'KS', 'LIBERAL MUNICIPAL AIRPORT', 37.05, -100.96667, 2873, 2883, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23020');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23023', 'MIDLAND', 'TX', 'MIDLAND INTERNATIONAL AIRPORT', 31.9475, -102.2086, 2862, 2862, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23023');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23033', 'SWEETWATER', 'TX', 'AVENGER FIELD AIRPORT', 32.47333, -100.46639, 2380, 2385, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23033');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23034', 'SAN ANGELO', 'TX', 'SAN ANGELO REGIONAL/MATHS FIELD AP', 31.35167, -100.495, 1916, 1892, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23034');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23036', 'AURORA', 'CO', 'BUCKLEY AIR FORCE BASE', 39.71667, -104.75, 5663, 5662, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23036');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23040', 'WINK', 'TX', 'WINKLER COUNTY AIRPORT', 31.7801, -103.2018, 2807, 2820, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23040');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23042', 'LUBBOCK', 'TX', 'LUBBOCK INTERNATIONAL AIRPORT', 33.6656, -101.8231, 3254, 3241, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23042');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23044', 'EL PASO', 'TX', 'EL PASO INTERNATIONAL AIRPORT', 31.81111, -106.37583, 3918, 3916, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23044');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23047', 'AMARILLO', 'TX', 'AMARILLO RICK HUSBAND INTL AIRPORT', 35.2295, -101.7042, 3604, 3604, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23047');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23048', 'TUCUMCARI', 'NM', 'TUCUMCARI MUNICIPAL ARPT', 35.18222, -103.60306, 4065, 4039, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23048');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23049', 'SANTA FE', 'NM', 'SANTA FE MUNICIPAL AIRPORT', 35.61694, -106.08889, 6344, 6344, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23049');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23050', 'ALBUQUERQUE', 'NM', 'ALBUQUERQUE INTL SUNPORT AIRPORT', 35.0419, -106.6155, 5310, 5314, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23050');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23051', 'CLAYTON', 'NM', 'CLAYTON MUNICIPAL AIRPARK AIRPORT', 36.4486, -103.1539, 4960, 4972, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23051');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23052', 'RATON', 'NM', 'RATON MUNI/CREWS FIELD APT', 36.74139, -104.50222, 6349, 6349, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23052');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23054', 'LAS VEGAS', 'NM', 'LAS VEGAS MUNICIPAL ARPT', 35.65417, -105.14194, 6874, 6875, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23054');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23055', 'GUADALUPE PASS', 'TX', 'GUADALUPE PASS AMOS', 31.83111, -104.80889, 5456, 5456, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23055');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23061', 'ALAMOSA', 'CO', 'SAN LUIS VALLEY REGIONAL AIRPORT', 37.4389, -105.8613, 7533, 7541, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23061');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23063', 'EAGLE', 'CO', 'EAGLE CO REGIONAL AIRPORT', 39.65, -106.91667, 6497, 6535, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23063');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23064', 'GARDEN CITY', 'KS', 'GARDEN CITY REGIONAL ARPT', 37.92722, -100.72472, 2882, 2882, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23064');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23065', 'GOODLAND', 'KS', 'RENNER FIELD/GOODLAND MINI AIRPORT', 39.36722, -101.69333, 3656, 3688, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23065');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23066', 'GRAND JUNCTION', 'CO', 'GRAND JUNCTION REGIONAL AIRPORT', 39.1342, -108.54, 4858, 4839, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23066');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23067', 'LA JUNTA', 'CO', 'LA JUNTA MUNICIPAL AIRPORT', 38.0494, -103.5122, 4194, 4215, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23067');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23070', 'TRINIDAD', 'CO', 'PERRY STOKES AIRPORT', 37.26222, -104.33778, 5741, 5743, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23070');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23077', 'CLOVIS', 'NM', 'CLOVIS MUNICIPAL AIRPORT', 34.43333, -103.08333, 4216, 4213, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23077');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23078', 'DEMING', 'NM', 'DEMING MUNICIPAL AIRPORT', 32.26222, -107.72056, 4301, 4324, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23078');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23081', 'GALLUP', 'NM', 'GALLUP MUNICIPAL AIRPORT', 35.5144, -108.794, 6471, 6468, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23081');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23090', 'FARMINGTON', 'NM', 'FOUR CORNERS REGIONAL ARPT', 36.74361, -108.22917, 5495, 5502, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23090');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23091', 'FORT STOCKTON', 'TX', 'FORT STOCKTON-PECOS CO APT', 30.91194, -102.91667, 3010, 3010, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23091');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23094', 'HOBBS', 'NM', 'LEA COUNTY REGIONAL AIRPORT', 32.688, -103.217, 0, 3661, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23094');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23097', 'PLAINVIEW', 'TX', 'HALE COUNTY AIRPORT', 34.16667, -101.71667, 3374, 3374, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23097');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23098', 'ROCKSPRINGS', 'TX', 'EDWARDS COUNTY AIRPORT', 29.94667, -100.17333, 2372, 2372, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23098');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23104', 'PHOENIX', 'AZ', 'WILLIAMS GATEWAY AIRPORT', 33.3, -111.66667, 1382, 1382, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23104');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23109', 'TUCSON', 'AZ', 'DAVIS-MONTHAN AFB AIRPORT', 32.16667, -110.88333, 2704, 2704, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23109');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23110', 'LEMOORE', 'CA', 'NAVAL AIR STATION', 36.33333, -119.95, 232, 234, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23110');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23111', 'GLENDALE', 'AZ', 'LUKE AFB AIRPORT', 33.55, -112.36667, 1085, 1085, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23111');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23112', 'LAS VEGAS', 'NV', 'NELLIS AFB AIRPORT', 36.25, -115.03333, 1870, 1868, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23112');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23114', 'EDWARDS', 'CA', 'EDWARDS AIR FORCE BASE', 34.9, -117.86667, 2311, 2302, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23114');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23119', 'RIVERSIDE', 'CA', 'MARCH AIR RESERVE BASE', 33.9, -117.25, 1536, 1535, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23119');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23122', 'SAN BERNARDINO', 'CA', 'SAN BERNARDINO INTERNATIONAL AIRPORT', 34.09528, -117.23472, 1159, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23122');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23129', 'LONG BEACH', 'CA', 'LONG BEACH / DAUGHERTY FIELD / AIRPORT', 33.8116, -118.1463, 31, 40, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23130', 'VAN NUYS', 'CA', 'VAN NUYS AIRPORT', 34.20972, -118.48917, 770, 799, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23130');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23131', 'VICTORVILLE', 'CA', 'SOUTHERN CALIFORNIA LOGISTICS AIRPORT', 34.58333, -117.38333, 2885, 2885, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23136', 'CAMARILLO', 'CA', 'CAMARILLO AIRPORT', 34.21667, -119.08333, 77, 65, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23141', 'NELLIS AIR FORCE BASE', 'NV', 'INDIAN SPRING AUX AIRFIELD', 36.58333, -115.68333, 3133, 3123, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23149', 'PORTERVILLE', 'CA', 'PORTERVILLE MUNICIPAL ARPT', 36.02944, -119.0625, 442, 442, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23149');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23152', 'BURBANK', 'CA', 'BURBANK-GLENDALE-PASA ARPT', 34.20056, -118.3575, 775, 732, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23152');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23153', 'TONOPAH', 'NV', 'TONOPAH AIRPORT', 38.0511, -117.0902, 5395, 5434, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23153');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23154', 'ELY', 'NV', 'ELY AIRPORT/YELLAND FIELD/AIRPORT', 39.2952, -114.8466, 6262, 6262, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23154');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23155', 'BAKERSFIELD', 'CA', 'MEADOWS FIELD AIRPORT', 35.4344, -119.0542, 489, 492, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23155');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23157', 'BISHOP', 'CA', 'BISHOP AIRPORT', 37.3711, -118.358, 4102, 4145, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23157');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23158', 'BLYTHE', 'CA', 'BLYTHE AIRPORT', 33.6186, -114.7142, 395, 392, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23158');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23159', 'BRYCE CANYON', 'UT', 'BRYCE CANYON AIRPORT', 37.70639, -112.14556, 7586, 7589, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23159');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23160', 'TUCSON', 'AZ', 'TUCSON INTERNATIONAL AIRPORT', 32.1313, -110.9552, 2549, 2555, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23160');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23161', 'DAGGETT', 'CA', 'BARSTOW-DAGGETT AIRPORT', 34.8536, -116.7858, 1917, 1929, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23161');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23162', 'DELTA', 'UT', 'DELTA MUNICIPAL AIRPORT', 39.38333, -112.51667, 4759, 4755, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23162');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23169', 'LAS VEGAS', 'NV', 'MCCARRAN INTERNATIONAL AIRPORT', 36.0719, -115.1634, 2180, 2180, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23169');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23170', 'HANKSVILLE', 'UT', 'HANKSVILLE', 38.3713, -110.7103, 4305, 4444, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23170');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23174', 'LOS ANGELES', 'CA', 'LOS ANGELES INTERNATIONAL AIRPORT', 33.938, -118.3888, 97, 326, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23174');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23176', 'MILFORD', 'UT', 'MILFORD MUNICIPAL AIRPORT', 38.41667, -113.01667, 5040, 5033, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23176');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23179', 'NEEDLES', 'CA', 'NEEDLES AIRPORT', 34.7675, -114.6188, 890, 914, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23179');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23182', 'PALMDALE', 'CA', 'PDLE PRODN FLT/TST AF PLT', 34.62944, -118.08361, 2523.5, 2582, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23182');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23183', 'PHOENIX', 'AZ', 'PHOENIX SKY HARBOR INTL AIRPORT', 33.4277, -112.0038, 1107, 1107, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23183');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23184', 'PRESCOTT', 'AZ', 'ERNEST A. LOVE FIELD ARPT', 34.65167, -112.42083, 5042, 5052, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23184');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23185', 'RENO', 'NV', 'RENO/TAHOE INTERNATIONAL AIRPORT', 39.4838, -119.7711, 4410, 4400, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23185');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23186', 'ST GEORGE', 'UT', 'ST GEORGE MUNICIPAL ARPT', 37.1, -113.6, 2936, 2941, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23186');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23187', 'SANDBERG', 'CA', 'SANDBERG', 34.7436, -118.7242, 4510, 4523, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23187');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23188', 'SAN DIEGO', 'CA', 'SAN DIEGO INTERNATIONAL AIRPORT', 32.7336, -117.1831, 15, 28, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23188');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23190', 'SANTA BARBARA', 'CA', 'SANTA BARBARA MUNICIPAL AIRPORT', 34.4258, -119.8425, 9, 20, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23190');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23191', 'AVALON', 'CA', 'CATALINA AIRPORT', 33.405, -118.41583, 1602, 1602, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23191');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23194', 'WINSLOW', 'AZ', 'WINSLOW-LINDBERGH REGIONAL AIRPORT', 35.0281, -110.7208, 4886, 4883, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23194');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23195', 'YUMA', 'AZ', 'YUMA MARINE CORPS AIR STATION/YUMA INTL AIRPORT', 32.66667, -114.6, 206, 216, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23195');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23199', 'EL CENTRO', 'CA', 'NAF', 32.81667, -115.68333, -42, -43, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23199');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23202', 'FAIRFIELD', 'CA', 'TRAVIS AIR FORCE BASE', 38.26667, -121.93333, 62, 62, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23202');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23203', 'ATWATER', 'CA', 'CASTLE AFB', 37.38333, -120.56667, 191, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23203');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23206', 'SACRAMENTO', 'CA', 'SACRAMENTO MATHER AIRPORT', 38.56667, -121.3, 99, 96, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23206');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23208', 'SACRAMENTO', 'CA', 'SACRAMENTO MCCLELLAN AFB', 38.66667, -121.4, 77, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23208');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23213', 'SANTA ROSA', 'CA', 'SONOMA COUNTY AIRPORT', 38.5038, -122.8102, 114, 148, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23213');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23224', 'AUBURN', 'CA', 'AUBURN MUNICIPAL AIRPORT', 38.95472, -121.08194, 1531, 1531, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23224');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23225', 'EMIGRANT GAP', 'CA', 'BLUE CANYON - NYACK ARPT', 39.2774, -120.7102, 5276, 5283, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23225');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23230', 'OAKLAND', 'CA', 'METRO OAKLAND INTL AIRPORT', 37.72139, -122.22083, 6, 88, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23230');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23232', 'SACRAMENTO', 'CA', 'SACRAMENTO EXECUTIVE AIRPORT', 38.5069, -121.495, 15, 25, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23232');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23233', 'SALINAS', 'CA', 'SALINAS MUNICIPAL AIRPORT', 36.6636, -121.6081, 74, 77, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23233');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23234', 'SAN FRANCISCO', 'CA', 'SAN FRANCISCO INTERNATIONAL AIRPORT', 37.6197, -122.3647, 8, 18, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23234');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23237', 'STOCKTON', 'CA', 'STOCKTON METROPOLITAN AIRPORT', 37.8891, -121.2258, 26, 27, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23237');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23239', 'ALAMEDA', 'CA', 'NAVAL AIR STATION', 37.73333, -122.31667, 14, 13, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23239');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23240', 'CROWS LANDING', 'CA', 'NAVAL AUXILIARY LANDING FIELD', 37.4, -121.13333, 165, 186, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23240');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23243', 'LOMPOC', 'CA', 'LOMPOC AIRPORT', 34.66667, -120.46667, 88, 87, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23243');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23244', 'MOUNTAIN VIEW', 'CA', 'MOFFETT FEDERAL AIRFLD APT', 37.40583, -122.04806, 39, 34, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23244');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23254', 'CONCORD', 'CA', 'BUCHANAN FIELD AIRPORT', 37.9917, -122.055, 18, 60, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23254');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23257', 'MERCED', 'CA', 'MRCD MUNI/MACREADY FLD APT', 37.28472, -120.51278, 152, 156, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23257');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23258', 'MODESTO', 'CA', 'MDSTO CTY-CO H SHAM FD APT', 37.6241, -120.9505, 73, 97, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23258');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23259', 'MONTEREY', 'CA', 'MONTEREY PENINSULA AIRPORT', 36.58806, -121.84528, 165, 220, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23259');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23271', 'SACRAMENTO', 'CA', 'DOWNTOWN', 38.5552, -121.4183, 38, 25, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23271');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23272', 'SAN FRANCISCO C.O.', 'CA', 'DOWNTOWN', 37.7705, -122.4269, 150, 75, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23272');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23273', 'SANTA MARIA', 'CA', 'SANTA MARIA PUB/CAPT G A HANCOCK AP', 34.8994, -120.4486, 242, 238, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23273');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23275', 'UKIAH', 'CA', 'UKIAH MUNICIPAL AIRPORT', 39.12583, -123.20083, 601, 626, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23275');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23277', 'WATSONVILLE', 'CA', 'WATSONVILLE MUNICIPAL ARPT', 36.93583, -121.78861, 160, 160, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23277');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23285', 'LIVERMORE', 'CA', 'LIVERMORE MUNICIPAL ARPT', 37.6927, -121.8144, 393, 397, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23285');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23289', 'PALO ALTO', 'CA', 'PALO ALTO AIRPORT OF SANTA CLARA COUNTY', 37.46667, -122.11667, 7, 4, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23289');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23293', 'SAN JOSE', 'CA', 'N Y. MINETA SN JO INTL APT', 37.3591, -121.924, 51, 50, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23293');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23801', 'TROY', 'AL', 'TROY 2 W', 31.7901, -86.0004, 472, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23801');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23802', 'THOMASVILLE', 'AL', 'THOMASVILLE 2 S', 31.8814, -87.7367, 350, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23803', 'HOLLY SPRINGS', 'MS', 'HOLLY SPRINGS 4 N', 34.8223, -89.4348, 484, 484, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23901', 'CALDWELL', 'TX', 'CALDWELL MUNICIPAL AIRPORT', 30.51556, -96.70417, 391, 391, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23902', 'SEMINOLE', 'OK', 'SEMINOLE MUNICIPAL AIRPORT', 35.27472, -96.675, 1024, 1024, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23903', 'TUNICA', 'MS', 'TUNICA MUNICIPAL AIRPORT', 34.68111, -90.34667, 194, 194, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23903');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23904', 'BATESVILLE', 'AR', 'BATESVILLE 8 WNW', 35.8201, -91.7812, 455, 455, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23906', 'PORT ARANSAS', 'TX', 'PORT ARANSAS 32 NNE', 28.3045, -96.823, 15, 15, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23906');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23907', 'AUSTIN', 'TX', 'AUSTIN 33 NW', 30.6222, -98.0846, 1361, 1361, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23907');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23908', 'JOPLIN', 'MO', 'JOPLIN 24 N', 37.4277, -94.5829, 952, 952, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '23909', 'SALEM', 'MO', 'SALEM 10 W', 37.6344, -91.7226, 1198, 1198, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='23909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24006', 'RAPID CITY', 'SD', 'ELLSWORTH AIR FORCE BASE', 44.15, -103.1, 3278, 3279, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24006');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24011', 'BISMARCK', 'ND', 'BISMARCK MUNICIPAL AIRPORT', 46.7825, -100.7572, 1651, 1660, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24011');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24012', 'DICKINSON', 'ND', 'DICKINSON THEODORE ROOSEVELT RGNL ARPT', 46.7994, -102.7972, 2580, 2583, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24012');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24013', 'MINOT', 'ND', 'MINOT INTERNATIONAL ARPT', 48.2552, -101.2733, 1665, 1714, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24013');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24015', 'AKRON', 'CO', 'COLORADO PLAINS RGNL ARPT', 40.16667, -103.21667, 4663, 4621, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24015');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24017', 'CHADRON', 'NE', 'CHADRON MUNICIPAL AIRPORT', 42.8374, -103.0981, 3294, 3296, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24017');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24018', 'CHEYENNE', 'WY', 'CHEYENNE AIRPORT', 41.15, -104.8167, 6130, 6140, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24018');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24021', 'LANDER', 'WY', 'HUNT FIELD AIRPORT', 42.8154, -108.7261, 5592, 5558, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24021');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24022', 'LARAMIE', 'WY', 'LARAMIE REGIONAL AIRPORT', 41.3167, -105.6833, 7266, 7272, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24022');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24023', 'NORTH PLATTE', 'NE', 'NORTH PLATTE RGNL AP, LEE BIRD FD AP', 41.1213, -100.6694, 2778, 2787, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24023');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24024', 'PHILIP', 'SD', 'PHILIP AIRPORT', 44.05111, -101.60111, 2206, 2212, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24024');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24025', 'PIERRE', 'SD', 'PIERRE REGIONAL AIRPORT', 44.3813, -100.2855, 1742, 1726, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24025');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24027', 'ROCK SPRINGS', 'WY', 'RCK SRINGS-SWETWTER CO APT', 41.5944, -109.0529, 6741, 6745, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24027');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24028', 'SCOTTSBLUFF', 'NE', 'WESTERN NE REGIONAL/HEILIG FIELD AP', 41.8705, -103.593, 3945, 3958, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24028');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24029', 'SHERIDAN', 'WY', 'SHERIDAN COUNTY AIRPORT', 44.7694, -106.9688, 3945, 3968, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24029');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24030', 'SIDNEY', 'NE', 'SIDNEY MUNICIPAL AIRPORT', 41.0993, -102.9863, 4296, 4306, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24030');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24032', 'VALENTINE', 'NE', 'MILLER FIELD AIRPORT', 42.8783, -100.55, 2590, 2598, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24032');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24033', 'BILLINGS', 'MT', 'BILLINGS LOGAN INTERNATIONAL AIRPORT', 45.8069, -108.5422, 3581, 3570, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24033');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24036', 'LEWISTOWN', 'MT', 'LEWISTOWN MUNICIPAL ARPT', 47.0492, -109.4578, 4145, 4146, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24036');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24037', 'MILES CITY', 'MT', 'FRANK WILEY FIELD AIRPORT', 46.4266, -105.8825, 2624, 2634, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24037');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24044', 'ALLIANCE', 'NE', 'ALLIANCE MUNICIPAL AIRPORT', 42.0573, -102.8017, 3929, 3929, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24044');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24045', 'CODY', 'WY', 'YELLOWSTONE REGIONAL ARPT', 44.51667, -109.01667, 5092, 5095, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24045');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24046', 'CRAIG', 'CO', 'CRAIG-MOFFAT AIRPORT', 40.49278, -107.52417, 6190, 6193, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24046');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24048', 'GREYBULL', 'WY', 'SOUTH BIG HORN COUNTY ARPT', 44.51694, -108.08222, 3933, 3933, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24048');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24051', 'GREELEY', 'CO', 'GREELEY-WELD COUNTY ARPT', 40.43556, -104.63194, 4697, 4649, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24051');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24057', 'RAWLINS', 'WY', 'RAWLINS MUNI/HARVEY FIELD ARPT', 41.8, -107.2, 6736, 6743, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24057');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24061', 'RIVERTON', 'WY', 'RIVERTON REGIONAL AIRPORT', 43.06417, -108.45889, 5445, 5525, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24061');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24062', 'WORLAND', 'WY', 'WORLAND MUNICIPAL AIRPORT', 43.96583, -107.95083, 4172, 4210, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24062');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24087', 'GLENDIVE', 'MT', 'DAWSON COMMUNITY AIRPORT', 47.13333, -104.8, 2457, 2457, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24087');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24089', 'CASPER', 'WY', 'NATRONA COUNTY INTERNATIONAL AP', 42.8977, -106.4739, 5313, 5290, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24089');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24090', 'RAPID CITY', 'SD', 'RAPID CITY REGIONAL AIRPORT', 44.0433, -103.0536, 3160, 3168, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24090');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24091', 'IMPERIAL', 'NE', 'IMPERIAL MUNICIPAL AIRPORT', 40.51, -101.62, 3268, 3273, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24091');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24101', 'OGDEN', 'UT', 'HILL AFB AIRPORT', 41.11667, -111.96667, 4789, 4789, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24101');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24103', 'DUGWAY PROVING GROUND', 'UT', 'MICHAEL AAF AIRPORT', 40.18333, -112.93333, 4349, 4349, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24103');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24106', 'MOUNTAIN HOME', 'ID', 'MOUNTAIN HOME AFB AIRPORT', 43.05, -115.86667, 2996, 2996, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24106');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24110', 'MOSES LAKE', 'WA', 'GRANT COUNTY INTL AIRPORT', 47.20778, -119.31917, 1196, 1196, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24110');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24112', 'GREAT FALLS', 'MT', 'MALMSTROM AFHP HELIPORT', 47.51667, -111.18333, 3472, 3472, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24112');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24114', 'SPOKANE', 'WA', 'FAIRCHILD AIR FORCE BASE', 47.63333, -117.65, 2461, 2461, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24114');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24119', 'BATTLE MOUNTAIN', 'NV', 'BATTLE MOUNTAIN AIRPORT', 40.6118, -116.8917, 4505, 4532, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24119');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24121', 'ELKO', 'NV', 'ELKO REGIONAL AIRPORT', 40.8288, -115.7886, 5030, 5074, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24121');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24126', 'OGDEN', 'UT', 'OGDEN-HINCKLEY AIRPORT', 41.19611, -112.01139, 4470, 4439, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24126');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24127', 'SALT LAKE CITY', 'UT', 'SALT LAKE CITY INTERNATIONAL AIRPORT', 40.7781, -111.9694, 4225, 4227, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24127');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24128', 'WINNEMUCCA', 'NV', 'WINNEMUCCA MUNICIPAL AIRPORT', 40.9017, -117.808, 4296, 4314, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24128');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24130', 'BAKER CITY', 'OR', 'BAKER CITY MUNICIPAL ARPT', 44.8428, -117.8086, 3361, 3373, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24130');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24131', 'BOISE', 'ID', 'BOISE AIR TERMINAL/GOWEN FD AIRPORT', 43.5666, -116.2405, 2814, 2868, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24132', 'BOZEMAN', 'MT', 'GALLATIN FIELD AIRPORT', 45.788, -111.1608, 4427, 4449, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24132');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24133', 'BURLEY', 'ID', 'BURLEY MUNICIPAL AIRPORT', 42.5416, -113.7661, 4154, 4157, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24133');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24135', 'BUTTE', 'MT', 'BERT MOONEY AIRPORT', 45.9647, -112.5006, 5506, 5533, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24135');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24136', 'COEUR D''ALENE', 'ID', 'COEUR D''ALENE AIR TERM APT', 47.76667, -116.81667, 2307, 2320, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24137', 'CUT BANK', 'MT', 'CUT BANK MUNICIPAL AIRPORT', 48.6033, -112.3752, 3838, 3838, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24137');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24138', 'DILLON', 'MT', 'DILLON AIRPORT', 45.2575, -112.5544, 5200, 5222, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24138');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24139', 'DRUMMOND', 'MT', 'DRUMMOND AVIATION', 46.6383, -113.1761, 4000, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24139');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24141', 'EPHRATA', 'WA', 'EPHRATA MUNICIPAL AIRPORT', 47.3078, -119.5154, 1252, 1259, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24143', 'GREAT FALLS', 'MT', 'GREAT FALLS INTERNATIONAL AIRPORT', 47.4733, -111.3822, 3664, 3657, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24143');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24144', 'HELENA', 'MT', 'HELENA REGIONAL AIRPORT', 46.6056, -111.9636, 3828, 3868, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24144');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24145', 'IDAHO FALLS', 'ID', 'IDAHO FALLS REGIONAL ARPT', 43.51639, -112.06722, 4729, 4744, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24145');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24146', 'KALISPELL', 'MT', 'GLACIER PARK INTERNATIONAL AIRPORT', 48.3042, -114.2636, 2957, 2973, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24146');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24148', 'LA GRANDE', 'OR', 'LA GRANDE/UNION COUNTY APT', 45.28333, -118, 2713, 2717, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24148');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24149', 'LEWISTON', 'ID', 'LEWISTON-NEZ PERCE COUNTY AIRPORT', 46.3747, -117.0156, 1436, 1436, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24149');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24150', 'LIVINGSTON', 'MT', 'MISSION FIELD AIRPORT', 45.6983, -110.4408, 4643, 4618, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24150');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24151', 'MALAD CITY', 'ID', 'MALAD CITY AIRPORT', 42.1492, -112.2873, 4470, 4503, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24151');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24152', 'MEACHAM', 'OR', 'MEACHAM', 45.51139, -118.42472, 3723, 3726, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24152');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24153', 'MISSOULA', 'MT', 'MISSOULA INTERNATIONAL AIRPORT', 46.9208, -114.0925, 3192, 3189, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24153');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24154', 'MULLAN PASS', 'ID', 'MULLAN PASS', 47.45694, -115.645, 6028, 6074, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24154');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24155', 'PENDLETON', 'OR', 'EASTERN OR REGIONAL AT PENDLETON AP', 45.6983, -118.8547, 1486, 1514, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24155');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24156', 'POCATELLO', 'ID', 'POCATELLO REGIONAL AIRPORT', 42.9202, -112.5711, 4452, 4478, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24156');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24157', 'SPOKANE', 'WA', 'SPOKANE INTERNATIONAL AIRPORT', 47.6216, -117.528, 2353, 2365, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24157');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24160', 'WALLA WALLA', 'WA', 'WALLA WALLA REGIONAL ARPT', 46.09472, -118.28694, 1166, 1205, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24160');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24162', 'ONTARIO', 'OR', 'ONTARIO MUNICIPAL AIRPORT', 44.02056, -117.01278, 2193, 2193, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24162');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24163', 'PASCO', 'WA', 'TRI-CITIES AIRPORT', 46.26667, -119.11667, 407, 402, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24163');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24164', 'BIG PINEY', 'WY', 'BIG PINEY-MARBLETON ARPT', 42.58444, -110.1075, 6970, 6974, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24164');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24165', 'KEMMERER', 'WY', 'KEMMERER MUNICIPAL AIRPORT', 41.82389, -110.55694, 7285, 7285, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24165');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24166', 'JACKSON', 'WY', 'JACKSON HOLE AIRPORT', 43.6, -110.73333, 6419, 6447, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24166');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24172', 'LOVELOCK', 'NV', 'DERBY FIELD AIRPORT', 40.0681, -118.5692, 3902, 3904, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24172');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24174', 'PROVO', 'UT', 'PROVO MUNICIPAL AIRPORT', 40.21889, -111.72333, 4497, 4497, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24174');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24180', 'BRIGHAM CITY', 'UT', 'BRIGHAM CITY AIRPORT', 41.55222, -112.06222, 4226, 4226, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24180');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24193', 'WENDOVER', 'UT', 'WENDOVER AIRPORT', 40.7206, -114.0358, 4237, 4235, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24193');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24196', 'SALMON', 'ID', 'LEMHI COUNTY AIRPORT', 45.11667, -113.88333, 4044, 4043, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24196');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24198', 'WEST YELLOWSTONE', 'MT', 'WEATHER SERVICE OFFICE', 44.65, -111.1, 6663, 6663, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24198');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24201', 'FORT LEWIS/TACOMA', 'WA', 'GRAY AFF AIRPORT', 47.08333, -122.58333, 300, 302, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24201');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24202', 'CORVALLIS', 'OR', 'CORVALLIS MUNICIPAL APRT', 44.5, -123.28333, 250, 246, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24202');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24207', 'TACOMA', 'WA', 'MCCHORD AFB AIRPORT', 47.15, -122.48333, 322, 323, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24207');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24213', 'EUREKA', 'CA', 'WOODLEY ISLAND (NEW 10/94)', 40.8097, -124.1602, 20, 60, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24213');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24215', 'MOUNT SHASTA', 'CA', 'MOUNT SHASTA', 41.3325, -122.33278, 3535, 3535, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24215');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24216', 'RED BLUFF', 'CA', 'RED BLUFF MUNICIPAL ARPT', 40.1519, -122.2536, 353, 353, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24216');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24217', 'BELLINGHAM', 'WA', 'BELLINGHAM INTL AIRPORT', 48.79389, -122.53722, 149, 151, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24217');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24219', 'THE DALLES', 'WA', 'MUNICIPAL AIRPORT', 45.6194, -121.1661, 235, 210, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24219');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24220', 'ELLENSBURG', 'WA', 'BOWERS FIELD AIRPORT', 47.03389, -120.53028, 1766, 1765, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24220');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24221', 'EUGENE', 'OR', 'MAHLON SWEET FIELD AIRPORT', 44.1278, -123.2206, 353, 373, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24221');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24222', 'EVERETT', 'WA', 'SNOHOMISH CO (PAINE FD) AP', 47.90778, -122.28028, 606, 606, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24222');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24223', 'KELSO', 'WA', 'KELSO-LONGVIEW AIRPORT', 46.11667, -122.89389, 20, 20, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24223');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24225', 'MEDFORD', 'OR', 'ROGUE VALLEY INTL-MEDFORD AIRPORT', 42.3811, -122.8722, 1297, 1329, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24225');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24227', 'OLYMPIA', 'WA', 'OLYMPIA AIRPORT', 46.9733, -122.9033, 188, 200, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24227');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24229', 'PORTLAND', 'OR', 'PORTLAND INTERNATIONAL AIRPORT', 45.5958, -122.6093, 19, 107, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24229');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24230', 'REDMOND', 'OR', 'ROBERTS FIELD AIRPORT', 44.2558, -121.1392, 3043, 3084, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24230');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24231', 'ROSEBURG', 'OR', 'ROSEBURG REGIONAL AIRPORT', 43.23889, -123.35472, 525, 509, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24231');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24232', 'SALEM', 'OR', 'MCNARY FIELD AIRPORT', 44.905, -123.0011, 205, 201, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24232');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24233', 'SEATTLE', 'WA', 'SEATTLE-TACOMA INTERNATIONAL AIRPORT', 47.4444, -122.3138, 370, 434, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24233');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24234', 'SEATTLE', 'WA', 'BOEING FLD/KING CO INTL AP', 47.53028, -122.30083, 18, 30, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24234');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24235', 'SEXTON SUMMIT', 'OR', 'SEXTON SUMMIT', 42.6003, -123.3641, 3832, 3841, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24235');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24237', 'STAMPEDE PASS', 'WA', 'STAMPASS PASS FLTWO', 47.2767, -121.3372, 3959, 3967, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24237');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24241', 'TOLEDO', 'WA', 'TOLEDO-WINLOCK ED CARLSON MEMORIAL FIELD AIRPORT', 46.48333, -122.81667, 379, 375, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24241');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24242', 'PORTLAND', 'OR', 'PORTLAND-TROUTDALE AIRPORT', 45.55111, -122.40889, 29, 29, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24242');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24243', 'YAKIMA', 'WA', 'YAKIMA AIR TERMINAL/MCALSR FIELD AP', 46.5683, -120.5428, 1064, 1066, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24243');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24255', 'WHIDBEY ISLAND', 'WA', 'WHIDBEY ISLAND NAS', 48.35, -122.66667, 47, 47, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24255');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24257', 'REDDING', 'CA', 'REDDING MUNICIPAL AIRPORT', 40.5175, -122.2986, 497, 502, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24257');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24259', 'MONTAGUE', 'CA', 'SISKIYOU COUNTY AIRPORT', 41.78139, -122.46806, 2651, 2641, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24259');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24267', 'BROOKINGS', 'OR', 'BROOKINGS AIRPORT', 42.07444, -124.29, 459, 459, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24267');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24281', 'SEATTLE C.O.', 'WA', 'WSMO (EMSU)', 47.65, -122.3, 19, 2761, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24281');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24283', 'ARCATA/EUREKA', 'CA', 'ARCATA AIRPORT', 40.97806, -124.10861, 200, 210, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24283');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24284', 'NORTH BEND', 'OR', 'NORTH BEND MUNICIPAL ARPT', 43.41333, -124.24361, 17, 17, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24284');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24285', 'NEWPORT', 'OR', 'NEWPORT MUNICIPAL AIRPORT', 44.58333, -124.05, 122, 160, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24285');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '24286', 'CRESCENT CITY', 'CA', 'JACK MCNAMARA FIELD ARPT', 41.78028, -124.23667, 57, 55, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='24286');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25308', 'ANNETTE', 'AK', 'ANNETTE ISLAND AIRPORT', 55.0389, -131.5787, 109, 110, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25308');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25309', 'JUNEAU', 'AK', 'JUNEAU INTERNATIONAL AIRPORT', 58.3566, -134.564, 16, 24, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25309');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25310', 'ANGOON', 'AK', 'ANGOON SEAPLANE BASE', 57.49889, -134.58639, 28, 0, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25310');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25322', 'GUSTAVUS', 'AK', 'GUSTAVUS', 58.4111, -135.7089, 40, 30, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25322');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25323', 'HAINES', 'AK', 'HAINES', 59.2433, -135.5094, 15, 16, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25323');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25325', 'KETCHIKAN', 'AK', 'KETCHIKAN INTL AIRPORT', 55.35667, -131.71167, 76, 96, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25325');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25329', 'PETERSBURG', 'AK', 'JAMES A JOHNSON AIRPORT', 56.8055, -132.9372, 107, 107, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25329');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25331', 'PALMER', 'AK', 'PALMER MUNICIPAL AIRPORT', 61.5961, -149.0916, 230, 248, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25331');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25333', 'SITKA', 'AK', 'SITKA ROCKY GUTIERREZ ARPT', 57.0481, -135.3647, 14, 67, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25333');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25335', 'SKAGWAY', 'AK', 'SKAGWAY AIRPORT', 59.4556, -135.3239, 20, 44, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25335');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25338', 'WRANGELL', 'AK', 'WRANGELL AIRPORT', 56.4732, -132.3874, 56, 44, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25338');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25339', 'YAKUTAT', 'AK', 'YAKUTAT AIRPORT', 59.512, -139.6712, 33, 31, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25339');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25348', 'PORT ALEXANDER', 'AK', 'PORT ALEXANDER SPB', 56.2472, -134.6447, 12, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25348');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25357', 'ELFIN COVE', 'AK', 'ELFIN COVE SEAPLANE BASE', 58.1916, -136.3435, 20, 0, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25357');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25367', 'KLAWOCK', 'AK', 'KLAWOCK AIRPORT', 55.58, -133.075, 12, 50, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25367');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25369', 'KAKE', 'AK', 'KAKE AIRPORT', 56.96667, -133.9, 172, 172, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25369');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25376', 'HYDABURG', 'AK', 'HYDABURG SEAPLANE BASE', 55.20631, -132.82831, 0, 0, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25376');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25377', 'METLAKATLA', 'AK', 'METLAKATLA SEAPLANE BASE', 55.13104, -131.57806, 0, 0, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25377');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25378', 'HOONAH', 'AK', 'HOONAH SEAPLANE BASE', 58.09619, -135.40875, 0, 0, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25378');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25379', 'SITKA', 'AK', 'SITKA 1 NE', 57.0574, -135.3266, 78, 78, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25379');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25380', 'GUSTAVUS', 'AK', 'GUSTAVUS 2 NE', 58.4292, -135.69, 20, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25380');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25381', 'METLAKATLA', 'AK', 'METLAKATLA 6 S', 55.0455, -131.5877, 105, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25381');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25402', 'MIDDLETON ISLAND', 'AK', 'MIDDLETON ISLAND METEOROLOGY RADAR SITE', 59.43333, -146.33333, 100, 46, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25402');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25501', 'KODIAK', 'AK', 'KODIAK AIRPORT', 57.75111, -152.48556, 80, 111, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25501');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25503', 'KING SALMON', 'AK', 'KING SALMON AIRPORT', 58.6829, -156.6563, 67, 49, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25503');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25506', 'ILIAMNA', 'AK', 'ILIAMNA AIRPORT', 59.7494, -154.9089, 143, 160, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25506');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25507', 'HOMER', 'AK', 'HOMER AIRPORT', 59.642, -151.4908, 64, 73, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25507');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25508', 'PORT HEIDEN', 'AK', 'PORT HEIDEN AIRPORT', 56.95917, -158.63167, 88, 95, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25508');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25513', 'DILLINGHAM', 'AK', 'DILLINGHAM AIRPORT', 59.05, -158.5167, 86, 95, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25513');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25516', 'SELDOVIA', 'AK', 'SELDOVIA AIRPORT', 59.44333, -151.70167, 29, 29, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25516');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25518', 'CHIGNIK', 'AK', 'CHIGNIK AIRPORT', 56.31139, -158.37306, 18, 16, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25518');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25519', 'EGEGIK', 'AK', 'EGEGIK AIRPORT', 58.18528, -157.38556, 92, 92, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25519');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25521', 'IGIUGIG', 'AK', 'IGIUGIG AIRPORT', 59.32417, -155.90167, 90, 90, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25521');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25522', 'KING SALMON', 'AK', 'KING SALMON 42 SE', 58.2077, -155.9225, 661, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25522');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25604', 'PLATINUM', 'AK', 'PLATINUM AIRPORT', 59.01139, -161.81972, 15, 15, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25604');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25616', 'DUTCH HARBOR', 'AK', 'DUTCH HARBOR AIRPORT', 53.895, -166.5433, 10, 13, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25616');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25617', 'SAND POINT', 'AK', 'SAND POINT AIRPORT', 55.31944, -160.52083, 21, 21, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25617');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25623', 'CAPE HEWENHAM', 'AK', 'CAPE HEWENHAM LRRS AIRPORT', 58.65, -162.06667, 541, 541, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25623');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25624', 'COLD BAY', 'AK', 'COLD BAY AIRPORT', 55.22083, -162.7325, 78, 103, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25624');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25628', 'ST. GEORGE ISLAND', 'AK', 'ST. GEORGE AIRPORT', 56.6, -169.565, 125, 125, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25628');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25629', 'TOGIAK VILLAGE', 'AK', 'TOGIAK AIRPORT', 59.06667, -160.36667, 20, 20, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25629');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25630', 'SAND POINT', 'AK', 'SAND POINT 1 ENE', 55.3473, -160.466, 240, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25630');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25704', 'ADAK', 'AK', 'ADAK NAS', 51.88333, -176.65, 17, 19, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25711', 'ST. PAUL', 'AK', 'ST. PAUL 4 NE', 57.1575, -170.2119, 20, 20, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25711');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25713', 'ST PAUL ISLAND', 'AK', 'ST PAUL ISLAND AIRPORT', 57.15528, -170.22222, 35, 28, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25713');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '25715', 'ATKA', 'AK', 'ATKA AIRPORT', 52.22028, -174.20611, 56, 56, -10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='25715');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26401', 'ANCHORAGE', 'AK', 'ELMENDORF AFB AIRPORT', 61.25306, -149.79361, 212, 213, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26407', 'FAIRBANKS', 'AK', 'EIELSON AFB AIRPORT', 64.68333, -147.08333, 547, 548, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26407');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26409', 'ANCHORAGE', 'AK', 'MERRILL FIELD AIRPORT', 61.21694, -149.855, 138, 137, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26409');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26410', 'CORDOVA', 'AK', 'M.K.(MUDHOLE) SMITH ARPT', 60.4888, -145.4511, 31, 48, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26410');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26411', 'FAIRBANKS', 'AK', 'FAIRBANKS INTERNATIONAL AIRPORT', 64.8039, -147.8761, 432, 454, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26411');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26412', 'NORTHWAY', 'AK', 'NORTHWAY AIRPORT', 62.9617, -141.9378, 1713, 1721, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26412');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26413', 'FORT YUKON', 'AK', 'FORT YUKON AIRPORT', 66.56667, -145.26667, 445, 454, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26413');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26414', 'SUMMIT', 'AK', 'SUMMIT AIRPORT', 63.33139, -149.12722, 2409, 2409, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26414');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26415', 'DELTA JUNCTION/FT GREELY', 'AK', 'ALLEN ARMY AIRFIELD', 63.9944, -145.7214, 1277, 1274, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26415');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26418', 'CENTRAL', 'AK', 'CENTRAL AIRPORT', 65.5667, -144.7653, 920, 932, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26418');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26422', 'EAGLE', 'AK', 'EAGLE AIRPORT', 64.77667, -141.14833, 908, 898, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26422');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26425', 'GULKANA', 'AK', 'GULKANA AIRPORT', 62.1591, -145.4588, 1562, 1579, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26425');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26435', 'NENANA', 'AK', 'NENANA MUNICIPAL AIRPORT', 64.55, -149.07167, 360, 362, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26435');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26438', 'SEWARD', 'AK', 'SEWARD AIRPORT', 60.12833, -149.41667, 22, 22, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26438');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26439', 'SHEEP MOUNTAIN', 'AK', 'SHEEP MOUNTAIN AIRPORT', 61.81194, -147.50694, 2800, 2740, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26439');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26442', 'VALDEZ', 'AK', 'NATIONAL WEATHER SERVICE OFFICE', 61.1303, -146.3517, 95, 37, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26442');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26443', 'WASILLA', 'AK', 'WASILLA AIRPORT', 61.57194, -149.54056, 354, 354, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26443');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26444', 'WHITTIER', 'AK', 'WHITTIER AIRPORT', 60.78333, -148.73333, 39, 30, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26444');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26445', 'WASILLA', 'AK', 'WASILLA AIRPORT', 61.572, -149.54, 0, 354, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26445');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26451', 'ANCHORAGE', 'AK', 'TED STEVENS ANCHORAGE INTL AIRPORT', 61.169, -150.0278, 120, 132, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26451');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26465', 'GALBRAITH LAKE', 'AK', 'GALBRAITH LAKE AIRPORT', 68.47889, -149.49, 2666, 2665, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26465');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26479', 'VALDEZ', 'AK', 'VALDEZ PIONEER FIELD AIRPORT', 61.1315, -146.2435, 60, 118, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26479');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26480', 'CHANDALAR LAKE', 'AK', 'CHANDALAR LAKE AIRPORT', 67.5111, -148.4925, 1895, 1920, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26480');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26484', 'PAXSON', 'AK', 'PAXSON AIRPORT', 63.03222, -145.49833, 2700, 2653, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26484');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26486', 'SLANA', 'AK', 'DUFFY''S TAVERN AIRPORT', 62.7089, -143.9808, 2192, 2420, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26486');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26491', 'ANCHORAGE', 'AK', 'LAKE HOOD SEAPLANE BASE', 61.17806, -149.96639, 90, 132, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26491');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26492', 'PORTAGE GLACIER', 'AK', 'PORTAGE GLACIER', 60.785, -148.83889, 103, 103, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26492');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26494', 'FAIRBANKS', 'AK', 'FAIRBANKS 11 NE', 64.9735, -147.51, 1140, 1149, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26494');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26497', 'BIRCHWOOD', 'AK', 'BIRCHWOOD AIRPORT', 61.41639, -149.50722, 96, 96, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26497');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26498', 'FAIRBANKS (FT WAINWRIGHT)', 'AK', 'WAINWRIGHT AAF AIRPORT', 64.8375, -147.61444, 454, 454, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26498');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26499', 'HEALY', 'AK', 'HEALY RIVER AIRPORT', 63.86611, -148.96889, 12944, 1294, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26499');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26501', 'GALENA', 'AK', 'GALENA AIRPORT', 64.73667, -156.93444, 153, 151, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26501');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26502', 'KALTAG', 'AK', 'KALTAG AIRPORT', 64.32667, -158.74167, 181, 200, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26502');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26510', 'MC GRATH', 'AK', 'MC GRATH AIRPORT', 62.9574, -155.6103, 333, 338, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26510');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26512', 'MINCHUMINA', 'AK', 'MINCHUMINA', 63.88611, -152.30194, 678, 702, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26512');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26514', 'SKWENTNA', 'AK', 'SKWENTNA AIRPORT', 61.9772, -151.2169, 150, 148, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26514');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26516', 'ANIAK', 'AK', 'ANIAK AIRPORT', 61.58159, -159.54304, 85, 85, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26516');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26521', 'HOLY CROSS', 'AK', 'HOLY CROSS AIRPORT', 62.18333, -159.76667, 20, 70, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26521');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26523', 'KENAI', 'AK', 'KENAI MUNICIPAL AIRPORT', 60.5797, -151.2391, 91, 92, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26523');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26524', 'MANLEY HOT SPRINGS', 'AK', 'MANLEY HOT SPRINGS', 64.9988, -150.638, 272, 269, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26524');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26526', 'PUNTILLA LAKE', 'AK', 'PUNTILLA LAKE', 62.0911, -152.7349, 1858, 1837, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26526');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26528', 'TALKEETNA', 'AK', 'TALKEETNA AIRPORT', 62.32, -150.095, 350, 356, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26528');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26529', 'TANANA', 'AK', 'RALPH M CALHOUN MEML ARPT', 65.175, -152.1067, 222, 220, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26529');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26533', 'BETTLES', 'AK', 'BETTLES AIRPORT', 66.54, -151.31, 643, 643, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26533');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26534', 'SPARREVOHN', 'AK', 'SPARREVOHN LRRS AIRPORT', 61.1, -155.58333, 1588, 1588, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26534');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26535', 'UTOPIA CREEK', 'AK', 'INDIAN MOUNTAIN LRRS ARPT', 66, -153.7, 1276, 1276, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26535');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26536', 'TAKOTNA', 'AK', 'TATALINA LRRS AIRPORT', 62.89417, -155.97639, 964, 965, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26536');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26542', 'ANAKTUVUK PASS', 'AK', 'ANAKTUVUK PASS AIRPORT', 68.16667, -151.76667, 2100, 2103, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26542');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26546', 'PORT ALSWORTH', 'AK', 'PORT ALSWORTH AIRPORT', 60.2036, -154.3163, 260, 280, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26546');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26547', 'HAYES RIVER', 'AK', 'HAYES RIVER', 61.98333, -152.08333, 1001, 1001, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26547');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26551', 'AMBLER', 'AK', 'AMBLER AIRPORT', 67.1, -157.85, 289, 289, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26551');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26552', 'HUSLIA', 'AK', 'HUSLIA AIRPORT', 65.69778, -156.35139, 180, 213, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26552');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26553', 'SLEETMUTE', 'AK', 'SLEETMUTE AIRPORT', 61.71667, -157.15, 178, 178, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26553');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26554', 'MERRILL PASS WEST', 'AK', 'MERRILL PASS WEST', 61.26667, -153.8, 1170, 1170, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26554');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26555', 'FAREWELL LAKE', 'AK', 'FAREWELL LAKE SEAPLANE BASE', 62.5425, -153.6225, 10524, 1052, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26555');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26557', 'BIG RIVER LAKE', 'AK', 'BIG RIVER LAKE', 60.81417, -152.29694, 60, 39, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26557');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26558', 'SELAWIK', 'AK', 'SELAWIK', 66.6, -159.98611, 25, 25, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26558');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26559', 'SOLDONTA', 'AK', 'SOLDONTA AIRPORT', 60.47583, -151.03417, 113, 113, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26559');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26560', 'WILLOW', 'AK', 'WILLOW AIRPORT', 61.748, -150.0541, 205, 221, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26560');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26561', 'RUBY', 'AK', 'RUBY AIRPORT', 64.72722, -155.46972, 653, 653, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26561');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26562', 'PORT ALSWORTH', 'AK', 'PORT ALSWORTH 1 SW', 60.1951, -154.3196, 321, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26562');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26563', 'KENAI', 'AK', 'KENAI 29 ENE', 60.7237, -150.4484, 282, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26563');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26601', 'POINT HOPE', 'AK', 'POINT HOPE AIRPORT', 68.35, -166.8, 13, 13, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26601');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26602', 'KOYUK', 'AK', 'KOYUK AIRPORT', 64.935, -161.155, 110, 142, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26602');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26615', 'BETHEL', 'AK', 'BETHEL AIRPORT', 60.785, -161.8293, 102, 150, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26615');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26616', 'KOTZEBUE', 'AK', 'RALPH WIEN MEMORIAL AIRPORT', 66.86667, -162.63333, 30, 16, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26616');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26617', 'NOME', 'AK', 'NOME AIRPORT', 64.5111, -165.44, 13, 22, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26617');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26622', 'MEKORYUK', 'AK', 'MEKORYUK AIRPORT', 60.38333, -166.2, 45, 48, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26622');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26623', 'POINT HOPE', 'AK', 'POINT HOPE AIRPORT', 68.35, -166.8, 13, 13, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26623');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26624', 'POINT LAY', 'AK', 'POINT LAY LRRS AIRPORT', 69.73288, -163.00533, 25, 25, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26624');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26625', 'SHISHMAREF', 'AK', 'SHISHMAREF/NEW AIRPORT', 66.24958, -166.08936, 7, 7, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26625');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26627', 'UNALAKLEET', 'AK', 'UNALAKLEET AIRPORT', 63.88333, -160.8, 18, 20, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26627');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26628', 'GOLOVIN', 'AK', 'CAPE DARBY REMOT COM OUTLT', 64.55044, -163.00716, 25, 25, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26628');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26631', 'CAPE LISBURNE', 'AK', 'CAPE LISBURNE LRRS AIRPORT', 68.86667, -166.13333, 16, 10, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26631');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26634', 'TIN CITY', 'AK', 'TIN CITY LRRS AIRPORT', 65.56306, -167.92139, 271, 269, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26634');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26635', 'MOUNTAIN VILLAGE', 'AK', 'MOUNTAIN VILLAGE AIRPORT', 62.09528, -163.68194, 337, 337, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26635');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26642', 'KIVALINA', 'AK', 'KIVALINA AIRPORT', 67.73167, -164.54833, 10, 10, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26642');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26643', 'DEERING', 'AK', 'DEERING AIRPORT', 66.06889, -162.76389, 21, 15, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26643');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26644', 'ANVIK', 'AK', 'ANVIK AIRPORT', 62.65, -160.18333, 88, 88, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26644');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26645', 'BUCKLAND', 'AK', 'BUCKLAND AIRPORT', 65.98333, -161.13333, 30, 30, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26645');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26646', 'CAPE ROMANZOF', 'AK', 'CAPE ROMANZOF LRRS ARPT', 61.78333, -166.03333, 479, 479, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26646');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26647', 'ST MARY''S', 'AK', 'ST MARY''S AIRPORT', 62.06167, -163.3, 312, 312, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26647');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26648', 'NOATAK', 'AK', 'NOATAK AIRPORT', 67.56611, -162.975, 88, 88, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26648');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26649', 'RED DOG', 'AK', 'RED DOG AIRPORT', 68.03139, -162.90306, 974, 974, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26649');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26650', 'EMMONAK', 'AK', 'EMMONAK', 62.78528, -164.49111, 14, 14, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26650');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26651', 'HOOPER BAY', 'AK', 'HOOPER BAY AIRPORT', 61.52417, -166.14667, 18, 18, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26651');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26652', 'KALSKAG', 'AK', 'KALSKAG AIRPORT', 61.53639, -160.34139, 55, 55, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26652');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26653', 'TOKSOOK BAY', 'AK', 'TOKSOOK BAY AIRPORT', 60.54139, -165.08722, 59, 59, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26653');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26654', 'RUSSIAN MISSION', 'AK', 'RUSSIAN MISSION AIRPORT', 61.775, -161.31944, 51, 51, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26654');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26655', 'RED DOG MINE', 'AK', 'RED DOG MINE 3 SSW', 68.0277, -162.9212, 942, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26655');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26703', 'GAMBELL', 'AK', 'GAMBELL AIRPORT', 63.76667, -171.73278, 28, 26, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26703');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '26704', 'SAVOONGA', 'AK', 'SAVOONGA AIRPORT', 63.68667, -170.49333, 55, 53, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='26704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27401', 'BARTER ISLAND', 'AK', 'BARTER ISLAND AIRPORT', 70.13389, -143.57694, 2, 5, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27406', 'DEADHORSE', 'AK', 'DEADHORSE AIRPORT', 70.1917, -148.4772, 61, 60, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27406');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27408', 'KUPRAUK', 'AK', 'UGNU-KUPRAUK AIRPORT', 70.33083, -149.5975, 67, 67, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27408');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27502', 'BARROW', 'AK', 'W POST-WILL ROGERS MEMORIAL AIRPORT', 71.2834, -156.7815, 31, 13, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27502');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27503', 'WAINWRIGHT', 'AK', 'WAINWRIGHT AIRPORT', 70.63917, -159.995, 30, 30, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27503');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27515', 'NUIQSUT', 'AK', 'NUIQSUT AIRPORT', 70.21167, -151.00167, 57, 57, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27515');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27516', 'BARROW', 'AK', 'BARROW 4 ENE', 71.3213, -156.611, 15, 15, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27516');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27517', 'DEADHORSE', 'AK', 'ALPINE AIRSTRIP', 70.34417, -150.94472, 18, 18, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27517');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '27518', 'ATQASUK', 'AK', 'ATQASUK EDWARD BURNELL SR. MEMORIAL AIRPORT', 70.46722, -157.43583, 96, 96, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='27518');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '33126', 'SIGONELLA', 'NULL', 'SIGONELLA NAVAL AIR STATION', 37.4, 14.91667, 82, 77, 1
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='33126');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '33209', 'SOUDA BAY', 'NULL', 'SOUDA BAY CRETE NAVAL AIR STATION', 35.53333, 24.15, 480, 480, 2
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='33209');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '34113', 'NAPLES', 'NULL', 'NAPLES NAVAL AIR STATION', 40.9, 14.3, 220, 289, 1
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='34113');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40308', 'YAP ISLAND', 'FM', 'YAP INTERNATIONAL AIRPORT', 9.48333, 138.08333, 44, 56, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40308');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40309', 'KOROR', 'PW', 'WEST CAROLINE ISLANDS, PACIFIC', 7.33333, 134.48333, 90, 109, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40309');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40310', 'BABELTHUAP ISLAND', 'PW', 'BABELTHUAP/KOROR AIRPORT', 7.36722, 134.54417, 176, 176, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40310');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40504', 'POHNPEI', 'FM', 'WEATHER SERVICE BUILDING', 6.9667, 158.2167, 120, 151, 11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40504');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40505', 'WENO ISLAND', 'FM', 'CHUUK INTERNATIONAL AIRPORT', 7.45, 151.83333, 5, 8, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40505');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40507', 'NULL', 'FM', 'KAPINGAMARANGI', 1.08333, 154.8, 9, NULL, 11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40507');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40509', 'POHNPEI', 'FM', 'POHNPEI INTERNATIONAL AIRPORT', 6.98333, 158.2, 9, NULL, 11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40509');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40604', 'KWAJALEIN', 'MH', 'BUCHOLZ AAF, KWAJALEIN KMR ATOL AP', 8.73333, 167.73333, 7, 26, 12
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40604');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '40710', 'MAJURO', 'MH', 'WEATHER SERVICE OFFICE BUILDING', 7.06497, 171.27203, 10, 10, 12
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='40710');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41231', 'CUBI POINT', 'NULL', 'CUBI POINT NAVAL AIR STATION', 14.8, 120.26667, 60, 56, 8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41231');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41414', 'ANDERSEN, MARIANA ISLAND, GUAM', 'GU', 'ANDERSEN AFB AIRPORT', 13.58333, 144.91667, 612, 612, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41414');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41415', 'AGANA', 'GU', 'GUAM INTERNATIONAL AIRPORT', 13.48333, 144.8, 254, 250, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41415');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41418', 'OBYAN', 'MP', 'FRANCISCO C. ADA/SAIPAN INTERNATIONAL ARPT', 15.11667, 145.71667, 215, 210, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41418');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41419', 'ROTA ISLAND, NORTHERN MARIANA IS.', 'MP', 'ROTA INTERNATIONAL AIRPORT', 14.1717, 145.2428, 588, 607, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41419');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41420', 'TINIAN ISLAND, NORTHERN MARIANA IS.', 'MP', 'TINIAN INTERNATIONAL AIRPORT', 14.99917, 145.62111, 271, 271, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41420');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41606', 'WAKE ISLAND', 'UM', 'WAKE ISLAND AIRFIELD', 19.28333, 166.65, 23, 12, 12
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41606');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '41818', 'OBYAN', 'CQ', 'FRANCISCO C. ADA/SAIPAN INTERNATIONAL ARPT', 15.119, 145.729, 210, 210, 10
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='41818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '42215', 'FUTEMA', 'NULL', 'OKINAWA MARINE CORPS AIR STATION', 26.26667, 127.75, 247, 246, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='42215');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '43285', 'SASEBO', 'NULL', 'NAVSTA', 33.16667, 129.71667, 10.9, 49, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='43285');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '43319', 'ATSUGI', 'NULL', 'ATSUGI US NAVAL AIR STATION', 35.45, 139.45, 167, 170, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='43319');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '43323', 'YOKOSUKA', 'NULL', 'YOKOSUKA NAVAL STATION', 35.28333, 139.66667, 161, 10, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='43323');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '43324', 'IWAKUNI', 'NULL', 'AIRFIELD & HARBOR OPERATIONS', 34.15, 132.23333, 7, 8, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='43324');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '44402', 'MISAWA', 'NULL', 'MISAWA NAVAL AIR STATION', 40.7, 141.36667, 115, 130, 9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='44402');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '45715', 'SHEMYA', 'AK', 'EARECKSON AIR STATION AIRPORT', 52.71667, 174.1, 122, 97, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='45715');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46401', 'CHANDALAR LAKE', 'AK', 'CHANDALAR LAKE AIRPORT', 67.504, -148.483, 0, 1920, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46402', 'NABESNA', 'AK', 'DEVILS MOUNTAIN LODGE AIRPORT', 62.40167, -142.99528, 2880, 2880, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46402');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46403', 'MCKINLEY PARK', 'AK', 'MCKINLEY NATIONAL PARK AIRPORT', 63.73333, -148.91667, 1720, 1720, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46403');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46404', 'MCCARTHY', 'AK', 'MCCARTHY AIRPORT', 61.43722, -142.90389, 1531, 1531, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46404');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46405', 'ARCTIC VILLAGE', 'AK', 'ARCTIC VILLAGE AIRPORT', 68.11472, -145.57944, 2092, 2092, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46405');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46406', 'CANTWELL', 'AK', 'CANTWELL AIRPORT', 63.39139, -148.95556, 2190, 2490, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46406');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '46407', 'EUREKA', 'AK', 'SKELTON AIRPORT', 61.94889, -147.16889, 3289, 3289, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='46407');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53002', 'ARTESIA', 'NM', 'ARTESIA 2 WNW', 32.8546, -104.4569, 3501, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53002');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53003', 'GRANTS', 'NM', 'GRANTS 2 S', 35.1188, -107.8303, 6443, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53003');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53004', 'BLUFF', 'UT', 'BLUFF 32 NW', 37.601, -109.9886, 6428, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53004');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53005', 'BUENA VISTA', 'CO', 'BUENA VISTA 2 SSE', 38.811, -106.1219, 7933, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53005');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53006', 'AMADO', 'AZ', 'AMADO 23 W', 31.6922, -111.4481, 3300, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53006');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53007', 'COLORADO SPRINGS', 'CO', 'COLORADO SPRINGS 23 NW', 39.0878, -105.0879, 7872, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53007');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53008', 'CLOVIS', 'NM', 'CLOVIS 7 N', 34.5119, -103.1744, 4316, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53008');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53009', 'RAMAH', 'NM', 'RAMAH 9 SE', 35.0461, -108.3635, 7164, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53009');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53010', 'MONTICELLO', 'UT', 'MONTICELLO 24 NW', 38.1402, -109.6165, 5025, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53010');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53011', 'TSAILE', 'AZ', 'TSAILE 1 SSW', 36.292, -109.2181, 7071, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53011');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53012', 'BLANDING', 'UT', 'BLANDING 26 SSW', 37.2667, -109.6074, 4390, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53012');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53013', 'CEDAR CITY', 'UT', 'CEDAR CITY 18 SSE', 37.4572, -113.2248, 5096, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53013');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53014', 'MEXICAN HAT', 'UT', 'MEXICAN HAT 10 NW', 37.2507, -109.9877, 6364, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53014');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53015', 'HAGERMAN', 'NM', 'HAGERMAN 10 ESE', 33.0875, -104.1617, 3552, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53015');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53016', 'VAUGHN', 'NM', 'VAUGHN 36 SSE', 34.1373, -104.9303, 5034, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53016');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53017', 'AZTEC', 'NM', 'AZTEC 43 E', 36.7499, -107.2189, 7024, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53017');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53119', 'HANFORD', 'CA', 'HANFORD MUNICIPAL AIRPORT', 36.31889, -119.62889, 249, 242, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53119');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53120', 'RAMONA', 'CA', 'RAMONA AIRPORT', 33.0375, -116.91583, 1393, 1393, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53120');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53121', 'OCEANSIDE', 'CA', 'OCEANSIDE MUNICIPAL ARPT', 33.21944, -117.34944, 28, 28, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53121');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53123', 'LAS VEGAS', 'NV', 'NORTH LAS VEGAS AIRPORT', 36.21167, -115.19583, 2203, 2205, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53123');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53126', 'GLENDALE', 'AZ', 'GLENDALE MUNICIPAL AIRPORT', 33.52722, -112.295, 1066, 1066, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53126');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53127', 'LAS VEGAS', 'NV', 'HENDERSON EXECUTIVE ARPT', 35.97611, -115.1325, 2458, 2458, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53127');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53128', 'CHANDLER', 'AZ', 'CHANDLER MUNICIPAL AIRPORT', 33.26889, -111.81278, 1243, 1243, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53128');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53129', 'COLORADO CITY', 'AZ', 'COLORADO CITY MUNI AIRPORT', 36.95972, -113.01389, 4874, 4874, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53130', 'LOS ANGELES', 'CA', 'WHITMAN AIRPORT', 34.25917, -118.41333, 1003, 1003, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53130');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53131', 'TUCSON', 'AZ', 'TUCSON 11 W', 32.2395, -111.1696, 2733, 2733, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53131');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53132', 'ELGIN', 'AZ', 'ELGIN 5 S', 31.5907, -110.5087, 4811, 4811, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53132');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53135', 'BULLHEAD CITY', 'AZ', 'LAUGHLIN/BULLHEAD INTERNATIONAL AIRPORT', 35.15722, -114.55944, 695, 695, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53135');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53136', 'MERCURY', 'NV', 'MERCURY 3 SSW', 36.624, -116.0225, 3284, 3284, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53138', 'BAKER', 'NV', 'BAKER 5 W', 39.0118, -114.209, 6617, 6592, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53138');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53139', 'STOVEPIPE WELLS', 'CA', 'STOVEPIPE WELLS 1 SW', 36.602, -117.1449, 84, 84, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53139');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53141', 'LOS ALAMITOS', 'CA', 'LOS ALAMITOS AAF AIRPORT', 33.79, -118.05194, 35, 35, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53143', 'SAN DIEGO/EL CAJON', 'CA', 'GILLESPIE FIELD AIRPORT', 32.82611, -116.9725, 387, 387, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53143');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53144', 'EDWARDS', 'CA', 'NORTH AUXILIARY AIRFIELD EDWARDS AFB', 34.98833, -117.86472, 2283, 2278, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53144');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53145', 'NELLIS AIR FORCE BASE', 'NV', 'TONOPAH TEST RANGE #74', 37.61722, -116.26417, 5760, 5760, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53145');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53146', 'FORT HUACHUCA', 'AZ', 'PIONEER AIRFIELD', 31.60722, -110.42806, 4768, 4768, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53146');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53147', 'NELLIS AIR FORCE BASE', 'NV', 'INDIAN SPRING AUX AIRFIELD', 36.583, -115.667, 0, 3123, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53147');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53149', 'TORREY', 'UT', 'TORREY 7 E', 38.3028, -111.2935, 6204, 6204, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53149');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53150', 'YOSEMITE VILLAGE', 'CA', 'YOSEMITE VILLAGE 12 W', 37.7592, -119.8208, 6620, 6620, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53150');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53151', 'FALLBROOK', 'CA', 'FALLBROOK 5 NE', 33.4392, -117.1904, 1140, 1140, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53151');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53152', 'SANTA BARBARA', 'CA', 'SANTA BARBARA 11 W', 34.4141, -119.8796, 18, 18, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53152');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53154', 'YUMA', 'AZ', 'YUMA 27 ENE', 32.835, -114.1884, 620, 620, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53154');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53155', 'WILLIAMS', 'AZ', 'WILLIAMS 35 NNW', 35.7552, -112.3374, 5990, 5990, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53155');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53156', 'PHOENIX', 'AZ', 'PHOENIX 7 S', 33.3471, -112.0832, 1404, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53156');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53159', 'CAMERON', 'AZ', 'CAMERON 25 SSE', 35.5037, -111.3445, 4807, 533, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53159');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53160', 'CAMP VERDE', 'AZ', 'CAMP VERDE 3 N', 34.6136, -111.8431, 3434, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53160');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53162', 'COOLIDGE', 'AZ', 'COOLIDGE 5 W', 32.9918, -111.5326, 1425, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53162');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53163', 'SOCORRO', 'NM', 'SOCORRO 17 WSW', 33.9795, -107.1872, 10486, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53163');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53164', 'PAGE', 'AZ', 'PAGE 9 WSW', 36.8637, -111.6012, 3254, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53164');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53165', 'DELTA', 'UT', 'DELTA 4 NE', 39.3872, -112.5095, 4761, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53165');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53166', 'NULL', 'UT', 'ST. GEORGE 15 NE', 37.2157, -113.3794, 3417, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53166');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53167', 'MIDWAY', 'UT', 'MIDWAY 3 NE', 40.5467, -111.4137, 5748, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53167');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53168', 'AJO', 'AZ', 'AJO 29 S', 31.9497, -112.8013, 1661, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53168');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53169', 'KAYENTA', 'AZ', 'KAYENTA 16 WSW', 36.6823, -110.5424, 7274, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53169');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53170', 'HEBER', 'AZ', 'HEBER 3 SE', 34.3983, -110.5656, 6625, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53170');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53171', 'MILFORD', 'UT', 'MILFORD 42 WNW', 38.5943, -113.7511, 5255, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53171');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53172', 'LAKE HAVASU CITY', 'AZ', 'LAKE HAVASU CITY 19 SE', 34.2529, -114.134, 416, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53172');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53173', 'NAGEEZI', 'NM', 'NAGEEZI 18 SSW', 36.0346, -107.9045, 6449, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53173');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53175', 'CORONA', 'CA', 'CORONA MUNICIPAL AIRPORT', 33.89765, -117.60243, 533, NULL, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53175');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53176', 'GILA BEND', 'AZ', 'GILA BEND 3 ENE', 32.9649, -112.6697, 780, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53176');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53801', 'LEWISBURG', 'WV', 'GREENBRIER VALLEY ARIPORT', 37.86667, -80.4, 2302, 2302, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53801');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53802', 'MATTOON/CHARLESTON', 'IL', 'COLES COUNTY MEMO AIRPORT', 39.47806, -88.28028, 721, 721, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53803', 'OWENSBORO', 'KY', 'OWENSBORO-DAVIESS COUNTY AIRPORT', 37.75, -87.16667, 403, 406, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53806', 'OXFORD', 'MS', 'OXFORD UNIV', 34.38333, -89.55, 452, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53808', 'HATTIESBURG-LAUREL', 'MS', 'HATTIESBURG-LAUREL RGNL AP', 31.46667, -89.33333, 293, 298, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53817', 'MOULTRIE', 'GA', 'MOULTRIE MUNICIPAL AIRPORT', 31.08333, -83.8, 283, 294, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53818', 'ABINGDON', 'VA', 'VIRGINIA HIGHLANDS AIRPORT', 36.68333, -82.03333, 2070, 2087, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53819', 'ATLANTA', 'GA', 'PCHTRE CTY-FALCON FLD ARPT', 33.35528, -84.56694, 798, 798, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53820', 'EVERGREEN', 'AL', 'MIDDLETON FIELD AIRPORT', 31.41556, -87.04417, 258, 258, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53822', 'OLNEY-NOBLE', 'IL', 'OLNEY-NOBLE AIRPORT', 38.72167, -88.17639, 482, 482, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53838', 'GAINESVILLE', 'GA', 'LEE GILMER MEMORIAL ARPT', 34.27194, -83.83028, 1275, 1275, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53841', 'FRANKFORT', 'KY', 'CAPITAL CITY AIRPORT', 38.18472, -84.90333, 804, 804, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53842', 'INDIANAPOLIS', 'IN', 'EAGLE CREEK AIRPARK ARPT', 39.825, -86.29583, 823, 823, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53843', 'ANDALUSIA (FT RUCKER)', 'AL', 'ANDALUSIA-OPP MUNICIPAL AIRPORT', 31.30889, -86.39389, 310, 310, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53843');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53844', 'LANCASTER', 'OH', 'FAIRFIELD COUNTY AIRPORT', 39.75556, -82.65722, 869, 869, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53844');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53846', 'KINGS BAY', 'GA', 'KINGS BAY/NAVAL STATION', 30.8, -81.5, 50, 50, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53847', 'MILTON', 'FL', 'WHITING FIELD NAVAL AIR STATION-SOUTH', 30.70444, -87.02306, 178, 178, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53848', 'MILTON', 'FL', 'CHOCTAW NAVAL OUTLYING FIELD', 30.50694, -86.95972, 102, 111, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53848');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53849', 'MERIDIAN', 'MS', 'RANGE B', 32.8, -88.83333, 540, 540, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53850', 'CLEMSON', 'SC', 'OCCONEE COUNTY RGNL ARPT', 34.67194, -82.88639, 891, 891, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53852', 'DECATUR', 'AL', 'PRYOR FIELD REGIONAL ARPT', 34.6525, -86.94528, 592, 592, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53853', 'DESTIN', 'FL', 'DESTIN-FT.WALTON BEACH APT', 30.4, -86.47167, 22, 22, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53854', 'ORANGEBURG', 'SC', 'ORANGEBURG MUNICIPAL ARPT', 33.46167, -80.85806, 197, 197, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53854');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53855', 'HAMILTON', 'OH', 'BUTLER CO. REGIONAL ARPT', 39.36444, -84.52472, 634, 634, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53858', 'PASCAGOULA', 'MS', 'TRENT LOTT INTL AIRPORT', 30.46361, -88.53194, 18, 12, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53859', 'DAYTON', 'OH', 'DAYTON-WRIGHT BROTHERS APT', 39.59361, -84.22639, 962, 962, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53859');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53860', 'JACKSONVILLE', 'FL', 'CRAIG MUNICIPAL AIRPORT', 30.3361, -81.5147, 41, 41, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53860');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53861', 'FORT RUCKER/OZARK', 'AL', 'LOWE ARMY HELIPORT (FORT RUCKER)', 31.35583, -85.75111, 244, 244, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53861');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53862', 'PERRY-FOLEY', 'FL', 'PERRY-FOLEY AIRPORT', 30.07194, -83.57361, 45, 45, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53862');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53863', 'ATLANTA', 'GA', 'DEKALB-PEACHTREE AIRPORT', 33.875, -84.30222, 1002, 992, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53863');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53864', 'ALABASTER', 'AL', 'SHELBY COUNTY AIRPORT', 33.17833, -86.78167, 565, 584, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53864');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53865', 'SLIDELL', 'LA', 'SLIDELL AIRPORT', 30.34333, -89.82222, 27, 27, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53865');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53866', 'SHELBYVILLE', 'IN', 'SHELBYVILLE MINUCIPAL ARPT', 39.57806, -85.80333, 804, 804, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53866');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53867', 'COLUMBIA', 'SC', 'COLUMBIA OWENS DOWNTOWN AP', 33.97056, -80.99583, 212, 212, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53867');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53868', 'OAK RIDGE', 'TN', 'OAK RIDGE', 36.0236, -84.2375, 910, 910, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53869', 'BLYTHEVILLE', 'AR', 'BLYTHEVILLE MUNI AIRPORT', 35.94028, -89.83083, 255, 255, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53869');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53870', 'GASTONIA', 'NC', 'GASTONIA MUNICIPAL AIRPORT', 35.19667, -81.15583, 797, 797, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53871', 'ROCK HILL', 'SC', 'RK HL/YORK CO/BRYNT FD APT', 34.98694, -81.0575, 669, 669, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53872', 'MONROE', 'NC', 'MONROE AIRPORT', 35.01694, -80.62056, 679, 672, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53872');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53873', 'CARTERSVILLE', 'GA', 'CARTERSVILLE AIRPORT', 34.12306, -84.84861, 763, 763, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53873');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53874', 'GREENWOOD', 'SC', 'GREENWOOD COUNTY AIRPORT', 34.24861, -82.15917, 631, 631, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53874');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53877', 'ASHEVILLE', 'NC', 'ASHEVILLE 8 SSW', 35.4945, -82.6142, 2151, 2151, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53877');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53878', 'ASHEVILLE', 'NC', 'ASHEVILLE 13 S', 35.4185, -82.5567, 2103, 2103, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53878');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53879', 'LEE''S SUMMIT', 'MO', 'LEE''S SUMMIT MUNICIPAL APT', 38.95972, -94.37139, 997, 1004, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53879');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53881', 'BLACKSBURG', 'VA', 'VIRGINIA TECH AIRPORT', 37.2075, -80.40778, 2132, 2132, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53881');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53882', 'BAINBRIDGE', 'GA', 'DCATR CO INDUS AIRPK ARPT', 30.98333, -84.63333, 141, 141, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53883', 'BRUNSWICK', 'GA', 'BRUNSWICK GOLDEN ISLES APT', 31.25889, -81.46611, 26, 26, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53884', 'NEWNAN', 'GA', 'NEWNAN COWETA COUNTY ARPT', 33.31194, -84.77028, 970, 970, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53884');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53885', 'DALTON', 'GA', 'DALTON MUNICIPAL AIRPORT', 34.72167, -84.86889, 710, 710, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53885');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53886', 'HENDERSON', 'KY', 'HENDERSON CITY-COUNTY ARPT', 37.8, -87.68333, 387, 386, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53887', 'CENTRALIA', 'IL', 'CENTRALIA MUNICIPAL ARPT', 38.51472, -89.09194, 534, 534, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53887');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53888', 'LEXINGTON', 'NC', 'DAVIDSON COUNTY AIRPORT', 35.78111, -80.30361, 733, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53888');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53889', 'FLORA', 'IL', 'FLORA MUNICIPAL AIRPORT', 38.66472, -88.45278, 472, 472, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53890', 'RUTHERFORDTON', 'NC', 'RURFTON CO-MARCHMAN FLD AP', 35.42806, -81.935, 1078, 1078, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53890');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53891', 'FAIRFIELD', 'IL', 'FAIRFIELD MUNICIPAL ARPT', 38.37861, -88.4125, 436, 436, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53892', 'JEFFERSON', 'NC', 'ASHE COUNTY AIRPORT', 36.43222, -81.41944, 3180, 3180, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53892');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53893', 'COLUMBUS/W. PT/STARKVILLE', 'MS', 'GOLDEN TRIANGLE RGNL APT', 33.45, -88.58333, 264, 264, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53894', 'JACKSONVILLE', 'FL', 'HERLONG AIRPORT', 30.27778, -81.80583, 87, 87, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53895', 'GALAX HILLSVILLE', 'VA', 'TWIN COUNTY AIRPORT', 36.76611, -80.82333, 2693, 2693, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53896', 'HUNTINGBURG', 'IN', 'HUNTINGBURG AIRPORT', 38.24889, -86.95361, 529, 529, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53897', 'HARRISBURG', 'IL', 'HARRISBURG-RALEIGH AIRPORT', 37.81139, -88.54889, 396, 396, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53898', 'WASHINGTON', 'GA', 'WASHINGTON-WILKES CO ARPT', 33.77972, -82.81639, 645, 645, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53899', 'LINCOLNTON', 'NC', 'LICONTN-LINCOLN CO RGNL AP', 35.48306, -81.16111, 875, 875, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53899');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53901', 'WEST PLAINS', 'MO', 'WEST PLAINS MUNICIPAL ARPT', 36.87806, -91.9025, 1223, 1228, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53902', 'CONROE', 'TX', 'MONTGOMERY COUNTY AIRPORT', 30.35667, -95.41389, 245, 247, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53903', 'HUNTSVILLE', 'TX', 'HUNTSVILLE MUNICIPAL ARPT', 30.74389, -95.58611, 366, 366, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53903');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53904', 'ST CHARLES', 'MO', 'ST CHARLES CO SMARTT ARPT', 38.92861, -90.42806, 436, 438, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53905', 'SHREVEPORT', 'LA', 'SHREVEPORT DOWNTOWN ARPT', 32.54278, -93.745, 179, 179, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53905');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53907', 'ARLINGTON', 'TX', 'ARLINGTON MUNICIPAL AIRPORT', 32.66361, -97.09389, 630, 630, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53907');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53908', 'TULSA', 'OK', 'RICHARD LLOYD JONES JR APT', 36.03944, -95.98444, 638, 625, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53909', 'FORT WORTH', 'TX', 'FORT WORTH ALLIANCE ARPT', 32.97333, -97.31806, 685, 722, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53910', 'HOUSTON', 'TX', 'HOOKS MEMORIAL AIRPT', 30.0675, -95.55611, 152, 152, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53911', 'TERRELL', 'TX', 'TERRELL MUNICIPAL AIRPORT', 32.71, -96.26722, 475, 475, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53912', 'CORSICANA', 'TX', 'CAMPBELL FLD-CORSICANA MU', 32.03111, -96.39889, 448, 446, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53912');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53913', 'GUTHRIE', 'OK', 'GUTHRIE MUNICIPAL AIRPORT', 35.8517, -97.4142, 1068, 1068, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53913');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53914', 'MC KINNEY', 'TX', 'MC KINNEY MUNICIPAL ARPT', 33.19028, -96.59139, 586, 585, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53914');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53915', 'NEW IBERIA', 'LA', 'ACADIANA REGIONAL AIRPORT', 30.0375, -91.8839, 24, 24, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53915');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53916', 'CHILLICOTHE', 'MO', 'AGRICULTURAL SCIENCE CENTER', 39.82333, -93.57917, 769, 769, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53916');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53917', 'NEW ORLEANS', 'LA', 'LAKEFRONT AIRPORT', 30.0494, -90.0288, 9, 9, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53917');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53918', 'MOUNTAIN HOME', 'AR', 'OZARK REGIONAL AIRPORT', 36.36889, -92.47028, 928, 928, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53919', 'MONTICELLO', 'AR', 'MONTCELO MUNI/ELLIS FLD AP', 33.6361, -91.7556, 290, 270, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53920', 'RUSSELLVILLE', 'AR', 'MUNICIPAL AIRPORT', 35.25778, -93.09472, 403, 403, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53920');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53921', 'MOUNT IDA', 'AR', 'MOUNT IDA', 34.5467, -93.57806, 702, 702, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53921');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53922', 'FAYETTEVILLE/SPRINGDALE', 'AR', 'NW ARKANSAS REGIONAL ARPT', 36.28333, -94.3, 1287, 1319, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53922');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53925', 'DE QUEEN', 'AR', 'J.L. HELMS SEVIER CO ARPT', 34.05, -94.40083, 355, 355, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53926', 'STILLWATER', 'OK', 'STILLWATER 2 W', 36.1181, -97.0914, 890, 890, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53927', 'STILLWATER', 'OK', 'STILLWATER 5 WNW', 36.1346, -97.1082, 888, 888, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53928', 'BRENHAM', 'TX', 'BRENHAM MUNICIPAL AIRPORT', 30.21889, -96.37417, 308, 308, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53929', 'ARDMORE', 'OK', 'ARDMORE DOWNTOWN EXEC ARPT', 34.14694, -97.1225, 844, 844, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53930', 'ADA', 'OK', 'ADA MUNICIPAL AIRPORT', 34.80417, -96.67111, 1016, 1016, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53931', 'KAISER LAKE OZARK', 'MO', 'LEE C FINE MEMORIAL ARPT', 38.09639, -92.55306, 869, 869, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53932', 'ATOKA', 'OK', 'ATOKA MUNICIPAL AIRPORT', 34.39833, -96.14806, 590, 590, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53933', 'ALVA', 'OK', 'ALVA REGIONAL AIRPORT', 36.77306, -98.66972, 1474, 1474, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53934', 'BATESVILLE', 'AR', 'BATESVILLE REGIONAL ARPT', 35.72611, -91.64722, 463, 463, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53934');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53935', 'CHICKASHA', 'OK', 'CHICKASHA MUNICIPAL ARPT', 35.09611, -97.96611, 1150, 1150, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53935');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53936', 'CHANDLER', 'OK', 'CHANDLER MUNICIPAL AIRPORT', 35.72361, -96.82, 984, 984, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53936');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53937', 'DURANT', 'OK', 'EAKER FIELD AIRPORT', 33.94222, -96.39444, 698, 698, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53938', 'DUNCAN', 'OK', 'HALLIBURTON FIELD AIRPORT', 34.47083, -97.95083, 1113, 1113, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53938');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53939', 'NEWTON', 'KS', 'NEWTON-CITY-COUNTY AIRPORT', 38.06778, -97.27472, 1532, 1532, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53939');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53940', 'CLAREMORE', 'OK', 'CLAREMORE REGIONAL AIRPORT', 36.29417, -95.47944, 725, 725, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53941', 'GROVE', 'OK', 'GROVE MUNICIPAL AIRPORT', 36.605, -94.73833, 833, 833, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53941');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53942', 'GEORGETOWN', 'TX', 'GEORGETOWN MUNICIPAL ARPT', 30.67917, -97.67944, 787, 787, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53943', 'NATCHITOCHES', 'LA', 'NATCHITOCHES REGIONAL ARPT', 31.73583, -93.09917, 121, 121, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53944', 'JACKSONVILLE', 'IL', 'JACKSONVILLE MUNICIPAL AIRPORT', 39.77972, -90.23833, 624, 624, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53944');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53945', 'JASPER', 'TX', 'JASPER COUNTY-BELL FLD APT', 30.88556, -94.03472, 213, 213, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53945');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53946', 'SALLISAW', 'OK', 'SALLISAW MUNICIPAL AIRPORT', 35.43806, -94.80278, 528, 528, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53946');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53947', 'WATONGA', 'OK', 'WATONGA AIRPORT', 35.86444, -98.42056, 1548, 1548, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53948', 'NACOGDOCHES', 'TX', 'A L MANGHAM JR RGNL ARPT', 31.57778, -94.70944, 355, 355, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53948');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53949', 'OKMULGEE', 'OK', 'OKMULGEE MUNICIPAL AIRPORT', 35.66806, -95.94861, 720, 720, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53949');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53950', 'PITTSFIELD', 'IL', 'PITTSFIELD-PENSTON MUNI AP', 39.63861, -90.77833, 710, 710, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53951', 'PAULS VALLEY', 'OK', 'PAULS VALLEY MUNI AIRPORT', 34.71083, -97.22306, 968, 968, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53951');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53952', 'WACO', 'TX', 'MC GREGOR EXECUTIVE ARPT', 31.48472, -97.31639, 592, 592, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53952');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53953', 'POTEAU', 'OK', 'ROBERT S KERR AIRPORT', 35.02139, -94.62111, 451, 451, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53954', 'ROGERS', 'AR', 'ROGERS MUNI-CARTER FLD APT', 36.37222, -94.10667, 1353, 1353, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53954');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53955', 'SILOAM SPRINGS', 'AR', 'SMITH FIELD AIRPORT', 36.19056, -94.49083, 1193, 1193, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53955');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53956', 'TAHLEQUAH', 'OK', 'TAHLEQUAH MUNICIPAL ARPT', 35.92889, -95.00444, 873, 873, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53956');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53957', 'WOODWARD', 'OK', 'WEST WOODWARD AIRPORT', 36.43667, -99.52083, 2188, 2188, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53958', 'RUSTON', 'LA', 'RUSTON REGIONAL AIRPORT', 32.51444, -92.58833, 311, 311, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53959', 'WEST MEMPHIS', 'AR', 'WEST MEMPHIS MUNICIPAL APT', 35.135, -90.23444, 214, 214, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53960', 'LAFAYETTE', 'LA', 'LAFAYETTE 13 SE', 30.0918, -91.8731, 35, 35, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53961', 'MONROE', 'LA', 'MONROE 26 N', 32.8833, -92.1165, 88, 88, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53962', 'MOUNT PLEASANT', 'TX', 'MOUNT PLEASANT RGNL ARPT', 33.09556, -94.96139, 363, 363, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53963', 'CLARKSVILLE', 'TX', 'CRKVIL-RED RIVER CO ARPT', 33.59306, -95.06361, 440, 440, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53963');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53964', 'DECATUR', 'TX', 'DECATUR MUNICIPAL AIRPORT', 33.25444, -97.58056, 1047, 1047, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53964');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53965', 'GRAHAM', 'TX', 'GRAHAM MUNICIPAL AIRPORT', 33.11, -98.55528, 1123, 1123, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53965');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53966', 'MIDLOTHIAN/WAXAHACHIE', 'TX', 'MID-WAY REGIONAL AIRPORT', 32.45611, -96.9125, 749, 713, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53967', 'SHERMAN/DENISON', 'TX', 'GRAYSON COUNTY AIRPORT', 33.71417, -96.67361, 749, 749, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53968', 'PALESTINE', 'TX', 'PALESTINE 6 WNW', 31.7796, -95.7232, 383, 383, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53969', 'VERNON', 'TX', 'WILBARGER COUNTY AIRPORT', 34.22556, -99.28389, 1265, 1265, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53969');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53970', 'MESQUITE', 'TX', 'MESQUITE METROPOLITAN ARPT', 32.74694, -96.53056, 447, 447, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53970');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53971', 'JACKSONVILLE', 'TX', 'CHEROKEE COUNTY AIRPORT', 31.86944, -95.2175, 677, 677, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53972', 'HILLSBORO', 'TX', 'HILLSBORO MUNICIPAL ARPT', 32.08361, -97.09722, 685, 685, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53972');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53973', 'HEARNE', 'TX', 'HEARNE MUNICIPAL AIRPORT', 30.87194, -96.62222, 285, 285, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53974', 'MANHATTAN', 'KS', 'MANHATTAN 6 SSW', 39.1027, -96.6098, 1137, 1137, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53974');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53975', 'LANCASTER', 'TX', 'LANCASTER AIRPORT', 32.57917, -96.71917, 501, 501, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53976', 'BRIDGEPORT', 'TX', 'BRIDGEPORT MUNICIPAL ARPT', 33.17528, -97.82833, 851, 852, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53977', 'GRANBURY', 'TX', 'GRANBURY MUNICIPAL ARPT', 32.44444, -97.81694, 778, 778, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53978', 'QUITMAN', 'TX', 'WOOD COUNTY AIRPORT, MINEOLA', 32.74222, -95.49639, 434, 434, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53979', 'GIDDINGS', 'TX', 'GIDDINGS-LEE COUNTY AIRPORT', 30.16917, -96.98, 485, 485, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53979');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53981', 'CLEBURNE', 'TX', 'CLEBURNE MUNICIPAL AIRPORT', 32.35389, -97.43389, 854, 854, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53981');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53982', 'GILMER', 'TX', 'FOX STEPHENS FIELD - GILMER MINICIPAL AIRPORT', 32.69889, -94.94889, 415, 415, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53982');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53983', 'LLANO', 'TX', 'LLANO MUNICIPAL AIRPORT', 30.78361, -98.66194, 1102, 1102, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53983');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53984', 'GRAND PRAIRIE', 'TX', 'GRAND PRAIRIE MUNICIPAL AIRPORT', 32.69861, -97.04667, 589, 589, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53984');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53985', 'SEARCY', 'AR', 'SEARCY MUNICIPAL AIRPORT', 35.21194, -91.73694, 264, 264, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53986', 'ENID', 'OK', 'ENID WOODRING REGIONAL AIRPORT', 36.38333, -97.8, 1167, 1167, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53986');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53988', 'FORT POLK', 'LA', 'FULLERTON LANDING STRIP', 31.15, -92.96667, 310, 310, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53988');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53989', 'FORT RILEY', 'KS', 'MULTIPLE PURPOSE RANGE', 39.29417, -96.92639, 1264, 1264, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53990', 'IDABEL', 'OK', 'MC CURTAIN COUNTY REGIONAL AIRPORT', 33.90944, -94.85944, 472, 472, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53991', 'OAKDALE', 'LA', 'ALLEN PARISH AIRPORT', 30.75028, -92.68833, 107, 107, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53992', 'BASTROP', 'LA', 'MOREHOUSE MEMORIAL AIRPORT', 32.75611, -91.88056, 167, 167, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53993', 'CUSHING', 'OK', 'CUSHING MUNICIPAL AIRPORT', 35.95, -96.77306, 916, 916, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53995', 'SULPHUR', 'LA', 'SOUTHLAND FIELD AIRPORT', 30.13139, -93.37611, 11, 11, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53995');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53996', 'MENA', 'AR', 'MENA INTERMOUNTAIN MUNICIPAL AIRPORT', 34.54528, -94.2025, 1079, 1079, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53997', 'COMANCHE', 'TX', 'COMANCHE COUNTY-CITY AIRPORT', 31.91667, -98.60028, 1388, 1388, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53998', 'ORANGE', 'TX', 'ORANGE COUNTY AIRPORT', 30.06917, -93.80361, 13, 13, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53998');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '53999', 'EL RENO', 'OK', 'EL RENO REGIONAL AIRPORT', 35.47278, -98.00583, 1417, 1417, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='53999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54704', 'NORWOOD', 'MA', 'NORWOOD MEMORIAL AIRPORT', 42.19083, -71.17361, 50, 50, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54723', 'FORT INDIANTOWN GAP (ANNVILLE)', 'PA', 'MUIR ARMY AIRFIELD (FORT INDIANTOWN GAP)', 40.43333, -76.56667, 488, 489, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54723');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54728', 'WHITEFIELD', 'NH', 'MT WASHINGTON RGNL AIRPORT', 44.3675, -71.545, 1074, 1074, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54728');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54733', 'BEVERLY', 'MA', 'BEVERLY MUNICIPAL AIRPORT', 42.58417, -70.9175, 108, 108, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54734', 'DANBURY', 'CT', 'DANBURY MUNICIPAL AIRPORT', 41.37139, -73.48278, 457, 457, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54734');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54735', 'LATROBE', 'PA', 'ARNOLD PALMER REGIONAL APT', 40.28333, -79.4, 1199, 1185, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54735');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54736', 'LACONIA', 'NH', 'LACONIA MUNICIPAL AIRPORT', 43.56667, -71.43333, 545, 545, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54736');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54737', 'LANCASTER', 'PA', 'LANCASTER AIRPORT', 40.12028, -76.29444, 403, 403, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54738', 'MORRISTOWN', 'NJ', 'MORRISTOWN MUNICIPAL AIRPORT', 40.8, -74.41667, 187, 187, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54738');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54739', 'STATE COLLEGE', 'PA', 'UNIVERSITY PARK AIRPORT', 40.85, -77.85, 1239, 1239, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54739');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54740', 'SPRINGFIELD', 'VT', 'HARTNESS ST (SPNGFLD) ARPT', 43.34361, -72.51778, 578, 577, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54740');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54742', 'SAINT JOHNSBURY', 'VT', 'FAIRBANKS MUSEUM', 44.42, -72.01944, 697, 697, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54742');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54743', 'CALDWELL', 'NJ', 'ESSEX COUNTY AIRPORT', 40.87639, -74.28306, 173, 175, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54743');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54746', 'MONTICELLO', 'NY', 'SULLIVAN COUNTY INTL ARPT', 41.70139, -74.795, 1403, 1403, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54746');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54752', 'NORTH KINGSTOWN', 'RI', 'QUONSET STATE AIRPORT', 41.59713, -71.41213, 19, 18, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54752');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54754', 'NASHUA', 'NH', 'BOIRE FIELD AIRPORT', 42.78333, -71.51667, 200, 200, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54754');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54756', 'ORANGE', 'MA', 'ORANGE MUNICIPAL AIRPORT', 42.57, -72.29111, 555, 555, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54756');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54757', 'WELLSVILLE', 'NY', 'MUNI ARPT,TARANTINE FLD AP', 42.10944, -77.99194, 2124, 2113, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54757');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54758', 'NEWPORT', 'VT', 'NEWPORT AP', 44.88333, -72.23333, 915, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54758');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54760', 'BELMAR/FARMINGDALE', 'NJ', 'MONMOUTH EXECUTIVE AIRPORT', 40.18333, -74.13333, 159, 159, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54760');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54767', 'WILLIMANTIC', 'CT', 'WINDHAM AIRPORT', 41.74194, -72.18361, 247, 247, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54767');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54768', 'NORTH ADAMS', 'MA', 'HARRIMAN-AND-WEST AIRPORT', 42.7, -73.16667, 655, 654, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54768');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54769', 'PLYMOUTH', 'MA', 'PLYMOUTH MUNICIPAL AIRPORT', 41.90972, -70.72944, 149, 149, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54769');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54770', 'JAFFREY', 'NH', 'JAFFREY MINI-SLVR RNCH APT', 42.805, -72.00361, 1040, 1040, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54770');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54771', 'MORRISVILLE', 'VT', 'MORRISVILLE-STOWE ST ARPT', 44.53444, -72.61444, 732, 732, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54771');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54772', 'FRYEBURG', 'ME', 'EASTERN SLOPES RGNL ARPT', 43.99056, -70.9475, 445, 452, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54772');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54773', 'FULTON', 'NY', 'OSWEGO COUNTY AIRPORT', 43.34972, -76.38472, 475, 475, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54773');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54777', 'TAUNTON', 'MA', 'TAUNTON MUNICIPAL AIRPORT', 41.87556, -71.02111, 43, 43, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54777');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54778', 'PENN YAN', 'NY', 'PENN YAN AIRPORT', 42.6425, -77.05639, 903, 903, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54778');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54779', 'ANDOVER', 'NJ', 'AEROFLEX-ANDOVER AIRPORT', 41.00889, -74.73667, 583, 583, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54779');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54780', 'MONTAUK', 'NY', 'MONTAUK AIRPORT', 41.07306, -71.92333, 7, 7, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54780');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54781', 'BENNINGTON', 'VT', 'W. H. MORSE STATE AIRPORT', 42.89139, -73.24694, 826, 828, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54781');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54782', 'POTTSTOWN', 'PA', 'POTTSTOWN LIMERICK AIRPORT', 40.23833, -75.55722, 291, 309, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54782');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54785', 'SOMERVILLE', 'NJ', 'SOMERSET AIRPORT', 40.62389, -74.66944, 105, 100, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54785');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54786', 'DOYLESTOWN', 'PA', 'DOYLESTOWN AIRPORT', 40.33, -75.1225, 394, 385, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54786');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54787', 'FARMINGDALE', 'NY', 'REPUBLIC AIRPORT', 40.73417, -73.41694, 81, 81, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54787');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54788', 'MERIDEN', 'CT', 'MERIDEN MARKHAM MUNI ARPT', 41.50972, -72.82778, 103, 105, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54788');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54789', 'MOUNT POCONO', 'PA', 'POCONO MTNS MUNI AIRPORT', 41.13889, -75.37944, 1916, 1894, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54789');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54790', 'SHIRLEY', 'NY', 'BROOKHAVEN AIRPORT', 40.82167, -72.86889, 82, 82, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54790');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54791', 'ROCHESTER', 'NH', 'SKYHAVEN AIRPORT', 43.27806, -70.92222, 323, 340, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54791');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54792', 'CLEARFIELD', 'PA', 'CLEARFIELD-LAWRENCE ARPT', 41.04667, -78.41167, 1516, 1516, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54792');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54793', 'SUSSEX', 'NJ', 'SUSSEX AIRPORT', 41.20028, -74.62306, 421, 421, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54793');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54794', 'DURHAM', 'NH', 'DURHAM 2 N', 43.1716, -70.9277, 119, 119, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54794');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54795', 'DURHAM', 'NH', 'DURHAM 2 SSW', 43.1092, -70.9484, 63, 63, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54795');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54796', 'KINGSTON', 'RI', 'KINGSTON 1 NW', 41.4911, -71.5413, 115, 115, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54796');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54797', 'KINGSTON', 'RI', 'KINGSTON 1 W', 41.4782, -71.5417, 106, 106, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54797');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54807', 'KOKOMO', 'IN', 'KOKOMO MUNICIPAL AIRPORT', 40.52806, -86.05889, 830, 830, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54807');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54808', 'CHAMPAIGN', 'IL', 'CHAMPAIGN 9 SW', 40.0528, -88.3729, 700, 700, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54809', 'WARSAW', 'IN', 'WARSAW MUNICIPAL AIRPORT', 41.27472, -85.84, 850, 850, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54810', 'CHATHAM', 'MI', 'CHATHAM 1 SE', 46.3345, -86.92, 875, 875, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54811', 'SHABBONA', 'IL', 'SHABBONA 5 NNE', 41.843, -88.8513, 861, 861, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54811');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54813', 'MUNISING', 'MI', 'MUNISING LAKESHORE', 46.41667, -86.65, 612, 612, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54816', 'ALMA', 'MI', 'GRATIOT COMMUNITY AIRPORT', 43.32222, -84.68806, 747, 754, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54817', 'BAD AXE', 'MI', 'HURON COUNTY MEMORIAL AIRPORT', 43.78028, -82.98556, 766, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54818', 'FRANKFORT', 'MI', 'FRANKFORT DOW MEMORIAL FIELD AIRPORT', 44.62556, -86.20083, 632, 632, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54819', 'DETROIT/GROSSE ILE', 'MI', 'GROSSE ILE MUNICIPAL AIRPORT', 42.09861, -83.16111, 587, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54820', 'MACKINAC ISLAND', 'MI', 'MACKINAC ISLAND AIRPORT', 45.865, -84.63722, 740, 740, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54821', 'MARSHALL', 'MI', 'BROOKS FIELD AIRPORT', 42.25111, -84.95556, 941, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54822', 'MASON', 'MI', 'MASON JEWETT FIELD AIRPORT', 42.56583, -84.43306, 922, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54823', 'MONROE', 'MI', 'CUSTER AIRPORT', 41.94, -83.43472, 612, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54823');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54824', 'OWOSSO', 'MI', 'OWOSSO COMMUNITY AIRPORT', 42.99306, -84.13889, 736, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54825', 'ROGERS CITY', 'MI', 'PRESQUE ISLE COUNTY AIRPORT', 45.40722, -83.81306, 670, 670, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54826', 'SOUTH HAVEN', 'MI', 'SOUTH HAVEN AREA REGIONAL AIRPORT', 42.35083, -86.25611, 666, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54827', 'STURGIS', 'MI', 'KIRSCH MUNICIPAL AIRPORT', 41.81667, -85.43333, 925, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54828', 'CARO', 'MI', 'TUSCOLA AREA AIRPORT', 43.45889, -83.44556, 701, 701, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54828');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54829', 'TROY', 'MI', 'OAKLAND/TROY AIRPORT', 42.54306, -83.17806, 716, 729, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54829');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54831', 'BLOOMINGTON/NORMAL', 'IL', 'CENTRAL ILLINOIS REGIONAL AIRPORT', 40.48333, -88.95, 865, 871, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54831');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54832', 'CLINTONVILLE', 'WI', 'CLINTONVILLE MUNICIPAL AIRPORT', 44.61389, -88.73139, 822, 882, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54832');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54833', 'BARABOO', 'WI', 'BARABOO WISCONSIN DELLS AIRPORT', 43.52194, -89.77361, 976, 976, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54834', 'WATERTOWN', 'WI', 'WATERTOWN MUNICIPAL AIRPORT', 43.16667, -88.71667, 820, 833, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54834');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54838', 'EAGLE RIVER', 'WI', 'EAGLE RIVER UNION AIRPORT', 45.93167, -89.26889, 1642, 1642, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54844', 'DRUMMOND ISLAND', 'MI', 'DRUMMOND ISLAND AIRPORT', 46.00722, -83.74278, 665, 668, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54844');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54850', 'THREE RIVERS', 'MI', 'HREE RIVERS MUNICIPAL DR HAINES AIRPORT', 41.95972, -85.59333, 824, 824, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54851', 'COSHOCTON', 'OH', 'COSHOCTON 8 NNE', 40.3667, -81.7829, 1120, 1120, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54851');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54852', 'WAUPACA', 'WI', 'WAUPACA MUNICIPAL AIRPORT', 44.33333, -89.01972, 826, 826, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54854', 'GAYLORD', 'MI', 'GAYLORD 9 SSW', 44.908, -84.7203, 1461, 1461, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54854');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54901', 'BUFFALO', 'MN', 'BUFFALO MUNICIPAL AIRPORT', 45.15917, -93.84333, 967, 967, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54902', 'DES MOINES', 'IA', 'DES MOINES 17 E', 41.5562, -93.2855, 921, 921, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54903', 'NECEDAH', 'WI', 'NECEDAH 5 WNW', 44.0604, -90.1737, 933, 933, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54903');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54904', 'BENSON', 'MN', 'BENSON MUNICIPAL AIRPORT', 45.33194, -95.65056, 1039, 1039, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54904');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54905', 'COOK', 'MN', 'COOK MUNICIPAL AIRPORT', 47.82194, -92.68944, 1327, 1327, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54905');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54906', 'ST JAMES', 'MN', 'ST JAMES MUNICIPAL AIRPORT', 43.98611, -94.55778, 1067, 1067, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54906');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54907', 'MINERAL POINT', 'WI', 'IOWA COUNTY AIRPORT', 42.88667, -90.23611, 1171, 1171, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54907');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54908', 'SUPERIOR', 'WI', 'RICHARD I BONG AIRPORT', 46.68944, -92.09444, 674, 674, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54909', 'RICE LAKE', 'WI', 'RICE LAKE REGIONAL-CARL''S FIELD AIRPORT', 45.41861, -91.77333, 1105, 1105, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54910', 'STAPLES', 'MN', 'STAPLES MUNICIPAL AIRPORT', 46.38083, -94.80639, 1287, 1287, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54911', 'MEDFORD', 'WI', 'TAYLOR COUNTY AIRPORT', 45.10083, -90.30333, 1470, 1470, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54912', 'OSCEOLA', 'WI', 'L.O. SIMENSTAD MUNICIPAL AIRPORT', 45.30833, -92.69, 903, 903, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54912');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54913', 'PHILLIPS', 'WI', 'PRICE COUNTY AIRPORT', 45.70889, -90.40222, 1497, 1497, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54913');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54914', 'PRINCETON', 'MN', 'PRINCETON MUNICIPAL AIRPORT', 45.55972, -93.60806, 979, 979, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54914');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54915', 'PINE RIVER', 'MN', 'PINE RIVER REGIONAL AIRPORT', 46.72472, -94.38167, 1295, 1295, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54915');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54916', 'WASECA', 'MN', 'WASECA MUNICIPAL AIRPORT', 44.07361, -93.55306, 1126, 1126, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54916');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54917', 'APPLETON', 'MN', 'APPLETON MUNICIPAL AIRPORT', 45.2275, -96.00722, 1020, 1020, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54917');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54919', 'OSKALOOSA', 'IA', 'OSKALOOSA MUNICIPAL AIRPORT', 41.22611, -92.49083, 840, 840, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54920', 'CHEROKEE', 'IA', 'CHEROKEE MUNICIPAL AIRPORT/', 42.73167, -95.55583, 1226, 1226, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54920');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54921', 'ALBION', 'NE', 'ALBION MUNICIPAL AIRPORT', 41.73, -98.05444, 1799, 1806, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54921');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54922', 'WAHPETON', 'ND', 'HARRY STERN AIRPORT', 46.24417, -96.60722, 968, 968, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54922');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54923', 'CANBY', 'MN', 'MYERS FIELD AIRPORT', 44.72944, -96.26611, 1193, 1193, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54923');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54924', 'SLAYTON', 'MN', 'SLAYTON MUNICIPAL AIRPORT', 43.98667, -95.7825, 1623, 1623, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54924');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54925', 'WAYNE', 'NE', 'WAYNE MUNICIPAL AIRPORT', 42.24222, -96.98306, 1424, 1431, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54926', 'LUVERNE', 'MN', 'QUENTIN AANENSON FIELD AIRPORT', 43.62111, -96.21583, 1431, 1431, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54927', 'PAYNESVILLE', 'MN', 'PAYNESVILLE MUNICIPAL AP', 45.37222, -94.74611, 1180, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54928', 'LADYSMITH', 'WI', 'RUSK COUNTY AIRPORT', 45.49694, -91.00056, 1238, 1238, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54930', 'STANTON', 'MN', 'STANTON AIRFIELD', 44.47556, -93.01639, 920, 920, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54931', 'TRACY', 'MN', 'TRACY MUNICIPAL AIRPORT', 44.24917, -95.60722, 1340, 1340, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54932', 'SANDSTONE', 'MN', 'SANDSTONE 6 W', 46.1135, -92.9936, 1130, 1130, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54932');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54933', 'ABERDEEN', 'SD', 'ABERDEEN 35 WNW', 45.7115, -99.1296, 1957, 1957, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54937', 'JAMESTOWN', 'ND', 'JAMESTOWN 38 WSW', 46.7702, -99.4778, 1920, 1920, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54939', 'CENTERVILLE', 'IA', 'CENTERVILLE MUNICIPAL AIRPORT', 40.684, -92.901, 1023.1, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54939');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54940', 'FOREST CITY', 'IA', 'FOREST CITY MUNICIPAL AIRPORT', 43.23475, -93.6241, 1229, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54941', 'IOWA FALLS', 'IA', 'IOWA FALLS MUNICIPAL AIRPORT', 42.47138, -93.20707, 1137, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54941');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54942', 'OSCEOLA', 'IA', 'OSCEOLA MUNICIPAL AIRPORT', 41.05222, -93.68913, 1115, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54943', 'PERRY', 'IA', 'PERRY MUNICIPAL AIRPORT', 41.828, -94.15983, 1013, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '54953', 'TOMAHAWK', 'WI', 'TOMAHAWK REGIONAL AIRPORT', 45.46917, -89.80556, 1486, 1486, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='54953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '56401', 'GLENNALLEN', 'AK', 'GLENNALLEN 64 N', 63.0295, -145.5003, 2669, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='56401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '61705', 'PAGO PAGO', 'AS', 'TAFUNA/PAGO PAGO INTERNATIONAL AP', -14.33056, -170.71361, 12, 10, -11
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='61705');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63801', 'CONCORD', 'NC', 'CONCORD REGIONAL AIRPORT', 35.38222, -80.49056, 705, 705, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63801');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63802', 'WISE', 'VA', 'LONESOME PINE AIRPORT', 36.9875, -82.53, 2684, 2684, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63803', 'LAWRENCEVILLE', 'GA', 'GWINNETT CO-BRISCOE FD APT', 33.97972, -83.96333, 1061, 1061, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63804', 'HUNTSVILLE', 'AL', 'MADISON CO EXECUTIVE ARPT', 34.86139, -86.55722, 755, 755, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63804');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63805', 'MARION/WYTHEVILLE', 'VA', 'MOUNTIAN EMPIRE AIRPORT', 36.89472, -81.34972, 2559, 2559, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63805');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63806', 'MARTINSVILLE', 'VA', 'BLUE RIDGE AIRPORT', 36.63056, -80.01833, 941, 941, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63807', 'MOUNT AIRY', 'NC', 'MOUNT AIRY/SURRY CO ARPT', 36.45972, -80.55278, 1247, 1247, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63807');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63808', 'OLIVE BRANCH', 'MS', 'OLIVE BRANCH AIRPORT', 34.97861, -89.78667, 402, 402, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63809', 'THOMASTON', 'GA', 'THOMASTON-UPSON CO AIRPORT', 32.955, -84.26417, 796, 796, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63810', 'PARIS', 'IL', 'EDGAR COUNTY AIRPORT', 39.7, -87.66944, 654, 654, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63811', 'ANDREWS', 'NC', 'ANDREWS-MURPHY AIRPORT', 35.195, -83.86528, 1696, 1696, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63811');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63812', 'SALISBURY', 'NC', 'ROWAN COUNTY AIRPORT', 35.64583, -80.52028, 773, 773, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63812');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63813', 'ATLANTA', 'GA', 'COBB CO-MC COLLUM FLD ARPT', 34.01306, -84.59861, 1040, 1040, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63814', 'SPARTA', 'IL', 'SPARTA COMMUNITY-HUNTER FIELD AIRPORT', 38.14889, -89.69861, 538, 538, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63814');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63815', 'SOMERSET', 'KY', 'SOMERSET-PULASKI CO-J.T. WILSON FIELD AIRPORT', 37.05417, -84.61472, 927, 927, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63816', 'STATESVILLE', 'NC', 'STATESVILLE MUNICIPAL ARPT', 35.765, -80.95667, 965, 965, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63817', 'TAYLORVILLE', 'IL', 'TAYLORVILLE MINICIPAL ARPT', 39.53417, -89.32778, 622, 622, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63818', 'STATESBORO', 'GA', 'STATESBORO-BULLOCK CO ARPT', 32.48306, -81.73722, 187, 187, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63819', 'BOONE', 'NC', 'BOONE', 36.2, -81.65, 2987, 2987, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63820', 'UNION CITY', 'TN', 'EVERETT-STEWART AIRPORT', 36.38, -88.98528, 341, 341, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63821', 'NORTH WILKESBORO', 'NC', 'WILKES COUNTY AIRPORT', 36.22278, -81.09833, 1300, 1300, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63822', 'VIDALIA', 'GA', 'VIDALIA REGIONAL AIRPORT', 32.1925, -82.37194, 274, 274, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63823', 'JACKSONVILLE', 'FL', 'CECIL FIELD AIRPORT', 30.21861, -81.87639, 81, 81, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63823');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63824', 'ALBEMARLE', 'NC', 'STANLEY COUNTY AIRPORT', 35.41667, -80.15056, 609, 609, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63825', 'COLUMBUS', 'OH', 'BOLTON FIELD AIRPORT', 39.9, -83.13333, 905, 905, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63826', 'BLACKVILLE', 'SC', 'BLACKVILLE 3 W', 33.355, -81.3279, 317, 317, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63827', 'RICHLANDS', 'VA', 'TAZEWELL COUNTY AIRPORT', 37.06361, -81.79806, 2652, 2652, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63828', 'NEWTON', 'GA', 'NEWTON 8 W', 31.3127, -84.4706, 176, 176, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63828');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63829', 'NEWTON', 'GA', 'NEWTON 11 SW', 31.1923, -84.4465, 156, 156, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63829');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63831', 'NEWTON', 'MS', 'NEWTON 5 ENE', 32.3378, -89.0703, 374, 374, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63831');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63832', 'BUCKHANNON', 'WV', 'UPSHUR COUNTY RGNL AIRPORT', 39.00028, -80.27389, 1635, 1635, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63832');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63833', 'ALEXANDER CITY', 'AL', 'THOMAS C RUSSELL FLD ARPT', 32.91472, -85.96278, 686, 686, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63834', 'ELBERTON', 'GA', 'ELBERT CO-PATZ FLD AIRPORT', 34.095, -82.81583, 603, 603, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63834');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63835', 'BLAKELY', 'GA', 'EARLY COUNTY AIRPORT', 31.39694, -84.89528, 214, 214, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63835');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63836', 'THOMSON', 'GA', 'THOMSON-MCDUFFIE CO ARPT', 33.52972, -82.51639, 498, 498, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63836');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63837', 'HILTON HEAD ISLAND', 'SC', 'HILTON HEAD AIRPORT', 32.21667, -80.7, 24, 19, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63837');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63838', 'VERSAILLES', 'KY', 'VERSAILLES 3 NNW', 38.0945, -84.7465, 891, 891, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63838');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63839', 'CULLMAN', 'AL', 'FOLSOM FIELD AP', 34.26889, -86.85833, 963, 963, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63840', 'CARMI', 'IL', 'CARMI MINICIPAL AIRPORT', 38.08944, -88.12306, 388, 388, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63840');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63841', 'ROBINSON', 'IL', 'ROBINSON MUNICIPAL AIRPORT', 39.01611, -87.64972, 462, 462, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63842', 'DOUGLAS', 'GA', 'DOUGLAS MUNICIPAL AIRPORT', 31.47667, -82.86056, 257, 257, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63843', 'SHELBY', 'NC', 'SHELBY MUNICIPAL AIRPORT', 35.25556, -81.60111, 847, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63843');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63844', 'FRANKLIN', 'NC', 'MACON COUNTY AIRPORT', 35.2225, -83.41917, 2020, 2020, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63844');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63846', 'SYLVANIA', 'GA', 'PLANTATION AIRPARK', 32.64556, -81.59639, 188, 188, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63847', 'FORT PAYNE', 'AL', 'ISBELL FIELD AIRPORT', 34.47389, -85.72139, 877, 877, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63848', 'CANTON', 'GA', 'CHEROKEE COUNTY AIRPORT', 34.31056, -84.42389, 1219, 1219, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63848');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63849', 'BOWLING GREEN', 'KY', 'BOWLING GREEN 21 NNE', 37.2504, -86.2325, 790, 788, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63850', 'WATKINSVILLE', 'GA', 'WATKINSVILLE 5 SSE', 33.7837, -83.3896, 741, 741, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63851', 'METROPOLIS', 'IL', 'METROPOLIS MUNICIPAL AIRPORT', 37.18583, -88.75056, 384, 384, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63851');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63852', 'BAY ST LOUIS', 'MS', 'STENNIS INTERNATIONAL AIRPORT', 30.36778, -89.45472, 23, 23, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63853', 'MOUNT CARMEL', 'IL', 'MOUNT CARMEL MUNICIPAL AIRPORT', 38.60667, -87.72667, 529, 429, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63855', 'CROSSVILLE', 'TN', 'CROSSVILLE 7 NW', 36.0138, -85.1345, 1913, 1913, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63856', 'BRUNSWICK', 'GA', 'BRUNSWICK 23 S', 30.8078, -81.4596, 25, 25, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63856');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63857', 'GADSDEN', 'AL', 'GADSDEN 19 N', 34.2851, -85.9621, 1152, 1152, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63857');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63858', 'SELMA', 'AL', 'SELMA 13 WNW', 32.4567, -87.2422, 193, 193, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63858');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63859', 'MORGANTON', 'NC', 'MORGANTON-LENOIR AIRPORT', 35.82139, -81.61056, 1270, 1270, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63859');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63862', 'VALLEY HEAD', 'AL', 'VALLEY HEAD 1 SSW', 34.5653, -85.6171, 1020, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63862');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63866', 'GUNTERSVILLE', 'AL', 'GUNTERSVILLE 2 SW', 34.3337, -86.3156, 620, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63866');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63867', 'CULLMAN', 'AL', 'CULLMAN 3 ENE', 34.1954, -86.7963, 800, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63867');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63868', 'COURTLAND', 'AL', 'COURTLAND 2 WSW', 34.6602, -87.3462, 575, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63869', 'FAIRHOPE', 'AL', 'FAIRHOPE 3 NE', 30.5485, -87.8757, 95, 95, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63869');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63870', 'FLORALA', 'AL', 'FLORALA MUNICIPAL AIRPORT', 31.0425, -86.31167, 314, 314, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63871', 'BONIFAY', 'FL', 'TRI-COUNTY AIRPORT', 30.84583, -85.60139, 85, 85, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63872', 'EUFAULA', 'AL', 'WEEDON FIELD AIRPORT', 31.95139, -85.12889, 285, 285, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63872');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63873', 'FORT RUCKER/OZARK', 'AL', 'HANCHEY ARMY HELIPORT (FORT RUCKER)', 31.35, -85.66667, 317, 317, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63873');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63874', 'GREENVILLE', 'AL', 'MAC CRENSHAW MEMORIAL AIRPORT', 31.84556, -86.61083, 451, 451, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63874');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63875', 'MIDDLESBORO', 'KY', 'MIDDLESBORO-BELL COUNTY AIRPORT', 36.61056, -83.7375, 1154, 1154, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63875');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63876', 'POINT PLEASANT', 'WV', 'MASON COUNTY AIRPORT', 38.91472, -82.09861, 643, 643, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63876');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63877', 'GREENSBORO', 'GA', 'GREENE COUNTY REGIONAL AIRPORT', 33.59778, -83.13889, 677, 677, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63877');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63878', 'LITCHFIELD', 'IL', 'LITCHFIELD MUNICIPAL AIRPORT', 39.1625, -89.67472, 690, 690, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63878');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63879', 'SUTTON', 'WV', 'BRAXTON COUNTY AIRPORT', 38.68694, -80.65167, 1275, 1275, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63879');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63880', 'ALBERTVILLE', 'AL', 'THE ALBERTVILLE MUNI ARPT-THOMAS J BRUMLIK FLD', 34.22917, -86.25583, 1032, 1032, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63880');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63881', 'DUBLIN', 'GA', 'W H `BUD` BARRON AIRPORT', 32.56444, -82.98528, 309, 309, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63881');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63882', 'MONTICELLO', 'KY', 'WAYNE COUNTY AIRPORT', 36.85528, -84.85611, 963, 963, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63882');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63883', 'REDSTONE ARSENAL (HUNTSVILLE)', 'AL', 'REDSTONE ARMY AIRFIELD', 34.67861, -86.68472, 685, 685, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63883');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63884', 'PINEVILLE', 'WV', 'KEE FIELD AIRPORT', 37.60056, -81.55917, 1783, 1783, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63884');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63885', 'GULF SHORES', 'AL', 'JACK EDWARDS AIRPORT', 30.29056, -87.67167, 16, 16, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63885');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63886', 'MILLEDGEVILLE', 'GA', 'BALDWIN COUNTY AIRPORT', 33.15417, -83.24056, 384, 384, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63886');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63887', 'MIDDLETOWN', 'OH', 'HOOK FIELD MUNICIPAL AIRPORT', 39.53111, -84.39528, 650, 650, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63887');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63888', 'SPRINGFIELD', 'OH', 'SPRINGFIELD-BECKLEY MUNICIPAL AIRPORT', 39.84028, -83.84, 1051, 1051, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63888');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63889', 'GREENVILLE', 'SC', 'DONALDSON CENTER AIRPORT', 34.75833, -82.37639, 955, 955, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63890', 'JACKSONVILLE', 'FL', 'WHITEHOUSE NAVAL OUTLYING FIELD', 30.35028, -81.88306, 99, 99, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63890');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63891', 'CLANTON', 'AL', 'CLANTON 2 NE', 32.8516, -86.6115, 584, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63892', 'GAINESVILLE', 'AL', 'GAINESVILLE 2 NE', 32.8369, -88.1374, 107, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63892');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63893', 'GREENSBORO', 'AL', 'GREENSBORO 2 WNW', 32.7169, -87.6226, 280, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63894', 'MUSCLE SHOALS', 'AL', 'MUSCLE SHOALS 2 N', 34.7728, -87.6399, 530, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63895', 'RUSSELLVILLE', 'AL', 'RUSSELLVILLE 4 SSE', 34.4535, -87.7104, 720, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63896', 'SCOTTSBORO', 'AL', 'SCOTTSBORO 2 NE', 34.6941, -85.9998, 636, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63897', 'SELMA', 'AL', 'SELMA 6 SSE', 32.3349, -86.9787, 157, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63898', 'BEDFORD', 'IN', 'BEDFORD 5 WNW', 38.8882, -86.5707, 760, 760, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63899', 'BREWTON', 'AL', 'BREWTON 3 NNE', 31.1449, -87.0518, 170, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63899');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63901', 'ALTUS', 'OK', 'ALTUS/QUARTZ MOUNTAIN REGIONAL AIRPORT', 34.69861, -99.33806, 1433, 1433, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '63903', 'FORT POLK', 'LA', 'DIRT STRIP EAST OF FORT POLK', 31.1136, -92.9656, 374, 374, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='63903');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64705', 'BUTLER', 'PA', 'BTLR CO/K W SCHOLTER FD AP', 40.77694, -79.94972, 1247, 1247, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64705');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64706', 'INDIANA', 'PA', 'INDIANA COUNTY/JIMMY STEWART FIELD/AIRPORT', 40.63333, -79.1, 1405, 1405, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64706');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64707', 'OXFORD', 'CT', 'WATERBURY-OXFORD AIRPORT', 41.48333, -73.13333, 726, 726, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64707');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64708', 'PROVINCETOWN', 'MA', 'PROVINCETOWN MUNI AIRPORT', 42.07194, -70.22111, 8, 8, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64708');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64709', 'SANFORD', 'ME', 'SANFORD REGIONAL AIRPORT', 43.39361, -70.70778, 244, 244, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64709');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64710', 'PAWTUCKET', 'RI', 'NORTH CENTRAL STATE ARPT', 41.92056, -71.49111, 441, 441, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64710');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64752', 'PHILADELPHIA', 'PA', 'WINGS FIELD AIRPORT', 40.1, -75.26667, 302, 302, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64752');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64753', 'QUAKERTOWN', 'PA', 'QUAKERTOWN AIRPORT', 40.43528, -75.38194, 526, 526, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64753');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64756', 'MILLBROOK', 'NY', 'MILLBROOK 3 W', 41.7857, -73.7422, 413, 413, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64756');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64758', 'ITHACA', 'NY', 'ITHACA 13 E', 42.4401, -76.2462, 1228, 1228, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64758');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64761', 'EAST HAMPTON', 'NY', 'EAST HAMPTON AIRPORT', 40.95956, -72.25183, 56, 56, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64761');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64773', 'PLYMOUTH', 'NH', 'PLYMOUTH MUNICIPAL AIRPORT', 43.77917, -71.75361, 505, 505, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64773');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64774', 'MARSHFIELD', 'MA', 'MARSHFIELD MUNICIPAL AIRPORT - GEORGE HARLOW FIELD', 42.09833, -70.67222, 11, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64774');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64775', 'ROME', 'NY', 'GRIFFIS AIRFIELD', 43.23389, -75.41167, 519, 519, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64775');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '64776', 'PLATTSBURG', 'NY', 'PLATTSBURGH INTERNATIONAL AIRPORT', 44.65, -73.46667, 234, 234, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='64776');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '70701', 'DIEGO GARCIA', 'NULL', 'DIEGO GARCIA NAVAL AIR STATION', -7.3, 72.4, 14, 9, 6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='70701');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '73801', 'NORTHPORT', 'AL', 'NORTHPORT 2 S', 33.2125, -87.5914, 150, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='73801');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '73802', 'HIGHLAND HOME', 'AL', 'HIGHLAND HOME 2 S', 31.9155, -86.3115, 614, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='73802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '73803', 'TALLADEGA', 'AL', 'TALLADEGA 10 NNE', 33.5721, -86.0573, 525, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='73803');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '73805', 'PANAMA CITY', 'FL', 'NORTHWEST FLORIDA BEACHES INTL ARPT', 30.349, -85.788, 57, 69, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='73805');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '87601', 'MCMURDO SOUND', 'NULL', 'NSFA MCMURDO SOUND', -77.88333, 166.73333, 79, 110, 0
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='87601');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92762', 'SITE R', 'PA', 'SITE R', 39.734, -77.43, 907, 902, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92762');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92802', 'NEW PORT RITCHIE', 'FL', 'TAMPA BAY EXECUTIVE', 28.18778, -82.62583, 41, 40, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92802');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92805', 'POMPANO BEACH', 'FL', 'POMPANO BEACH AIRPARK ARPT', 26.25, -80.10833, 21, 21, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92805');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92806', 'ST PETERSBURG', 'FL', 'ALBERT WHITTED AIRPORT', 27.76472, -82.6275, 8, 8, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92808', 'NEW SMYRNA BEACH', 'FL', 'NEW SMYRNA BEACH MUNICIPAL AIRPORT', 29.05417, -80.94833, 10, 10, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92809', 'HOLLYWOOD', 'FL', 'NORTH PERRY AIRPORT', 25.99889, -80.24111, 9, 9, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92811', 'MIAMI BEACH', 'FL', 'LIFEGUARD OFFICE', 25.8063, -80.1334, 8, 65, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92811');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92813', 'ORLANDO', 'FL', 'KISSIMMEE GATEWAY AIRPORT', 28.28972, -81.43694, 82, 82, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92813');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92814', 'ST AUGUSTINE', 'FL', 'ST AUGUSTINE AIRPORT', 29.95917, -81.33972, 10, 10, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92814');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92815', 'STUART', 'FL', 'WITHAM FIELD AIRPORT', 27.18167, -80.22083, 18, 18, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92816', 'TAMPA', 'FL', 'VANDENBERG AIRPORT', 28.01389, -82.34528, 22, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92817', 'THE VILLAGES', 'FL', 'THE VILLAGES', 28.95, -81.96667, 89, 89, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92821', 'TITUSVILLE', 'FL', 'TITUSVILLE 7 E', 28.6159, -80.6929, 3, 3, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92822', 'ORMOND BEACH', 'FL', 'ORMOND BEACH MUNICIPAL AIRPORT', 29.29583, -81.1125, 29, 29, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92824', 'PLANT CITY', 'FL', 'PLANT CITY MUNICIPAL AIRPORT', 28.00028, -82.16417, 154, 154, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92825', 'TAMPA', 'FL', 'PETER O KNIGHT AIRPORT', 27.91556, -82.44917, 8, 8, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92825');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92826', 'EVERGLADES CITY', 'FL', 'EVERGLADES CITY 5 NE', 25.8996, -81.3183, 4, 4, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92826');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '92827', 'SEBRING', 'FL', 'SEBRING 23 SSE', 27.1526, -81.3689, 150, 150, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='92827');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93005', 'DURANGO', 'CO', 'DURA-LA PLATA CO AIRPORT', 37.14306, -107.76028, 6670, 6685, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93005');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93007', 'GUNNISON', 'CO', 'GUNSN-CRSTED BUTTE RGL APT', 38.53333, -106.93333, 7666, 7674, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93007');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93009', 'LEADVILLE', 'CO', 'LEADVILLE/LAKE COUNTY APT', 39.22917, -106.31694, 9932, 9927, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93009');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93010', 'LIMON', 'CO', 'LIMON MUNICIPAL AIRPORT', 39.18944, -103.71583, 5364, 5365, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93010');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93013', 'MONTROSE', 'CO', 'MONTROSE REGIONAL AIRPORT', 38.50583, -107.89889, 5720, 5759, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93013');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93026', 'DOUGLAS BISBEE', 'AZ', 'BISBEE DOUGLAS INTL ARPT', 31.4583, -109.6061, 4105, 4101, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93026');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93027', 'ST JOHNS', 'AZ', 'ST JOHNS INDUS AIRPK ARPT', 34.51833, -109.37917, 5733, 5733, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93027');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93033', 'CARLSBAD', 'NM', 'CAVERN CITY AIR TRML ARPT', 32.3335, -104.258, 3232, 3261, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93033');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93034', 'HOBBS', 'NM', 'LEA COUNTY REGIONAL AIRPORT', 32.6933, -103.2125, 3655, 3661, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93034');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93035', 'MARFA', 'TX', 'MARFA MUNICIPAL AIRPORT', 30.37139, -104.01667, 4849, 4849, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93035');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93037', 'COLORADO SPRINGS', 'CO', 'CITY OF COLORADO SPRINGS MUNICIPAL AP', 38.81, -104.6884, 6181, 6170, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93037');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93040', 'SOCORRO', 'NM', 'SOCORRO MUNICIPAL AP', 34.06667, -106.9, 4875, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93040');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93041', 'LAS CRUCES', 'NM', 'LAS CRUCES INTL AIRPORT', 32.28333, -106.91667, 4454, 4456, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93041');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93042', 'DALHART', 'TX', 'MUNICIPAL AIRPORT', 36.0167, -102.55, 3990, 3994, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93042');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93045', 'TRUTH OR CONSEQUENCES', 'NM', 'TRH OR CONSEQCES MUNI ARPT', 33.23667, -107.26806, 4850, 4858, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93045');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93046', 'CANADIAN', 'TX', 'HEMPHILL COUNTY AIRPORT', 35.9, -100.4, 2396, 2394, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93046');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93057', 'GRANTS', 'NM', 'GRANTS-MILAN MUNI AIRPORT', 35.16528, -107.90222, 6520, 6520, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93057');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93058', 'PUEBLO', 'CO', 'PUEBLO MEMORIAL AIRPORT', 38.2901, -104.4983, 4720, 4720, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93058');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93063', 'SILVER CITY', 'NM', 'GRANT COUNTY AIRPORT', 32.63333, -108.16667, 5373, 5446, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93063');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93065', 'COLORADO SPRINGS', 'CO', 'USAF ACADEMY AIRSTRIP AIRPORT', 38.96667, -104.81667, 6572, 6572, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93065');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93067', 'DENVER', 'CO', 'CENTENNIAL AIRPORT', 39.57028, -104.84889, 5883, 5828, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93067');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93069', 'CORTEZ', 'CO', 'CORTEZ MUNICIPAL AIRPORT', 37.30694, -108.62639, 5910, 5914, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93069');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93073', 'ASPEN', 'CO', 'ASPEN-PITKIN COUNTY ARPT', 39.23, -106.87056, 7720, 7742, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93073');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93075', 'MOAB', 'UT', 'CANYONLANDS FIELD AIRPORT', 38.75, -109.76278, 4560, 4565, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93075');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93076', 'ELKHART', 'KS', 'ELKHART-MORTON CO AIRPORT', 37, -101.88333, 3622, 3625, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93076');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93083', 'RUIDOSO', 'NM', 'SIERRA BLANCA RGNL AIRPORT', 33.45, -105.51667, 6810, 6814, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93083');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93084', 'SAFFORD', 'AZ', 'SAFFORD REGIONAL AIRPORT', 32.85472, -109.63528, 3176, 3158, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93084');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93091', 'LOS ALAMOS', 'NM', 'LOS ALAMOS AIRPORT', 35.88333, -106.28333, 7171, 7171, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93091');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93092', 'CHAMA', 'NM', 'NWS AWRS', 36.9, -106.58333, 7851, 7851, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93092');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93097', 'ALAMOGORDO', 'NM', 'ALAMOGORDO-WHITE SANDS RGL AIRPORT', 32.83972, -105.99056, 4200, 4200, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93097');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93101', 'SANTA ANA', 'CA', 'EL TORO MARINE CORPS AIR STATION', 33.66667, -117.73333, 384, 383, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93101');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93102', 'FALLON', 'NV', 'FALLON NAS', 39.41667, -118.71667, 3934, 3934, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93102');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93104', 'CHINA LAKE', 'CA', 'CHINA LAKE', 35.6875, -117.6931, 2230, 2283, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93104');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93107', 'MIRAMAR', 'CA', 'MARINE CORPS AIR STATION', 32.86667, -117.13333, 477, 478, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93107');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93110', 'OXNARD', 'CA', 'OXNARD AIRPORT', 34.20083, -119.20694, 36, 68, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93110');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93111', 'POINT MUGU', 'CA', 'POINT MUGU', 34.11667, -119.11667, 13, 12, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93111');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93112', 'SAN DIEGO', 'CA', 'NORTH ISLAND NAS', 32.7, -117.2, 26, 26, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93112');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93114', 'TUSTIN', 'CA', 'MARINE CORPS AIR STATION', 33.7, -117.83333, 56, 54, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93114');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93115', 'IMPERIAL BEACH', 'CA', 'NAVAL AUXILIARY LANDING FD', 32.56667, -117.11667, 24, 23, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93115');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93116', 'SAN NICOLAS ISLAND', 'CA', 'SAN NICOLAS ISLAND NAVAL OUTLYING FIELD', 33.23983, -119.45816, 502, 504, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93116');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93117', 'SAN CLEMENTE ISLAND', 'CA', 'NALF/F. SHERMAN FLD ARPT', 33.02333, -118.58806, 182, 182, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93117');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93121', 'TWENTY NINE PALMS', 'CA', 'TWENTY NINE PALMS', 34.3, -116.16667, 2051, 2283, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93121');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93129', 'CEDAR CITY', 'UT', 'CEDAR CITY REGIONAL ARPT', 37.7086, -113.0944, 5586, 5618, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93132', 'GREEN RIVER', 'UT', 'GREEN RIVER', 39, -110.15, 4072, 4120, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93132');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93134', 'LOS ANGELES', 'CA', 'DOWNTOWN L.A./USC CAMPUS', 34.0236, -118.2911, 179, 185, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93134');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93136', 'MOUNT WILSON', 'CA', 'MOUNT WILSON', 34.23333, -118.06667, 5706, 5705, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93136');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93138', 'PALM SPRINGS', 'CA', 'PALM SPRINGS INTL AIRPORT', 33.82806, -116.50528, 421, 450, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93138');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93141', 'PRICE', 'UT', 'CARBON COUNTY AIRPORT', 39.60917, -110.75472, 5921, 5856, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93141');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93144', 'VISALIA', 'CA', 'VISALIA MUNICIPAL AIRPORT', 36.31667, -119.4, 295, 295, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93144');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93167', 'KINGMAN', 'AZ', 'KINGMAN AIRPORT', 35.2577, -113.933, 3420, 3446, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93167');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93184', 'SANTA ANA', 'CA', 'J. WAYNE APT-ORANGE CO APT', 33.68, -117.86639, 54, 55, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93184');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93193', 'FRESNO', 'CA', 'FRESNO YOSEMITE INTERNATIONAL AIRPORT', 36.78, -119.7194, 333, 327, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93193');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93194', 'INYOKERN', 'CA', 'INYOKERN AIRPORT', 35.66667, -117.83333, 2457, 2455, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93194');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93197', 'SANTA MONICA', 'CA', 'SANTA MONICA MUNI AIRPORT', 34.01583, -118.45139, 174, 175, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93197');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93201', 'TRUCKEE', 'CA', 'TRUCKEE-TAHOE AIRPORT', 39.32, -120.13944, 5900, 5900, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93201');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93203', 'CHICO', 'CA', 'CHICO MUNICIPAL AIRPORT', 39.8, -121.85, 272, 238, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93203');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93205', 'MARYSVILLE', 'CA', 'YUBA COUNTRY AIRPORT', 39.1019, -121.5677, 62, 62, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93205');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93206', 'SAN LUIS OBISPO', 'CA', 'SAN LUIS CO REGIONAL ARPT', 35.23722, -120.64139, 200, 207, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93206');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93209', 'PASO ROBLES', 'CA', 'PASO ROBLES MUNICIPAL ARPT', 35.6697, -120.6283, 810, 817, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93209');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93210', 'OROVILLE', 'CA', 'OROVILLE MUNICIPAL AIRPORT', 39.49, -121.61833, 190, 192, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93210');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93214', 'LOMPOC', 'CA', 'VANDENBERG AFB', 34.71667, -120.56667, 369, 367, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93214');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93216', 'MARYSVILLE', 'CA', 'BEALE AIR FORCE BASE', 39.13333, -121.43333, 113, 113, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93216');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93225', 'SACRAMENTO', 'CA', 'SACRAMENTO INTL AIRPORT', 38.69556, -121.58972, 23, 33, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93225');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93226', 'SAN SIMEON', 'CA', 'PT POEDRAS BLANCA (SITE TERMINATED 1815Z, 6/8/05)', 35.66556, -121.28472, 59, 69, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93226');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93227', 'NAPA', 'CA', 'NAPA COUNTY AIRPORT', 38.2102, -122.2847, 14, 56, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93227');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93228', 'HAYWARD', 'CA', 'HAYWARD EXECUTIVE AIRPORT', 37.6542, -122.115, 43, 47, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93228');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93230', 'SOUTH LAKE TAHOE', 'CA', 'LAKE TAHOE AIRPORT', 38.8983, -119.9947, 6314, 6273, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93230');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93231', 'SAN CARLOS', 'CA', 'SAN CARLOS AIRPORT', 37.51667, -122.25, 5, 5, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93231');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93232', 'SAN JOSE', 'CA', 'REID-HILLVIEW AIRPORT OF SANTA CLARA COUNTY', 37.33333, -121.81667, 133, 133, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93232');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93241', 'VACAVILLE', 'CA', 'NUT TREE AIRPORT', 38.3775, -121.9575, 109, 114, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93241');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93242', 'MADERA', 'CA', 'MADERA MUNICIPAL AIRPORT', 36.98778, -120.11056, 253, 253, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93242');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93243', 'MERCED', 'CA', 'MERCED 23 WSW', 37.2381, -120.8825, 78, 78, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93243');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93244', 'SANTA YNEZ', 'CA', 'SANTA YNEZ AIRPORT', 34.60694, -120.07556, 671, 671, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93244');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93245', 'BODEGA', 'CA', 'BODEGA 6 WSW', 38.3208, -123.0747, 63, 63, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93245');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93706', 'HAGERSTOWN', 'MD', 'HGRTWN RGNL-R.HNSON FLD AP', 39.70778, -77.72972, 698, 704, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93706');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93714', 'PETERSBURG', 'VA', 'DINWIDDIE COUNTY AIRPORT', 37.18333, -77.5, 193, 953, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93714');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93718', 'NORTH MYRTLE BEACH', 'SC', 'GRAND STRAND AIRPORT', 33.81167, -78.72389, 32, 33, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93718');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93719', 'NEW BERN', 'NC', 'CRAVEN COUNTY REG AIRPORT', 35.0677, -77.048, 19, 24, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93719');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93720', 'SALISBURY', 'MD', 'SALBRY-OCN CTY WICO RGNL AP', 38.34056, -75.51028, 48, 60, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93720');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93721', 'BALTIMORE', 'MD', 'BALTIMORE-WASHINGTON INTL AIRPORT', 39.1666, -76.6833, 156, 155, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93721');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93726', 'KINSTON', 'NC', 'KINSTON REGIONAL JETPORT AT STALLING FIELD', 35.31667, -77.63333, 94, 94, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93726');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93727', 'NEW RIVER', 'NC', 'JACKSONVILLE', 34.70842, -77.43966, 26, 25, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93727');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93728', 'FORT BELVOIR', 'VA', 'DAVISON AAF AIRPORT', 38.71667, -77.18333, 73, 74, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93728');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93729', 'CAPE HATTERAS', 'NC', 'BILLY MITCHELL AIRPORT', 35.2326, -75.6219, 11, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93729');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93730', 'ATLANTIC CITY', 'NJ', 'ATLANTIC CITY INTERNATIONAL AIRPORT', 39.4494, -74.5672, 60, 67, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93730');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93733', 'FORT MEADE', 'MD', 'TIPTON AIRPORT', 39.08333, -76.76667, 151, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93735', 'FORT EUSTIS', 'VA', 'FELKER ARMY AIRFIELD', 37.13333, -76.6, 12, 12, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93735');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93736', 'CHARLOTTESVILLE', 'VA', 'CHARLOTTES-ALBEMARLE ARPT', 38.13861, -78.45306, 641, 644, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93736');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93737', 'FORT BRAGG', 'NC', 'SIMMONS AAF AIRPORT', 35.13333, -78.93333, 244, 242, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93738', 'WASHINGTON', 'VA', 'WASHINGTON DULLES INTERNATIONAL AP', 38.9408, -77.4636, 290, 323, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93738');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93739', 'WALLOPS ISLAND', 'VA', 'WALLOPS FLIGHT FACILITY AIRPORT', 37.9372, -75.4708, 46, 48, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93739');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93740', 'FAYETTEVILLE', 'NC', 'FAYETT RGNL/GRANNIS FLD AP', 34.99139, -78.88028, 186, 196, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93740');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93741', 'NEWPORT NEWS', 'VA', 'NWPT NEWS/WIMBURG INTL APT', 37.13194, -76.49306, 42, 51, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93741');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93743', 'SWANSBORO', 'NC', 'BOGUE FIELD MARINE CORPS AUXILIARY FIELD', 34.68333, -77.03333, 21, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93743');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93744', 'BALTIMORE', 'MD', 'MARTIN STATE AIRPORT', 39.33333, -76.41667, 21, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93744');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93747', 'CAMP MACKALL', 'NC', 'MACKALL AAF AIRPORT', 35.03333, -79.5, 376, 376, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93747');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93753', 'JACKSONVILLE', 'NC', 'ALBERT J ELLIS AIRPORT', 34.83333, -77.61667, 96, 94, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93753');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93757', 'HOT SPRINGS', 'VA', 'INGALLS FIELD AIRPORT', 37.95, -79.81667, 3793, 3792, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93757');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93759', 'ROCKY MOUNT', 'NC', 'ROCKY MOUNT-WILSON RGN APT', 35.855, -77.89306, 160, 158, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93759');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93760', 'STAUNTON/WAYNESBRO/HARRISONBG', 'VA', 'SHENANDOAH VALLEY RGNL ART', 38.26361, -78.89639, 1201, 1201, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93760');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93762', 'SITE R', 'PA', 'SITE R', 39.73333, -77.43333, 818, 902, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93762');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93764', 'GAITHERSBURG', 'MD', 'GAITHERSBURG MONTGOMERY COUNTY AIR PARK', 39.16667, -77.16667, 539, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93764');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93765', 'BEAUFORT', 'NC', 'MICHAEL J SMITH FLD ARPT', 34.73361, -76.66056, 11, 11, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93765');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93773', 'WAKEFIELD', 'VA', 'WAKEFIELD MUNICIPAL ARPT', 36.98389, -77.00722, 109, 108, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93773');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93775', 'RICHMOND/ASHLAND', 'VA', 'HANOVER CO MUNICIPAL ARPT', 37.70806, -77.43444, 205, 205, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93775');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93776', 'CHERRY POINT', 'NC', 'CHERRY POINT/MCAS ASOS 2', 34.88333, -76.86667, 30, 30, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93776');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93778', 'YORK', 'PA', 'YORK AIRPORT', 39.91806, -76.87417, 486, 480, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93778');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93780', 'MOUNT HOLLY', 'NJ', 'SOUTH JERSEY REGIONAL ARPT', 39.94917, -74.84167, 53, 53, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93780');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93781', 'ROANOKE RAPIDS (CLOSED JUL 2009)', 'NC', 'HALIFAX COUNTY AIRPORT', 36.43944, -77.70972, 256, 255, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93781');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93782', 'MAXTON', 'NC', 'LAURINBURG-MAXTON AIRPORT', 34.79167, -79.36611, 220, 220, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93782');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93783', 'BURLINGTON', 'NC', 'BURLINGTON ALAMANCE RGL AP', 36.04667, -79.47694, 617, 616, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93783');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93784', 'BALTIMORE', 'MD', 'BALTIMORE DOWNTOWN', 39.2814, -76.6089, 20, 19, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93784');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93785', 'CHAPEL HILL', 'NC', 'HORACE WILLIAMS AIRPORT', 35.93333, -79.06417, 512, 512, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93785');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93786', 'OCEAN CITY', 'MD', 'OCEAN CITY MUNICIPAL ARTP', 38.30833, -75.12389, 12, 12, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93786');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93795', 'CONNELLSVILLE', 'PA', 'CONNELLSVILLE AIRPORT', 39.95889, -79.65694, 1267, 1267, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93795');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93796', 'AHOSKIE', 'NC', 'TRI-COUNTY AIRPORT', 36.2975, -77.17083, 68, 68, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93796');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93797', 'SOUTH HILL', 'VA', 'MCKNBRG-BRUNWICK RGNL ARPT', 36.68806, -78.05444, 442, 442, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93797');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93798', 'CULPEPER', 'VA', 'CULPEPER REGIONAL AIRPORT', 38.52667, -77.85861, 316, 313, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93798');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93799', 'WHITEVILLE', 'NC', 'COLUMBUS CO MUNICIPAL ARPT', 34.27278, -78.71472, 98, 98, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93799');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93801', 'ROME', 'GA', 'RICHARD B RUSSELL AIRPORT', 34.34778, -85.16111, 639, 644, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93801');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93804', 'SPARTANBURG', 'SC', 'SPARTANBURG DOWNTOWN MEMORIAL AIRPORT', 34.91667, -81.95, 801, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93804');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93805', 'TALLAHASSEE', 'FL', 'TALLAHASSEE REGIONAL AIRPORT', 30.39306, -84.35333, 55, 68, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93805');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93806', 'TUSCALOOSA', 'AL', 'TUSCALOOSA MUNICIPAL ARPT', 33.2119, -87.6161, 150, 186, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93806');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93807', 'WINSTON SALEM', 'NC', 'SMITH REYNOLDS AIRPORT', 36.13361, -80.22222, 970, 970, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93807');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93808', 'BOWLING GREEN', 'KY', 'BOW GRN-WARREN CO. RGNL AP', 36.9647, -86.4238, 528, 537, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93808');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93809', 'CAIRO', 'IL', 'CAIRO REGIONAL AIRPORT', 37.06444, -89.21944, 321, 321, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93809');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93810', 'CARBONDALE/MURPHYBORO', 'IL', 'SOUTHERN ILLINOIS AIRPORT', 37.77972, -89.24972, 406, 416, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93810');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93812', 'CINCINNATI', 'OH', 'CINA MUNI APT/LUKN FD APT', 39.10333, -84.41889, 490, 497, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93812');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93814', 'COVINGTON', 'KY', 'CINCINNATI/NORTHERN KENTUCKY INTL AP', 39.04306, -84.67167, 869, 883, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93814');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93815', 'DAYTON', 'OH', 'J.M.COX DAYTON INTERNATIONAL AIRPORT', 39.90611, -84.21861, 1000, 1003, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93816', 'EFFINGHAM', 'IL', 'EFFINGHAM COUNTY MEMORIAL ARPT', 39.07028, -88.53333, 587, 587, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93816');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93817', 'EVANSVILLE', 'IN', 'EVANSVILLE REGIONAL AIRPORT', 38.0441, -87.5205, 400, 388, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93819', 'INDIANAPOLIS', 'IN', 'INDIANAPOLIS INTERNATIONAL AIRPORT', 39.7318, -86.2788, 790, 808, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93819');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93820', 'LEXINGTON', 'KY', 'BLUE GRASS AIRPORT', 38.0408, -84.6058, 980, 989, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93820');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93821', 'LOUISVILLE', 'KY', 'LOUISVILLE INTL-STANDIFORD FIELD AP', 38.1811, -85.7391, 488, 488, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93821');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93822', 'SPRINGFIELD', 'IL', 'ABRAHAM LINCOLN CAPITAL AIRPORT', 39.8447, -89.6839, 594, 613, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93824', 'ZANESVILLE', 'OH', 'ZANESVILLE MUNICIPAL ARPT', 39.94444, -81.89222, 880, 902, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93824');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93831', 'BEAUFORT', 'SC', 'BEAUFORT MCAS', 32.48333, -80.71667, 37, 38, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93831');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93832', 'CECIL FIELD', 'FL', 'CECIL FIELD NAS', 30.21667, -81.88333, 83, 80, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93832');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93837', 'JACKSONVILLE', 'FL', 'JACKSONVILLE NAS', 30.23333, -81.66667, 20, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93837');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93839', 'MILLINGTON', 'TN', 'MILLINGTON MUNICIPAL AIRPORT', 35.35, -89.86667, 320, 320, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93839');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93841', 'MILTON', 'FL', 'WHITING FIELD NAVAL AIR STATION NORTH', 30.71667, -87.01667, 199, 200, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93841');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93842', 'COLUMBUS', 'GA', 'COLUMBUS METRO AIRPORT', 32.5161, -84.9422, 392, 394, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93842');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93845', 'VALDOSTA', 'GA', 'VALDOSTA REGIONAL AIRPORT', 30.7825, -83.27667, 198, 198, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93845');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93846', 'ANDERSON', 'SC', 'ANDERSON REGIONAL AIRPORT', 34.4977, -82.7097, 760, 773, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93862', 'TUPELO', 'MS', 'TUPELO REGIONAL AIRPORT', 34.2622, -88.7713, 361, 361, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93862');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93874', 'GULFPORT', 'MS', 'GULFPORT-BILOXI INTL ARPT', 30.4119, -89.0808, 42, 28, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93874');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93894', 'MOUNT VERNON', 'IL', 'MOUNT VERNON AIRPORT', 38.32333, -88.85833, 480, 480, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93901', 'DALLAS', 'TX', 'NAVAL AIR STATION OPERATIONS', 32.73333, -96.96667, 492, 495, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93901');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93909', 'OLATHE', 'KS', 'NEW CENTURY AIRCENTER ARPT', 38.83167, -94.88972, 1087, 1087, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93909');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93911', 'SHAWNEE', 'OK', 'SHAWNEE MUNICIPAL AIRPORT', 35.35667, -96.94333, 1073, 1073, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93915', 'ALEXANDRIA', 'LA', 'ALEXANDRIA INTL AIRPORT', 31.33472, -92.55861, 84, 80, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93915');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93919', 'MCCOMB', 'MS', 'MCOM/PIKE CO/J E LWS FD AP', 31.1827, -90.4708, 413, 422, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93929', 'GAINESVILLE', 'TX', 'GAINESVILLE MUNICIPAL ARPT', 33.65139, -97.19694, 839, 839, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93937', 'SULPHUR SPRINGS', 'TX', 'SULPHUR SPRINGS MUNICIPAL AIRPORT', 33.16667, -95.61667, 489, 489, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93937');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93940', 'ARDMORE', 'OK', 'ARDMORE MUNICIPAL AIRPORT', 34.3, -97.01667, 725, 761, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93942', 'BRADY', 'TX', 'CURTIS FIELD AIRPORT', 31.17833, -99.32444, 1827, 1827, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93942');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93943', 'BROWNWOOD', 'TX', 'BROWNWOOD REGIONAL AIRPORT', 31.8, -98.95, 1377, 1387, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93947', 'FREDERICKSBURG', 'TX', 'GILLESPIE COUNTY AIRPORT', 30.24333, -98.90972, 1695, 1695, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93950', 'MC ALESTER', 'OK', 'MC ALESTER REGIONAL ARPT', 34.8822, -95.783, 770, 770, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93953', 'MUSKOGEE', 'OK', 'DAVIS FIELD AIRPORT', 35.65667, -95.36139, 610, 610, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93953');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93955', 'PARIS', 'TX', 'COX FIELD AIRPORT', 33.63333, -95.45, 547, 547, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93955');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93967', 'COFFEYVILLE', 'KS', 'COFFEYVILLE MUNICIPAL ARPT', 37.09111, -95.56639, 752, 752, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93978', 'PRATT', 'KS', 'PRATT INDUSTRIAL AIRPORT', 37.70167, -98.74694, 1952, 1952, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93983', 'PALESTINE', 'TX', 'PALESTINE MUNICIPAL ARPT', 31.77972, -95.70639, 423, 423, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93983');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93984', 'TEMPLE', 'TX', 'DRAUGHON-MILLER CNTRL TX RGNL ARPT', 31.15, -97.41667, 682, 682, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93984');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93985', 'MINERAL WELLS', 'TX', 'MINERAL WELLS AIRPORT', 32.7816, -98.0602, 930, 972, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93986', 'HOBART', 'OK', 'HOBART MUNICIPAL AIRPORT', 34.9894, -99.0525, 1556, 1570, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93986');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93987', 'LUFKIN', 'TX', 'ANGELINA COUNTY AIRPORT', 31.23611, -94.75444, 288, 316, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93987');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93988', 'PINE BLUFF', 'AR', 'GRIDER FIELD AIRPORT', 34.175, -91.93472, 206, 214, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93988');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93989', 'QUINCY', 'IL', 'QUINCY RGNL-BLDWN FLD ARPT', 39.93694, -91.19194, 769, 766, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93990', 'HILL CITY', 'KS', 'HILL CITY MUNICIPAL ARPT', 39.37556, -99.82972, 2188, 2221, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93991', 'WALNUT RIDGE', 'AR', 'WALNUT RIDGE REGIONAL ARPT', 36.12528, -90.92444, 273, 273, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93992', 'EL DORADO', 'AR', 'S. AR RGNL AT GDWIN FLD AP', 33.22083, -92.81417, 252, 285, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93993', 'FAYETTEVILLE', 'AR', 'DRAKE FIELD AIRPORT', 36.0097, -94.1694, 1251, 1259, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93996', 'FARMINGTON', 'MO', 'FARMINGTON REGIONAL ARPT', 37.76083, -90.42833, 947, 947, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93997', 'RUSSELL', 'KS', 'RUSSELL MUNICIPAL AIRPORT', 38.87611, -98.80917, 1864, 1863, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '93999', 'VICKSBURG', 'MS', 'VICKSBURG MUNICIPAL AIRPORT', 32.23333, -90.93333, 103, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='93999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94008', 'GLASGOW', 'MT', 'WOKAL FIELD/GLASGOW INTL AIRPORT', 48.2138, -106.6214, 2285, 2294, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94008');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94011', 'MINOT', 'ND', 'MINOT AFB AIRPORT', 48.41667, -101.35, 1667, 1668, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94011');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94012', 'HAVRE', 'MT', 'HAVRE CITY-COUNTY AIRPORT', 48.5428, -109.7633, 2585, 2599, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94012');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94014', 'WILLISTON', 'ND', 'SLOULIN FIELD INTERNATIONAL AIRPORT', 48.1738, -103.6366, 1902, 1905, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94014');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94015', 'FORT CARSON', 'CO', 'BUTTS AAF AIRPORT', 38.67833, -104.75667, 5838, 5838, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94015');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94017', 'WOLF POINT', 'MT', 'L M CLAYTON AIRPORT', 48.09444, -105.57444, 1986, 1986, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94017');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94023', 'GILLETTE', 'WY', 'GILLETTE-CAMPBELL CO. ARPT', 44.33944, -105.54194, 4354, 4340, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94023');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94025', 'HAYDEN', 'CO', 'YAMPA VALLEY AIRPORT', 40.48111, -107.2175, 6600, 6602, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94025');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94028', 'SIDNEY', 'MT', 'SIDNEY-RICHLAND MUNI ARPT', 47.71667, -104.18333, 1980, 1984, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94028');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94030', 'VERNAL', 'UT', 'VERNAL AIRPORT', 40.44278, -109.51278, 5270, 5274, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94030');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94032', 'CUSTER', 'SD', 'CUSTER COUNTY AIRPORT', 43.73306, -103.61139, 5545, 5565, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94032');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94033', 'STEAMBOAT SPRINGS', 'CO', 'STEAMBOAT SPRINGS/BOB ADAMS FIELD AIRPORT', 40.51667, -106.86667, 6879, 6878, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94033');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94035', 'LOVELAND', 'CO', 'FORT COLLINS LOVELAND AP', 40.45, -105.01667, 5015, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94035');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94037', 'BUFFALO', 'SD', 'BUFFALO', 45.60444, -103.54639, 3004, 3003, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94037');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94038', 'HETTINGER', 'ND', 'HETTINGER MUNICIPAL ARPT', 46.01389, -102.65472, 2705, 2705, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94038');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94039', 'PINE RIDGE', 'SD', 'PINE RIDGE AIRPORT', 43.02056, -102.51833, 3331, 3331, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94039');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94040', 'MC COOK', 'NE', 'MC COOK MUNICIPAL AIRPORT', 40.20639, -100.59139, 2530, 2579, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94040');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94041', 'GARRISON', 'ND', 'GARRISON', 47.64583, -101.43944, 1910, 1912, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94041');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94050', 'MEEKER', 'CO', 'MEEKER AIRPORT', 40.04417, -107.88861, 6366, 6390, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94050');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94051', 'JORDAN', 'MT', 'JORDAN AIRPORT', 47.32583, -106.9475, 2662, 2630, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94051');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94052', 'MOBRIDGE', 'SD', 'MOBRIDGE MUNICIPAL AIRPORT', 45.54639, -100.40778, 1715, 1664, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94052');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94053', 'TORRINGTON', 'WY', 'TORRINGTON MUNICIPAL ARPT', 42.0613, -104.1584, 4199, 4205, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94053');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94054', 'BUFFALO', 'WY', 'JOHNSON COUNTY AIRPORT', 44.38139, -106.72111, 4967, 4967, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94054');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94055', 'BAKER', 'MT', 'BAKER MUNICIPAL AIRPORT', 46.3583, -104.25, 2971, 2971, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94055');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94056', 'FAITH', 'SD', 'FAITH MUNICIPAL AIRPORT', 45.03194, -102.01917, 2580, 2574, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94056');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94057', 'DOUGLAS', 'WY', 'CONVERSE COUNTY AIRPORT', 42.79611, -105.38028, 4936, 4929, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94057');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94059', 'WOLF POINT', 'MT', 'WOLF POINT 34 NE', 48.4887, -105.2096, 2643, 2643, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94059');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94060', 'WOLF POINT', 'MT', 'WOLF POINT 29 ENE', 48.3082, -105.1018, 2085, 2085, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94060');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94061', 'BENKELMAN', 'NE', 'JONES AIRPORT', 40.05806, -101.54694, 3126, 3126, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94061');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94063', 'OGALLALA', 'NE', 'SEARLE FIELD AIRPORT', 41.11917, -101.76806, 3250, 3278, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94063');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94070', 'TRENTON', 'NE', 'TRENTON MUNICIPAL AIRPORT', 40.18722, -101.02528, 2796, 2796, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94070');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94073', 'KIMBALL', 'NE', 'KMBAL MUNI/R E ARRAJ FD AP', 41.18944, -103.67083, 4894, 4926, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94073');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94074', 'NUNN', 'CO', 'NUNN 7 NNE', 40.8066, -104.7552, 5390, 5390, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94074');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94075', 'BOULDER', 'CO', 'BOULDER 14 W', 40.0354, -105.5409, 9828, 9828, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94075');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94076', 'KREMMLING', 'CO', 'MC ELORY AIRFIELD AIRPORT', 40.05361, -106.36889, 7411, 7411, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94076');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94077', 'HARRISON', 'NE', 'HARRISON 20 SSE', 42.4247, -103.7363, 4406, 4406, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94077');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94078', 'LANDER', 'WY', 'LANDER 11 SSE', 42.6754, -108.6686, 5773, 5773, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94078');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94079', 'WHITMAN', 'NE', 'WHITMAN 5 ENE', 42.068, -101.445, 3740, 3740, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94079');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94080', 'MEDORA', 'ND', 'MEDORA 7 E', 46.8946, -103.3769, 2771, 2771, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94080');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94081', 'BUFFALO', 'SD', 'BUFFALO 13 ESE', 45.516, -103.3017, 2883, 2883, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94081');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94082', 'DINOSAUR', 'CO', 'DINOSAUR 2 E', 40.2446, -108.9677, 6062, 6062, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94082');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94084', 'NORTHGATE', 'ND', 'NORTHGATE 5 ESE', 48.9676, -102.1702, 1842, 1842, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94084');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94085', 'PIERRE', 'SD', 'PIERRE 24 S', 44.0194, -100.353, 2124, 2124, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94085');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94086', 'PINEDALE', 'WY', 'RALPH WENZ FIELD AIRPORT', 42.79556, -109.80694, 7086, 7086, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94086');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94087', 'THEDFORD', 'NE', 'THOMAS COUNTY AIRPORT', 41.96444, -100.56861, 2914, 2926, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94087');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94088', 'SUNDANCE', 'WY', 'SUNDANCE 8 NNW', 44.5169, -104.4363, 5792, 5792, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94088');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94092', 'AKRON A', 'CO', 'AKRON A 4 E', 40.1551, -103.1417, 4542, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94092');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94094', 'MEEKER', 'CO', 'MEEKER 15 W', 40.0822, -108.1996, 5761, NULL, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94094');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94103', 'WALLA WALLA', 'WA', 'POST 0FFICE BLDG.2ND&SUMACH', 46.03333, -118.33333, 949, 991, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94103');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94107', 'ROME', 'OR', 'ROME STATE AIRPORT', 42.59056, -117.86444, 4050, 4050, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94107');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94119', 'DEER PARK', 'WA', 'DEER PARK AIRPORT', 47.97417, -117.42833, 2191, 2206, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94119');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94128', 'LOGAN', 'UT', 'LOGAN-CACHE AIRPORT', 41.78722, -111.85333, 4454, 4454, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94128');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94129', 'PULLMAN/MOSCOW', 'WA', 'PULLMAN/MOSCOW RGNL ARPT', 46.74389, -117.10861, 2551, 2551, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94129');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94161', 'HAILEY', 'ID', 'FRIEDMAN MEMORIAL AIRPORT', 43.5, -114.3, 5306, 5320, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94161');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94163', 'WEST YELLOWSTONE', 'MT', 'YELLOWSTONE AIRPORT', 44.68333, -111.11667, 6649, 6644, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94163');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94173', 'YELLOWSTONE', 'WY', 'YELLOWSTONE', 44.54444, -110.42111, 7835, 7835, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94173');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94176', 'SPOKANE', 'WA', 'FELTS FIELD AIRPORT', 47.68306, -117.32139, 1953, 1968, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94176');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94178', 'TWIN FALLS', 'ID', 'JSIN FLD-MAGIC VLY RGN APT', 42.48194, -114.48694, 4151, 4188, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94178');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94182', 'MCCALL', 'ID', 'MCCALL MUNICIPAL AIRPORT', 44.88889, -116.10167, 5013, 5020, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94182');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94184', 'SODA SPRINGS', 'ID', 'ALLEN H TIGERT AIRPORT', 42.65, -111.58333, 5839, 5839, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94184');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94185', 'BURNS', 'OR', 'BURNS MUNICIPAL AIRPORT', 43.595, -118.9564, 4140, 4170, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94185');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94186', 'MALTA', 'ID', 'MALTA', 42.3061, -113.3688, 4521, 4511, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94186');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94187', 'HANFORD', 'WA', 'HANFORD AIRPORT', 46.56667, -119.6, 732, 732, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94187');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94190', 'MOUNTAIN CITY', 'NV', 'WILD HORSE RESERVOIR', 41.6358, -115.8008, 6239, 6240, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94190');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94194', 'REXBURG', 'ID', 'REXBURG-MADISON COUNTY APT', 43.83389, -111.80444, 4858, 4858, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94194');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94195', 'CALDWELL', 'ID', 'CALDWELL INDUSTRIAL ARPT', 43.65, -116.63333, 2429, 2432, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94195');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94196', 'JOHN DAY', 'OR', 'GRNT CO RGNL/OGLVIE FD APT', 44.40389, -118.96333, 3690, 3697, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94196');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94197', 'OMAK', 'WA', 'OMAK AIRPORT', 48.464444, -119.516944, 1300, 1301, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94197');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94204', 'CASCADE LOCKS', 'OR', 'CASCADE LOCKS STATE AIRPORT', 45.68333, -121.88333, 151, 151, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94204');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94224', 'ASTORIA', 'OR', 'ASTORIA REGIONAL AIRPORT', 46.1569, -123.8825, 9, 22, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94224');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94225', 'HOQUIAM', 'WA', 'BOWERMAN AIRPORT', 46.9727, -123.9302, 12, 15, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94225');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94227', 'SHELTON', 'WA', 'SANDERSON FIELD AIRPORT', 47.238, -123.1408, 271, 269, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94227');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94236', 'KLAMATH FALLS', 'OR', 'KLAMATH FALLS AIRPORT', 42.14694, -121.72417, 4084, 4092, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94236');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94239', 'WENATCHEE', 'WA', 'PANGBORN MEMORIAL AIRPORT', 47.3977, -120.2013, 1229, 1230, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94239');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94240', 'QUILLAYUTE', 'WA', 'QUILLAYUTE AIRPORT', 47.9375, -124.555, 185, 205, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94240');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94248', 'RENTON', 'WA', 'RENTON MUNICIPAL AIRPORT', 47.49333, -122.21444, 29, 29, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94248');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94261', 'PORTLAND', 'OR', 'PORTLAND-HILLSBORO AIRPORT', 45.54056, -122.94861, 204, 229, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94261');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94263', 'BREMERTON', 'WA', 'BREMERTON NATIONAL AIRPORT', 47.48333, -122.76667, 444, 439, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94263');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94266', 'PORT ANGELES', 'WA', 'WILLIAM R FAIRCHILD INT AP', 48.12028, -123.49833, 288, 288, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94266');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94273', 'MC MINNVILLE', 'OR', 'MC MINNVILLE MUNICIPAL APT', 45.19472, -123.13389, 159, 160, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94273');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94274', 'TACOMA', 'WA', 'TACOMA NARROWS AIRPORT', 47.2675, -122.57611, 292, 315, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94274');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94276', 'FRIDAY HARBOR', 'WA', 'FRIDAY HARBOR AIRPORT', 48.52222, -123.02306, 109, 109, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94276');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94281', 'AURORA', 'OR', 'AURORA STATE AIRPORT', 45.24861, -122.76861, 196, 196, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94281');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94282', 'BURLINGTON/MOUNT VERNON', 'WA', 'SKAGIT REGIONAL AIRPORT', 48.46667, -122.41667, 140, 144, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94282');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94285', 'LAKEVIEW', 'OR', 'LAKE COUNTY AIRPORT', 42.16667, -120.4, 4728, 4728, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94285');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94290', 'SEATTLE SAND POINT', 'WA', 'WSFO SEATTLE SAND POINT', 47.6872, -122.2553, 60, 60, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94290');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94297', 'COUPEVILLE', 'WA', 'COUPEVILLE/NOLF', 48.21667, -122.63333, 199, 199, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94297');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94298', 'VANCOUVER', 'WA', 'PEARSON FIELD AIRPORT', 45.62083, -122.65722, 30, 18, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94298');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94299', 'ALTURAS', 'CA', 'ALTURAS MUNICIPAL AIRPORT', 41.49139, -120.56444, 4375, 4366, -8
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94299');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94601', 'ROCKLAND', 'ME', 'KNOX COUNTY REGIONAL ARPT', 44.06667, -69.1, 47, 55, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94601');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94605', 'CLAYTON LAKE', 'ME', 'OLCO LANDING AREA SEAPLANE BASE', 46.61667, -69.53333, 1000, 1031, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94605');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94623', 'WISCASSET', 'ME', 'WISCASSET AIRPORT', 43.96361, -69.71167, 68, 68, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94623');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94624', 'CHATHAM', 'MA', 'CHATHAM MUNICIPAL AIRPORT', 41.6875, -69.99333, 68, 68, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94624');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94626', 'GREENVILLE', 'ME', 'MAINE FOREST SERVICE', 45.46222, -69.59528, 1401, 1387, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94626');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94644', 'OLD TOWN', 'ME', 'OLD TOWN 2 W', 44.9281, -68.7006, 127, 127, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94644');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94645', 'LIMESTONE', 'ME', 'LIMESTONE 4 NNW', 46.9601, -67.8833, 737, 737, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94645');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94700', 'BERLIN', 'NH', 'BERLIN MUNICIPAL AIRPORT', 44.57611, -71.17861, 1158, 1158, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94700');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94702', 'BRIDGEPORT', 'CT', 'IGOR I SIKORSKY MEMORIAL AIRPORT', 41.15833, -73.12889, 5, 17, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94702');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94704', 'DANSVILLE', 'NY', 'DANSVILLE MUNICIPAL ARPT', 42.57083, -77.71333, 685, 662, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94704');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94705', 'BARRE/MONTPELIER', 'VT', 'EDWARD F KNAPP STATE ARPT', 44.2035, -72.5623, 1126, 1121, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94705');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94709', 'AUBURN/LEWISTON', 'ME', 'AUBURN/LEWISTON MUNI ARPT', 44.05, -70.28333, 288, 288, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94709');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94720', 'HYANNIS', 'MA', 'BRNSBL MUNI-BMAN/POL FD AP', 41.66861, -70.28, 55, 52, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94720');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94721', 'KEENE', 'NH', 'DILLANT-HOPKINS AIRPORT', 42.9, -72.26667, 481, 489, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94721');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94723', 'LAWRENCE', 'MA', 'LAWRENCE MUNICIPAL AIRPORT', 42.71722, -71.12389, 149, 149, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94723');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94724', 'VINEYARD HAVEN', 'MA', 'MARTHAS VINEYARD AIRPORT', 41.39306, -70.615, 68, 68, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94724');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94725', 'MASSENA', 'NY', 'MASSENA INTL/RCHARDS FD AP', 44.93583, -74.84583, 214, 214, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94725');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94726', 'NEW BEDFORD', 'MA', 'NEW BEDFORD RGNL AIRPORT', 41.67639, -70.95833, 80, 80, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94726');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94728', 'NEW YORK', 'NY', 'CENTRAL PARK', 40.77889, -73.96917, 130, 156, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94728');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94732', 'PHILADELPHIA', 'PA', 'NE PHILADELPHIA AIRPORT', 40.08194, -75.01111, 100, 119, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94732');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94733', 'PLATTSBURGH', 'NY', 'CLINTON COUNTY AIRPORT', 44.68722, -73.52306, 372, 372, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94733');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94737', 'RUTLAND', 'VT', 'RUTLAND STATE AIRPORT', 43.53333, -72.95, 784, 787, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94737');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94740', 'SARANAC LAKE', 'NY', 'ADIRONDACK REGIONAL ARPT', 44.38528, -74.20667, 1663, 1663, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94740');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94741', 'TETERBORO', 'NJ', 'TETERBORO AIRPORT', 40.85, -74.06139, 9, 7, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94741');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94745', 'WHITE PLAINS', 'NY', 'WESTCHESTER COUNTY AIRPORT', 41.06694, -73.7075, 379, 397, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94745');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94746', 'WORCESTER', 'MA', 'WORCESTER REGIONAL AIRPORT', 42.2706, -71.8731, 1000, 1017, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94746');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94761', 'ITHACA', 'NY', 'ITHACA TOMPKINS REGIONAL AIRPORT', 42.48333, -76.46667, 1099, 1099, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94761');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94765', 'LEBANON', 'NH', 'LEBANON MUNICIPAL AIRPORT', 43.62639, -72.30472, 598, 570, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94765');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94789', 'NEW YORK', 'NY', 'JOHN F KENNEDY INTERNATIONAL AIRPORT', 40.63861, -73.76222, 11, 22, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94789');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94790', 'WATERTOWN', 'NY', 'WATERTOWN INTL AIRPORT', 43.9922, -76.0217, 318, 333, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94790');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94793', 'BLOCK IS.', 'RI', 'BLOCK ISLAND STATE AIRPORT', 41.16806, -71.57778, 105, 108, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94793');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94794', 'UTICA', 'NY', 'ONEIDA COUNTY AIRPORT', 43.145, -75.38389, 711, 744, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94794');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94814', 'HOUGHTON LAKE', 'MI', 'ROSCOMMON COUNTY AIRPORT', 44.3591, -84.6738, 1151, 1160, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94814');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94815', 'KALAMAZOO', 'MI', 'KLMAZO/BTL CREEK INTL ARPT', 42.23472, -85.55194, 868, 874, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94815');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94817', 'PONTIAC', 'MI', 'OAKLAND CO. INTNL AIRPORT', 42.665, -83.41806, 976, 980, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94817');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94818', 'RACINE', 'WI', 'JOHN H BATTEN AIRPORT', 42.76111, -87.81361, 674, 674, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94818');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94822', 'ROCKFORD', 'IL', 'GREATER ROCKFORD AIRPORT', 42.1927, -89.093, 730, 743, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94822');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94823', 'PITTSBURGH', 'PA', 'PITTSBURGH INTERNATIONAL AIRPORT', 40.4846, -80.2144, 1203, 1203, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94823');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94830', 'TOLEDO', 'OH', 'TOLEDO EXPRESS AIRPORT', 41.58861, -83.80139, 669, 692, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94830');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94833', 'PERU', 'IN', 'GRISSOM AFB AIRPORT', 40.65, -86.15, 812, 811, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94833');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94836', 'MARQUETTE', 'MI', 'SAWYER INTERNATIONAL APT', 46.35, -87.4, 1221, 1221, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94836');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94846', 'CHICAGO', 'IL', 'CHICAGO O''HARE INTERNATIONAL AIRPORT', 41.995, -87.9336, 662, 674, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94846');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94847', 'DETROIT', 'MI', 'DETROIT METRO WAYNE COUNTY AIRPORT', 42.2313, -83.3308, 631, 664, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94847');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94849', 'ALPENA', 'MI', 'ALPENA COUNTY REGIONAL AIRPORT', 45.0716, -83.5644, 684, 693, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94849');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94850', 'MARQUETTE', 'MI', 'MARQUETTE MICHIGAN COUNTY AP (WFO)', 46.5313, -87.5491, 1415, 1425, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94850');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94852', 'MARION', 'IN', 'MARION MUNICIPAL AP', 40.48333, -85.68333, 863, NULL, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94852');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94853', 'ESCANABA', 'MI', 'DELTA COUNTY AIRPORT', 45.73333, -87.08333, 594, 609, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94853');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94854', 'JANESVILLE', 'WI', 'ROCK COUNTY AIRPORT', 42.61667, -89.03333, 808, 808, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94854');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94855', 'OSHKOSH', 'WI', 'WHITTMAN REGIONAL AIRPORT', 43.98444, -88.55694, 782, 839, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94855');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94860', 'GRAND RAPIDS', 'MI', 'GERALD R FORD INTERNATIONAL AIRPORT', 42.8825, -85.52389, 803, 803, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94860');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94868', 'FRANKLIN', 'PA', 'VENANGO REGIONAL AIRPORT', 41.38333, -79.86667, 1540, 1539, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94868');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94869', 'MILWAUKEE', 'WI', 'LAWRENCE J TIMMERMAN AIRPORT', 43.1086, -88.0306, 734, 745, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94869');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94870', 'CHAMPAIGN/URBANA', 'IL', 'UNIVERSI OF IL WILLARD APT', 40.03972, -88.27778, 754, 763, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94870');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94871', 'BENTON HARBOR', 'MI', 'SW MICHIGAN REGIONAL ARPT', 42.1256, -86.4284, 643, 643, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94871');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94889', 'ANN ARBOR', 'MI', 'ANN ARBOR MUNICIPAL ARPT', 42.22278, -83.74444, 839, 839, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94889');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94890', 'MOSINEE', 'WI', 'CENTRAL WISCONSIN AIRPORT', 44.78333, -89.66667, 1277, 1277, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94890');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94891', 'DANVILLE', 'IL', 'VERMILION COUNTY AIRPORT', 40.2, -87.6, 664, 696, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94891');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94892', 'CHICAGO/WEST CHICAGO', 'IL', 'DUPAGE AIRPORT', 41.91444, -88.24639, 754, 758, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94892');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94893', 'IRON MOUNTAIN/KINGSFORD', 'MI', 'FORD AIRPORT', 45.81833, -88.11444, 1122, 1182, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94893');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94894', 'MANISTEE', 'MI', 'MANISTEE CO-BLACKER ARPT', 44.26667, -86.25, 621, 621, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94894');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94895', 'MUNCIE', 'IN', 'DELAWARE CO-JOHNSON FD APT', 40.23417, -85.39361, 937, 947, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94895');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94896', 'MENOMINEE', 'MI', 'MEONE-MARINETTE TWIN CO AP', 45.11667, -87.63333, 625, 625, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94896');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94897', 'MANITOWOC', 'WI', 'MANITOWOC COUNTY AIRPORT', 44.13333, -87.66667, 651, 651, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94897');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94898', 'HARBOR BEACH', 'MI', 'HARBOR BEACH', 44.02194, -82.79306, 590, 590, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94898');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94899', 'COPPER HARBOR', 'MI', 'COPPER HARBOR', 47.46667, -87.88333, 626, 626, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94899');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94902', 'BROOKINGS', 'SD', 'BROOKINGS MUNICIPAL ARPT', 44.3, -96.8, 1648, 1647, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94902');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94908', 'DUBUQUE', 'IA', 'DUBUQUE REGIONAL AIRPORT', 42.39778, -90.70361, 1056, 1080, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94908');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94910', 'WATERLOO', 'IA', 'WATERLOO MUNICIPAL AIRPORT', 42.5544, -92.4011, 868, 878, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94910');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94911', 'YANKTON', 'SD', 'CHAN GURNEY MUNICIPAL ARPT', 42.8783, -97.3633, 1180, 1306, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94911');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94915', 'FORT CARSON', 'CO', 'BUTTS AAF AIRPORT', 38.687, -104.77, 0, 5838, -7
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94915');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94918', 'OMAHA (NORTH)', 'NE', 'NORTH OMAHA AIRPORT', 41.36667, -96.01667, 1309, 1331, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94918');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94919', 'GRAND RAPIDS', 'MN', 'GRAND RAPIDS/ITASCA CO-G NEWSTROM FIELD ARPT', 47.21111, -93.50972, 1355, 1355, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94919');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94925', 'GRAND FORKS', 'ND', 'GRAND FORKS AFB AIRPORT', 47.96667, -97.4, 913, 911, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94925');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94926', 'IRONWOOD', 'MI', 'GOGEBIC-IRON COUNTY ARPT', 46.53333, -90.13333, 1230, 1230, -5
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94926');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94927', 'WORTHINGTON', 'MN', 'WORTHINGTON MUNICIPAL ARPT', 43.6449, -95.5802, 1570, 1574, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94927');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94928', 'DEVILS LAKE', 'ND', 'DEVILS LAKE MUNI AIRPORT', 48.11667, -98.9, 1441, 1453, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94928');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94929', 'ASHLAND', 'WI', 'JOHN F KENNEDY MEMO ARPT', 46.54861, -90.91889, 826, 826, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94929');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94930', 'CAMP DOUGLAS', 'WI', 'VOLK FIELD AIRPORT', 43.93333, -90.26667, 912, 910, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94930');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94931', 'HIBBING', 'MN', 'CHISHOLM-HIBBING AIRPORT', 47.38639, -92.83889, 1352, 1357, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94931');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94933', 'FORT DODGE', 'IA', 'FORT DODGE REGIONAL APRT', 42.55, -94.18333, 1156, 1157, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94933');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94938', 'BRAINERD', 'MN', 'BRAINERD LAKES RGNL ARPT', 46.40472, -94.13083, 1221, 1226, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94938');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94940', 'SPARTA', 'WI', 'SPARTA/FORT MC COY AIRPORT', 43.96667, -90.73333, 829, 837, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94940');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94943', 'CHAMBERLAIN', 'SD', 'CHAMBERLAIN MUNI AIRPORT', 43.76667, -99.31833, 1703, 1695, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94943');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94946', 'BROKEN BOW', 'NE', 'BROKEN BOW MUNICIPAL ARPT', 41.43333, -99.63333, 2530, 2530, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94946');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94947', 'BEATRICE', 'NE', 'BEATRICE MUNICIPAL AIRPORT', 40.30139, -96.75389, 1324, 1324, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94947');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94948', 'FAIRMONT', 'MN', 'FAIRMONT MUNICIPAL AIRPORT', 43.65, -94.41667, 1161, 1161, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94948');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94949', 'HASTINGS', 'NE', 'HASTINGS MUNICIPAL AIRPORT', 40.6005, -98.4258, 1961, 1954, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94949');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94950', 'MITCHELL', 'SD', 'MITCHELL MUNICIPAL AIRPORT', 43.7743, -98.0384, 1299, 1303, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94950');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94951', 'COLUMBUS', 'NE', 'COLUMBUS MUNICIPAL AIRPORT', 41.43333, -97.35, 1447, 1444, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94951');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94956', 'THIEF RIVER FALLS', 'MN', 'THIEF RIVER FALLS RGNL APT', 48.06667, -96.18333, 1115, 1116, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94956');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94957', 'FALLS CITY', 'NE', 'BRENNER FIELD AIRPORT', 40.08028, -95.59194, 980, 985, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94957');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94958', 'ORD', 'NE', 'EVELYN SHARP FIELD AIRPORT', 41.62333, -98.94833, 2064, 2064, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94958');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94959', 'GALESBURG', 'IL', 'GALESBURG MUNICIPAL ARPT', 40.93333, -90.43333, 764, 764, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94959');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94960', 'MINNEAPOLIS', 'MN', 'CRYSTAL AIRPORT', 45.06194, -93.35083, 861, 869, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94960');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94961', 'BAUDETTE', 'MN', 'BAUDETTE INTERNATIONAL APT', 48.71667, -94.6, 1082, 1084, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94961');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94962', 'DETROIT LAKES', 'MN', 'DETRT LKS-WETHING FLD ARPT', 46.83333, -95.88333, 1397, 1396, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94962');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94963', 'MINNEAPOLIS, FLYING CLOUD', 'MN', 'FLYING CLOUD AIRPORT', 44.8321, -93.4705, 907, 944, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94963');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94964', 'ELY', 'MN', 'ELY MUNICIPAL AIRPORT', 47.81667, -91.83333, 1456, 1455, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94964');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94966', 'FERGUS FALLS', 'MN', 'FERGUS FALLS MUNICIPAL AIRPORT-EINAR MICKELSON FLD', 46.28333, -96.15, 1183, 1184, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94966');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94967', 'PARK RAPIDS', 'MN', 'PK RPDS MUNI-KONSHOK FD AP', 46.90056, -95.06778, 1434, 1443, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94967');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94968', 'ALBERT LEA', 'MN', 'ALBERT LEA MUNICIPAL ARPT', 43.68333, -93.36667, 1259, 1259, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94968');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94969', 'FARIBAULT', 'MN', 'FARIBAULT MUNICIPAL ARPT', 44.33333, -93.31667, 1060, 1056, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94969');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94971', 'ESTHERVILLE', 'IA', 'ESTHERVILLE MUNI AIRPORT', 43.40111, -94.74722, 1317, 1317, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94971');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94973', 'HAYWARD', 'WI', 'SAWYER COUNTY AIRPORT', 46.02611, -91.44417, 1204, 1215, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94973');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94974', 'MINNEAPOLIS', 'MN', 'ANOKA CO-BLNE AP(JNS FD) AP', 45.15, -93.21667, 912, 912, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94974');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94975', 'AINSWORTH', 'NE', 'AINSWORTH MUNICIPAL ARPT', 42.57694, -100.00056, 2584, 2589, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94975');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94976', 'MARSHALL', 'MN', 'SW MN RGNL MRSHL/RYAN FIELD AIRPORT', 44.45, -95.81667, 1179, 1180, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94976');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94977', 'CRANE LAKE', 'MN', 'SCOTTS SPB', 48.26667, -92.48333, 1119, NULL, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94977');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94978', 'TEKAMAH', 'NE', 'TEKAMAH MUNICIPAL AIRPORT', 41.76361, -96.17778, 1028, 1027, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94978');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94979', 'CLINTON', 'IA', 'CLINTON MUNICIPAL AIRPORT', 41.83333, -90.33333, 700, 709, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94979');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94982', 'DAVENPORT', 'IA', 'DAVENPORT MUNICIPAL AIRPT', 41.61389, -90.59139, 750, 753, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94982');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94985', 'MARSHFIELD', 'WI', 'MARSHFIELD MUNICIPAL ARPT', 44.63806, -90.1875, 1255, 1277, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94985');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94988', 'MARSHALLTOWN', 'IA', 'MARSHALLTOWN MUNICIPAL APT', 42.11056, -92.91611, 974, 974, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94988');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94989', 'AMES', 'IA', 'AMES MUNICIPAL AIRPORT', 41.99056, -93.61889, 955, 955, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94989');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94990', 'WINNER', 'SD', 'WINNER REGIONAL AIRPORT', 43.39056, -99.84222, 2032, 2032, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94990');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94991', 'LAMONI', 'IA', 'LAMONI MUNICIPAL AIRPORT', 40.6306, -93.9008, 1135, 1135, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94991');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94992', 'GRAND MARAIS', 'MN', 'GRAND MARAIS', 47.74722, -90.34444, 610, 610, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94992');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94993', 'SISSETON', 'SD', 'SISSETON MUNICIPAL AIRPORT', 45.66889, -96.99139, 1161, 1162, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94993');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94994', 'BOSCOBEL', 'WI', 'BOSCOBEL AIRPORT', 43.15611, -90.6775, 672, 671, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94994');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94995', 'LINCOLN', 'NE', 'LINCOLN 8 ENE', 40.8484, -96.5651, 1189, 1189, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94995');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94996', 'LINCOLN', 'NE', 'LINCOLN 11 SW', 40.6954, -96.8541, 1372, 1372, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94996');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94997', 'WADENA', 'MN', 'WADENA MUNICIPAL AIRPORT', 46.44667, -95.21167, 1370, 1370, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94997');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94998', 'AUDUBON', 'IA', 'AUDUBON COUNTY AIRPORT', 41.7, -94.91667, 1287, 1287, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94998');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '94999', 'AITKIN', 'MN', 'ATKN MUNI-S KURTZ FLD ARPT', 46.5475, -93.67667, 1204, 1204, -6
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='94999');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '96401', 'CHULITNA', 'AK', 'CHULITNA', 62.82639, -149.90667, 1400, 1350, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='96401');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '96402', 'SUTTON', 'AK', 'JONESVILLE MINE AIRPORT', 61.7138, -148.9088, 550, 870, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='96402');

INSERT INTO sample.station(wban, name, state, location, latitude, longitude, ground_height, station_height, timeZone)
SELECT '96404', 'TOK', 'AK', 'TOK 70 SE', 62.737, -141.2083, 2000, NULL, -9
WHERE NOT EXISTS (SELECT 1 FROM sample.station WHERE wban='96404');

END IF;

END
$$;