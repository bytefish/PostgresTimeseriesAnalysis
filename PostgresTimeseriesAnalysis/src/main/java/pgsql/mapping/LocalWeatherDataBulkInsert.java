// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package pgsql.mapping;

import de.bytefish.pgbulkinsert.PgBulkInsert;

public class LocalWeatherDataBulkInsert extends PgBulkInsert<pgsql.model.LocalWeatherData> {

    public LocalWeatherDataBulkInsert(String schemaName, String tableName) {

        super(schemaName, tableName);

        mapString("wban", pgsql.model.LocalWeatherData::getWban);
        mapTimeStamp("dateTime", pgsql.model.LocalWeatherData::getDateTime);
        mapReal("temperature", pgsql.model.LocalWeatherData::getTemperature);
        mapReal("windSpeed", pgsql.model.LocalWeatherData::getWindSpeed);
        mapReal("stationPressure", pgsql.model.LocalWeatherData::getStationPressure);
        mapString("skyCondition", pgsql.model.LocalWeatherData::getSkyCondition);
    }

}
