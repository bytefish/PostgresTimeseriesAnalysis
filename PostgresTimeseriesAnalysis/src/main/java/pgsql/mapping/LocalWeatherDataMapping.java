// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package pgsql.mapping;

import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import pgsql.model.LocalWeatherData;

public class LocalWeatherDataMapping extends AbstractMapping<LocalWeatherData> {

    public LocalWeatherDataMapping(String schemaName, String tableName) {

        super(schemaName, tableName);

        mapText("wban", pgsql.model.LocalWeatherData::getWban);
        mapTimeStamp("dateTime", pgsql.model.LocalWeatherData::getDateTime);
        mapFloat("temperature", pgsql.model.LocalWeatherData::getTemperature);
        mapFloat("windSpeed", pgsql.model.LocalWeatherData::getWindSpeed);
        mapFloat("stationPressure", pgsql.model.LocalWeatherData::getStationPressure);
        mapText("skyCondition", pgsql.model.LocalWeatherData::getSkyCondition);
    }

}
