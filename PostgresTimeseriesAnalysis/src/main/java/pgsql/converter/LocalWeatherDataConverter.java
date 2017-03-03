// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package pgsql.converter;

import java.time.LocalDateTime;

public class LocalWeatherDataConverter {

    public static pgsql.model.LocalWeatherData convert(model.LocalWeatherData modelLocalWeatherData) {

        String wban = modelLocalWeatherData.getStation().getWban();
        LocalDateTime dateTime = modelLocalWeatherData.getDate().atTime(modelLocalWeatherData.getTime());
        Float temperature = modelLocalWeatherData.getTemperature();
        Float windSpeed = modelLocalWeatherData.getWindSpeed();
        Float stationPressure = modelLocalWeatherData.getStationPressure();
        String skyCondition = modelLocalWeatherData.getSkyCondition();

        return new pgsql.model.LocalWeatherData(wban, dateTime, temperature, windSpeed, stationPressure, skyCondition);
    }
}
