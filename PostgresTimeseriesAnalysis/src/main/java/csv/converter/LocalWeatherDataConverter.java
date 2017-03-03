// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package csv.converter;


import java.time.LocalDate;
import java.time.LocalTime;

public class LocalWeatherDataConverter {

    public static model.LocalWeatherData convert(csv.model.LocalWeatherData csvLocalWeatherData, csv.model.Station csvStation) {

        LocalDate date = csvLocalWeatherData.getDate();
        LocalTime time = csvLocalWeatherData.getTime();
        String skyCondition = csvLocalWeatherData.getSkyCondition();
        Float stationPressure = csvLocalWeatherData.getStationPressure();
        Float temperature = csvLocalWeatherData.getDryBulbCelsius();
        Float windSpeed = csvLocalWeatherData.getWindSpeed();

        // Convert the Station data:
        model.Station station = convert(csvStation);

        return new model.LocalWeatherData(station, date, time, temperature, windSpeed, stationPressure, skyCondition);
    }

    public static model.Station convert(csv.model.Station csvStation) {
        String wban = csvStation.getWban();
        String name = csvStation.getName();
        String state = csvStation.getState();
        String location = csvStation.getLocation();
        Integer timeZone = csvStation.getTimeZone();
        model.GeoLocation geoLocation = new model.GeoLocation(csvStation.getLatitude(), csvStation.getLongitude());

        return new model.Station(wban, name, state, location, timeZone, geoLocation);
    }

}