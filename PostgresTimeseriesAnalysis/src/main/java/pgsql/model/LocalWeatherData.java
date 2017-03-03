// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package pgsql.model;

import java.time.LocalDateTime;

public class LocalWeatherData {

    private String wban;

    private LocalDateTime dateTime;

    private Float temperature;

    private Float windSpeed;

    private Float stationPressure;

    private String skyCondition;

    public LocalWeatherData(String wban, LocalDateTime dateTime, Float temperature, Float windSpeed, Float stationPressure, String skyCondition) {
        this.wban = wban;
        this.dateTime = dateTime;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.stationPressure = stationPressure;
        this.skyCondition = skyCondition;
    }

    public String getWban() {
        return wban;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public Float getTemperature() {
        return temperature;
    }

    public Float getWindSpeed() {
        return windSpeed;
    }

    public Float getStationPressure() {
        return stationPressure;
    }

    public String getSkyCondition() {
        return skyCondition;
    }
}