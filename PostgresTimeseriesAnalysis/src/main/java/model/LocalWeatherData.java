// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package model;

import java.time.LocalDate;
import java.time.LocalTime;

public class LocalWeatherData {

    private Station station;

    private LocalDate date;

    private LocalTime time;

    private Float temperature;

    private Float windSpeed;

    private Float stationPressure;

    private String skyCondition;

    public LocalWeatherData() {

    }

    public LocalWeatherData(Station station, LocalDate date, LocalTime time, Float temperature, Float windSpeed, Float stationPressure, String skyCondition) {
        this.station = station;
        this.date = date;
        this.time = time;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
        this.stationPressure = stationPressure;
        this.skyCondition = skyCondition;
    }

    public void setStation(Station station) {
        this.station = station;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public void setTime(LocalTime time) {
        this.time = time;
    }

    public void setTemperature(Float temperature) {
        this.temperature = temperature;
    }

    public void setWindSpeed(Float windSpeed) {
        this.windSpeed = windSpeed;
    }

    public void setStationPressure(Float stationPressure) {
        this.stationPressure = stationPressure;
    }

    public void setSkyCondition(String skyCondition) {
        this.skyCondition = skyCondition;
    }

    public Station getStation() {
        return station;
    }

    public LocalDate getDate() {
        return date;
    }

    public LocalTime getTime() {
        return time;
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
