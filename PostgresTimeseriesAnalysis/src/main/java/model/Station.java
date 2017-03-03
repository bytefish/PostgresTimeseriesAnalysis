// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package model;

public class Station {

    private String wban;

    private String name;

    private String state;

    private String location;

    private Integer timeZone;

    private GeoLocation geoLocation;

    public Station() {

    }

    public Station(String wban, String name, String state, String location, Integer timeZone, GeoLocation geoLocation) {
        this.wban = wban;
        this.name = name;
        this.state = state;
        this.location = location;
        this.timeZone = timeZone;
        this.geoLocation = geoLocation;
    }

    public void setWban(String wban) {
        this.wban = wban;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setTimeZone(Integer timeZone) {
        this.timeZone = timeZone;
    }

    public void setGeoLocation(GeoLocation geoLocation) {
        this.geoLocation = geoLocation;
    }

    public String getWban() {
        return wban;
    }

    public String getName() {
        return name;
    }

    public String getState() {
        return state;
    }

    public String getLocation() {
        return location;
    }

    public Integer getTimeZone() {
        return timeZone;
    }

    public GeoLocation getGeoLocation() {
        return geoLocation;
    }
}
