// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package csv.mapping;

import csv.model.Station;
import de.bytefish.jtinycsvparser.builder.IObjectCreator;
import de.bytefish.jtinycsvparser.mapping.CsvMapping;
import de.bytefish.jtinycsvparser.typeconverter.IgnoreMissingValuesConverter;
import de.bytefish.jtinycsvparser.typeconverter.IntegerConverter;

public class StationMapper extends CsvMapping<Station>
{
    public StationMapper(IObjectCreator creator)
    {
        super(creator);

        mapProperty(0, String.class, Station::setWban);
        mapProperty(1, String.class, Station::setWmo);
        mapProperty(2, String.class, Station::setCallSign);
        mapProperty(3, String.class, Station::setClimateDivisionCode);
        mapProperty(4, String.class, Station::setClimateDivisionStateCode);
        mapProperty(5, String.class, Station::setClimateDivisionStationCode);
        mapProperty(6, String.class, Station::setName);
        mapProperty(7, String.class, Station::setState);
        mapProperty(8, String.class, Station::setLocation);
        mapProperty(9, Float.class, Station::setLatitude);
        mapProperty(10, Float.class, Station::setLongitude);
        mapProperty(11, Integer.class, Station::setGroundHeight, new IgnoreMissingValuesConverter<>(new IntegerConverter()));
        mapProperty(12, Integer.class, Station::setStationHeight, new IgnoreMissingValuesConverter<>(new IntegerConverter()));
        mapProperty(13, Integer.class, Station::setBarometer, new IgnoreMissingValuesConverter<>(new IntegerConverter()));
        mapProperty(14, Integer.class, Station::setTimeZone);
    }
}