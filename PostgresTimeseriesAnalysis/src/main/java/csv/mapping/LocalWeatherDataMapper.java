// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package csv.mapping;

import csv.model.LocalWeatherData;
import de.bytefish.jtinycsvparser.builder.IObjectCreator;
import de.bytefish.jtinycsvparser.mapping.CsvMapping;
import de.bytefish.jtinycsvparser.typeconverter.FloatConverter;
import de.bytefish.jtinycsvparser.typeconverter.IgnoreMissingValuesConverter;
import de.bytefish.jtinycsvparser.typeconverter.LocalDateConverter;
import de.bytefish.jtinycsvparser.typeconverter.LocalTimeConverter;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class LocalWeatherDataMapper extends CsvMapping<LocalWeatherData>
{
    public LocalWeatherDataMapper(IObjectCreator creator)
    {
        super(creator);

        mapProperty(0, String.class, LocalWeatherData::setWban);
        mapProperty(1, LocalDate.class, LocalWeatherData::setDate, new LocalDateConverter(DateTimeFormatter.ofPattern("yyyyMMdd")));
        mapProperty(2, LocalTime.class, LocalWeatherData::setTime, new LocalTimeConverter(DateTimeFormatter.ofPattern("HHmm")));
        mapProperty(4, String.class, LocalWeatherData::setSkyCondition);
        mapProperty(12, Float.class, LocalWeatherData::setDryBulbCelsius, new IgnoreMissingValuesConverter<>(new FloatConverter(), "M"));
        mapProperty(24, Float.class, LocalWeatherData::setWindSpeed, new IgnoreMissingValuesConverter<>(new FloatConverter(), "M"));
        mapProperty(30, Float.class, LocalWeatherData::setStationPressure, new IgnoreMissingValuesConverter<>(new FloatConverter(), "M"));
    }
}