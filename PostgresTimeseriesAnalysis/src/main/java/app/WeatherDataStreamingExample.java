// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package app;

import csv.model.Station;
import csv.parser.Parsers;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import de.bytefish.pgbulkinsert.pgsql.processor.BulkProcessor;
import de.bytefish.pgbulkinsert.pgsql.processor.handler.BulkWriteHandler;
import pgsql.connection.PooledConnectionFactory;
import pgsql.mapping.LocalWeatherDataBulkInsert;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WeatherDataStreamingExample {

    public static void main(String[] args) throws Exception {
        // Path to CSV File:
        final Path csvLocalWeatherDataFilePath = FileSystems.getDefault().getPath("/Users/bytefish/Data/Weather/QCLCD201503/201503hourly.txt");
        final Path csvStationDataFilePath = FileSystems.getDefault().getPath("/Users/bytefish/Data/Weather/QCLCD201503/201503station.txt");

        // A map between the WBAN and Station for faster Lookups:
        final Map<String, Station> stationMap = getStationMap(csvStationDataFilePath);

        // Create the Bulk Processor:
        try (BulkProcessor<pgsql.model.LocalWeatherData> pgBulkProcessor = getBulkProcessor()) {
            try (Stream<CsvMappingResult<csv.model.LocalWeatherData>> csvStream = getLocalWeatherData(csvLocalWeatherDataFilePath)) {
                csvStream
                        // Filter only valid entries:
                        .filter(x -> x.isValid())
                        // Now we can work on the Results:
                        .map(x -> x.getResult())
                        // Take only those measurements, that are also available in the list of stations:
                        .filter(x -> stationMap.containsKey(x.getWban()))
                        // Map into the general Analytics Model:
                        .map(x -> {
                            // Get the matching station:
                            csv.model.Station station = stationMap.get(x.getWban());
                            // Now build the Model:
                            return csv.converter.LocalWeatherDataConverter.convert(x, station);
                        })
                        // Now build the PostgresSQL Model:
                        .map(x -> {
                            return pgsql.converter.LocalWeatherDataConverter.convert(x);
                        })
                        // And now insert to the Database:
                        .forEach(x -> {
                            pgBulkProcessor.add(x);
                        });
            }
        }
    }

    private static BulkProcessor<pgsql.model.LocalWeatherData> getBulkProcessor() {
        // Database to connect to:
        URI databaseUri = URI.create("postgres://philipp:test_pwd@127.0.0.1:5432/sampledb");
        // Bulk Inserter to use:
        LocalWeatherDataBulkInsert bulkInsert = new LocalWeatherDataBulkInsert("sample", "weather_data");
        // Create the Connection Factory:
        PooledConnectionFactory connectionFactory = new PooledConnectionFactory(databaseUri);
        // Create the BulkWrite Handler:
        BulkWriteHandler<pgsql.model.LocalWeatherData> bulkWriteHandler = new BulkWriteHandler<>(bulkInsert, connectionFactory);
        // Build the BulkProcessor:
        return new BulkProcessor<>(bulkWriteHandler, 10000);
    }

    private static Stream<CsvMappingResult<csv.model.LocalWeatherData>> getLocalWeatherData(Path path) {
        return Parsers.LocalWeatherDataParser().readFromFile(path, StandardCharsets.US_ASCII);
    }

    private static Stream<csv.model.Station> getStations(Path path) {
        return Parsers.StationParser().readFromFile(path, StandardCharsets.US_ASCII)
                .filter(x -> x.isValid())
                .map(x -> x.getResult());
    }

    private static Map<String, Station> getStationMap(Path path) {
        try (Stream<csv.model.Station> stationStream = getStations(path)) {
            return stationStream
                    .collect(Collectors.toMap(csv.model.Station::getWban, x -> x));
        }
    }

}