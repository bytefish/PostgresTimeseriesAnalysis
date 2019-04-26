// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package app;

import csv.model.LocalWeatherData;
import csv.model.Station;
import csv.parser.Parsers;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import de.bytefish.pgbulkinsert.pgsql.processor.BulkProcessor;
import de.bytefish.pgbulkinsert.pgsql.processor.handler.BulkWriteHandler;
import io.reactivex.disposables.Disposable;
import org.postgresql.PGConnection;
import org.postgresql.jdbc.PgConnection;
import pgsql.connection.PooledConnectionFactory;
import pgsql.mapping.LocalWeatherDataBulkInsert;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import io.reactivex.Observable;

public class WeatherDataStreamingExample {

    private static final String databaseUri = "postgres://philipp:test_pwd@127.0.0.1:5432/sampledb";

    public static void main(String[] args) {

        // The PostgreSQL Bulk Writer:
        final LocalWeatherDataBulkInsert writer = new LocalWeatherDataBulkInsert("sample", "weather_data");

        // Path to QCLCD CSV Files:
        final Path csvStationDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503station.txt");
        final Path csvLocalWeatherDataFilePath = FileSystems.getDefault().getPath("D:\\datasets\\201503hourly.txt");

        // A map between the WBAN and Station for faster Lookups:
        final Map<String, Station> stationMap = getStationMap(csvStationDataFilePath);

        try (Stream<CsvMappingResult<LocalWeatherData>> csvStream = getLocalWeatherData(csvLocalWeatherDataFilePath)) {
            Stream<pgsql.model.LocalWeatherData> localWeatherDataStream = csvStream
                    // Filter only valid entries:
                    .filter(x -> x.isValid())
                    // Now we can work on the Results:
                    .map(x -> x.getResult())
                    // Take only measurements available in the list of stations:
                    .filter(x -> stationMap.containsKey(x.getWban()))
                    // Map into the general Analytics Model:
                    .map(x -> {
                        // Get the matching station now:
                        csv.model.Station station = stationMap.get(x.getWban());
                        // And build the Model:
                        return csv.converter.LocalWeatherDataConverter.convert(x, station);
                    })
                    // Now build the PostgresSQL Model:
                    .map(x -> pgsql.converter.LocalWeatherDataConverter.convert(x));

            // Turn it into an Observable for simplified Buffering:
            Disposable disposable = Observable.fromIterable(localWeatherDataStream::iterator)
                    // Wait two Seconds or Buffer up to 80000 entities:
                    .buffer(2, TimeUnit.SECONDS,80000)
                    // Subscribe to the Batches:
                    .subscribe(x -> {
                        // Connect to your Postgres Instance:
                        try (Connection connection = DriverManager.getConnection(databaseUri)) {
                            // Get the underlying PGConnection:
                            PGConnection pgConnection = connection.unwrap(PGConnection.class);

                            // And Bulk Write the Results:
                            writer.saveAll(pgConnection, x);
                        }
                    });

            // Probably not neccessary, but dispose anyway:
            if(disposable.isDisposed()) {
                disposable.dispose();
            }
        }
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