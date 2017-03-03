// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package pgsql.connection;

import de.bytefish.pgbulkinsert.functional.Func1;
import org.apache.commons.dbcp2.BasicDataSource;

import java.net.URI;
import java.sql.Connection;

public class PooledConnectionFactory implements Func1<Connection> {

    private final BasicDataSource connectionPool;

    public PooledConnectionFactory(URI databaseUri) {
        this.connectionPool = new BasicDataSource();

        initializeConnectionPool(connectionPool, databaseUri);
    }

    private void initializeConnectionPool(BasicDataSource connectionPool, URI databaseUri) {
        final String dbUrl = "jdbc:postgresql://" + databaseUri.getHost() + databaseUri.getPath();

        if (databaseUri.getUserInfo() != null) {
            connectionPool.setUsername(databaseUri.getUserInfo().split(":")[0]);
            connectionPool.setPassword(databaseUri.getUserInfo().split(":")[1]);
        }
        connectionPool.setDriverClassName("org.postgresql.Driver");
        connectionPool.setUrl(dbUrl);
        connectionPool.setInitialSize(1);
    }

    @Override
    public Connection invoke() throws Exception {
        return connectionPool.getConnection();
    }
}
