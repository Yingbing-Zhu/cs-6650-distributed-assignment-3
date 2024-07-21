package connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariCPConnectionPool {
    private HikariDataSource dataSource;
    private String baseJdbcUrl;
    private String username;
    private String password;
    private int maxPoolSize;

    public HikariCPConnectionPool(String jdbcUrl, String username, String password, int maxPoolSize) {
        this.baseJdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.maxPoolSize = maxPoolSize;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(30000); // 30 seconds
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes

        this.dataSource = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public Connection getConnection(String dbName) {
        String dbUrl = baseJdbcUrl + "/" + dbName;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(30000); // 30 seconds
        config.setIdleTimeout(600000); // 10 minutes
        config.setMaxLifetime(1800000); // 30 minutes

        HikariDataSource customDataSource = null;
        try {
            customDataSource = new HikariDataSource(config);
            return customDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            if (customDataSource != null) {
                customDataSource.close();
            }
            return null;
        }
    }

    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
