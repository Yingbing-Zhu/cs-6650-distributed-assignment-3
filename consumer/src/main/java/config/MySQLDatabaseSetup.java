package config;

import connection.HikariCPConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLDatabaseSetup {

    private static final String DATABASE_NAME = "skiers";
    private static final String JDBC_URL = "jdbc:mysql://database-cs6650.cmbyswtg1g87.us-west-2.rds.amazonaws.com:3306";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "rootroot";
    private static final int MAX_POOL_SIZE = 1;
    
    public static void main(String[] args) {
       HikariCPConnectionPool connectionPool = new HikariCPConnectionPool(JDBC_URL, USERNAME, PASSWORD, MAX_POOL_SIZE);

       try (Connection baseConn = connectionPool.getConnection();
            Statement baseStmt = baseConn.createStatement()) {

           // Create Database
           String createDatabaseSQL = "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME;
           baseStmt.executeUpdate(createDatabaseSQL);
           System.out.println("Database created successfully...");

           // Connect to the newly created database
           try (Connection dbConn = connectionPool.getConnection(DATABASE_NAME);
                Statement dbStmt = dbConn.createStatement()) {

               // Create Table
               String createTableSQL = "CREATE TABLE IF NOT EXISTS lift_rides (" +
                       "id INT AUTO_INCREMENT PRIMARY KEY," +
                       "skier_id INT NOT NULL," +
                       "season_id VARCHAR(10) NOT NULL," +
                       "day_id VARCHAR(10) NOT NULL," +
                       "lift_id INT NOT NULL," +
                       "time INT NOT NULL," +
                       "resort_id INT NOT NULL)";
               dbStmt.executeUpdate(createTableSQL);
               System.out.println("Table created successfully...");
           }

       } catch (SQLException e) {
           e.printStackTrace();
       } finally {
           connectionPool.close();
       }
   }
}
