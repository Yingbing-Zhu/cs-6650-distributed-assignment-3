import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import connection.HikariCPConnectionPool;

public class MySQLQuery {
    private Connection connection;
    private HikariCPConnectionPool connectionPool;

    public MySQLQuery(HikariCPConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
        this.connection = connectionPool.getConnection("skiers");
        System.out.println("Connected to MySQL Database.");
    }

    public void closeConnection() {
        if (this.connection != null) {
            try {
                this.connection.close();
                System.out.println("MySQL connection closed.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 1. For skier N, how many days have they skied this season?
    public long countSkiedDays(int skierId, String seasonId) throws SQLException {
        String query = "SELECT COUNT(DISTINCT day_id) FROM lift_rides WHERE skier_id = ? AND season_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, skierId);
            ps.setString(2, seasonId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    // 2. For skier N, what are the vertical totals for each ski day?
    public void printVerticalTotalsPerDay(int skierId, String seasonId) throws SQLException {
        String query = "SELECT day_id, SUM(lift_id * 10) AS total_vertical FROM lift_rides WHERE skier_id = ? AND season_id = ? GROUP BY day_id";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, skierId);
            ps.setString(2, seasonId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String dayId = rs.getString("day_id");
                int totalVertical = rs.getInt("total_vertical");
                System.out.println("Day: " + dayId + ", Total Vertical: " + totalVertical);
            }
        }
    }

    // 3. For skier N, show me the lifts they rode on each ski day
    public void printLiftsPerDay(int skierId, String seasonId) throws SQLException {
        String query = "SELECT day_id, GROUP_CONCAT(lift_id) AS lifts FROM lift_rides WHERE skier_id = ? AND season_id = ? GROUP BY day_id";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, skierId);
            ps.setString(2, seasonId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String dayId = rs.getString("day_id");
                String lifts = rs.getString("lifts");
                System.out.println("Day " + dayId + " Lifts: " + lifts);
            }
        }
    }

    // 4. How many unique skiers visited resort X on day N?
    public long countUniqueSkiersAtResortOnDay(int resortId, String dayId) throws SQLException {
        String query = "SELECT COUNT(DISTINCT skier_id) FROM lift_rides WHERE resort_id = ? AND day_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setInt(1, resortId);
            ps.setString(2, dayId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

//    public static void main(String[] args) {
//        String JDBC_URL = "jdbc:mysql://database-cs6650.cmbyswtg1g87.us-west-2.rds.amazonaws.com:3306";
//        String USERNAME = "admin";
//        String PASSWORD = "rootroot";
//        HikariCPConnectionPool connectionPool = new HikariCPConnectionPool(JDBC_URL, USERNAME, PASSWORD, 1);
//        MySQLQuery analytics = new MySQLQuery(connectionPool);
//        try {
//            // Example Usage:
//            System.out.println("Days skied: " + analytics.countSkiedDays(58580, "2024"));
//            analytics.printVerticalTotalsPerDay(58580, "2024");
//            analytics.printLiftsPerDay(58580, "2024");
//            System.out.println("Unique skiers at resort 9 on day '1': " + analytics.countUniqueSkiersAtResortOnDay(9, "1"));
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            analytics.closeConnection();
//        }
//    }
}
