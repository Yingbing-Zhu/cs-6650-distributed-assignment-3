import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import connection.HikariCPConnectionPool;


import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private final static String QUEUE_NAME = "skier_post_queue";
    private static final String SERVER = "34.220.244.203"; // broker url
    // private static final String SERVER = "localhost"; // local test

    // test on consumer threads: 50, 60, 100
    private final static Integer NUM_THREADS = 50;
    private static Gson gson = new Gson();
    private static HikariCPConnectionPool connectionPool;
    // mysql setup
    private static final String URL = "jdbc:mysql://database-cs6650.cmbyswtg1g87.us-west-2.rds.amazonaws.com:3306/skiers";
    private static final String USER = "admin";
    private static final String PASSWORD = "rootroot";
    public static void main(String[] args) throws IOException, TimeoutException {
        // avoid exceeding the maximum connections limit of your RDS instance
        connectionPool = new HikariCPConnectionPool(URL, USER, PASSWORD, Math.min(NUM_THREADS, 60));
        // RabittMQ set up
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(SERVER);
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("rootroot");

        // want the process to stay alive while the consumer is listening asynchronously for messages to arrive
        com.rabbitmq.client.Connection connection = factory.newConnection();

        // create N threads, each of which uses the channel pool to publish messages
        for (int i = 0; i < NUM_THREADS; i++) {
            Runnable thread = () -> {
                try  {
                    // create one channel per thread
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    // allow one message to be delivered at a time
                    channel.basicQos(1);
                    // callback
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        Connection dbConnection = null;
                        List<JsonObject> batch = new ArrayList<>();
                        try {
                            String message = new String(delivery.getBody(), "UTF-8");
                            JsonObject json = gson.fromJson(message, JsonObject.class);
                            batch.add(json);
                            if (batch.size() >= 1000) { // Adjust batch size as needed
                                dbConnection = connectionPool.getConnection();
                                String rideInsertQuery = "INSERT INTO lift_rides (skier_id, season_id, day_id, lift_id, time, resort_id) VALUES (?, ?, ?, ?, ?, ?)";
                                try (PreparedStatement ps = dbConnection.prepareStatement(rideInsertQuery)) {
                                    for (JsonObject ride : batch) {
                                        ps.setInt(1, ride.get("skierID").getAsInt());
                                        ps.setString(2, ride.get("seasonID").getAsString());
                                        ps.setString(3, ride.get("dayID").getAsString());
                                        ps.setInt(4, ride.getAsJsonObject("liftRide").get("liftID").getAsInt());
                                        ps.setInt(5, ride.getAsJsonObject("liftRide").get("time").getAsInt());
                                        ps.setInt(6, ride.get("resortID").getAsInt());
                                        ps.addBatch();
                                    }
                                    ps.executeBatch();
                                }
                                batch.clear();
                            }

                        } catch (RuntimeException | SQLException e) {
                            // Return error message
                            System.out.println(" [.] " + e);
                        } finally {
                            if (dbConnection != null) {
                                try {
                                    dbConnection.close(); // Properly close the connection
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                            // Send response back to the client
                            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        }
                    };
                    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            new Thread(thread).start();
        }
    }


}
