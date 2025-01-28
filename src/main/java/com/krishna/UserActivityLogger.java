package com.krishna;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class UserActivityLogger {

    private CqlSession session;

    public UserActivityLogger(String node, int port, String keyspace) {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(node, port))
                .withKeyspace(keyspace)
                .withLocalDatacenter("datacenter1")
                .build();
    }
  // for creating schema
//    public void createSchema() {
//        String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS activity_log "
//                + "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'};";
//        session.execute(createKeyspaceQuery);
//
//        String createTableQuery = "CREATE TABLE IF NOT EXISTS user_activity ("
//                + "user_id UUID, "
//                + "activity_id UUID, "
//                + "activity_type TEXT, "
//                + "timestamp TIMESTAMP, "
//                + "PRIMARY KEY (user_id, timestamp, activity_id)) "
//                + "WITH CLUSTERING ORDER BY (timestamp DESC, activity_id ASC);";
//        session.execute(createTableQuery);
//    }


    public void insertActivity(UUID userId, UUID activityId, String activityType, Instant timestamp, int ttlSeconds) {
        String insertQuery = "INSERT INTO user_activity (user_id, activity_id, activity_type, timestamp) "
                + "VALUES (?, ?, ?, ?) USING TTL ?;";
        PreparedStatement preparedStatement = session.prepare(insertQuery);
        BoundStatement boundStatement = preparedStatement.bind(userId, activityId, activityType, timestamp, ttlSeconds);
        session.execute(boundStatement);
    }


    public List<Row> getRecentActivities(UUID userId, int limit) {
        String selectQuery = "SELECT * FROM user_activity WHERE user_id = ? LIMIT ?;";
        PreparedStatement preparedStatement = session.prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(userId, limit);
        ResultSet resultSet = session.execute(boundStatement);
        return resultSet.all();
    }


    public List<Row> getActivitiesInTimeRange(UUID userId, Instant start, Instant end) {
        String selectQuery = "SELECT * FROM user_activity WHERE user_id = ? AND timestamp >= ? AND timestamp <= ?;";
        PreparedStatement preparedStatement = session.prepare(selectQuery);
        BoundStatement boundStatement = preparedStatement.bind(userId, start, end);
        ResultSet resultSet = session.execute(boundStatement);
        return resultSet.all();
    }


    public void close() {
        session.close();
    }

    public static void main(String[] args) {
        UserActivityLogger logger = new UserActivityLogger("localhost", 9042, "activity_log");
//        logger.createSchema();
        UUID userId1 = UUID.randomUUID();
        UUID userId2 = UUID.randomUUID();
        UUID activity1 = UUID.randomUUID();
        UUID activity2 = UUID.randomUUID();
        UUID activity3 = UUID.randomUUID();
        UUID activity4 = UUID.randomUUID();

        logger.insertActivity(userId1, activity1, "login", Instant.now(), 2592000);
        logger.insertActivity(userId1, activity2, "view", Instant.now().minusSeconds(120), 2592000);
        logger.insertActivity(userId2, activity3, "logout", Instant.now().minusSeconds(60), 2592000);
        logger.insertActivity(userId2, activity4, "purchase", Instant.now().minusSeconds(30), 2592000);


        List<Row> recentActivities = logger.getRecentActivities(userId1, 10);
        System.out.println("Recent Activities:");
        recentActivities.forEach(row -> {
            System.out.printf(
                    "User: %s, Activity: %s, Type: %s, Time: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getString("activity_type"),
                    row.getInstant("timestamp")
            );
        });

        Instant end = Instant.now();
        Instant start = end.minusSeconds(3600);
        List<Row> rangeActivities = logger.getActivitiesInTimeRange(userId2, start, end);
        System.out.println("\nActivities in Time Range:");
        rangeActivities.forEach(row -> {
            System.out.printf(
                    "User: %s, Activity: %s, Type: %s, Time: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getString("activity_type"),
                    row.getInstant("timestamp")
            );
        });


        logger.close();
    }
}
