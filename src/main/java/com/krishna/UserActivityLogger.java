package com.krishna;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import java.time.Instant;
import java.util.UUID;
import java.util.List;
import java.net.InetSocketAddress;

public class UserActivityLogger {
    private final CqlSession session;
    private final PreparedStatement insertStatement;
    private final PreparedStatement recentActivitiesStatement;
    private final PreparedStatement timeRangeStatement;

    public UserActivityLogger(String contactPoint, String keyspace) {
        this.session = CqlSession.builder()
                .addContactPoint(InetSocketAddress.createUnresolved(contactPoint, 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " " +
                "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}");
        session.execute("USE " + keyspace);
        session.execute("CREATE TABLE IF NOT EXISTS user_activities (" +
                "user_id UUID, " +
                "timestamp TIMESTAMP, " +
                "activity_id UUID, " +
                "activity_type TEXT, " +
                "PRIMARY KEY ((user_id), timestamp, activity_id)) " +
                "WITH CLUSTERING ORDER BY (timestamp DESC, activity_id ASC)");


        this.insertStatement = session.prepare("INSERT INTO user_activities " +
                "(user_id, timestamp, activity_id, activity_type) " +
                "VALUES (?, ?, ?, ?) USING TTL ?");

        this.recentActivitiesStatement = session.prepare("SELECT * FROM user_activities " +
                "WHERE user_id = ? LIMIT ?");

        this.timeRangeStatement = session.prepare("SELECT * FROM user_activities " +
                "WHERE user_id = ? AND timestamp >= ? AND timestamp <= ?");
    }

    public void logActivity(UUID userId, String activityType, Instant timestamp, int ttlDays) {
        BoundStatement statement = insertStatement.bind()
                .setUuid(0, userId)
                .setInstant(1, timestamp)
                .setUuid(2, UUID.randomUUID())
                .setString(3, activityType)
                .setInt(4, ttlDays * 86400)
                .setConsistencyLevel(ConsistencyLevel.QUORUM);

        session.execute(statement);
    }

    public List<Row> getRecentActivities(UUID userId, int limit) {
        BoundStatement statement = recentActivitiesStatement.bind()
                .setUuid(0, userId)
                .setInt(1, limit)
                .setConsistencyLevel(ConsistencyLevel.QUORUM);

        return session.execute(statement).all();
    }

    public List<Row> getActivitiesInTimeRange(UUID userId, Instant start, Instant end) {
        BoundStatement statement = timeRangeStatement.bind()
                .setUuid(0, userId)
                .setInstant(1, start)
                .setInstant(2, end)
                .setConsistencyLevel(ConsistencyLevel.QUORUM);

        return session.execute(statement).all();
    }

    public void close() {
        session.close();
    }

    public static void main(String[] args) {
        UserActivityLogger logger = new UserActivityLogger("127.0.0.1", "activity_log");
        UUID userId = UUID.randomUUID();
        Instant baseTime = Instant.now();

        Object[][] activities = {
                {"login", 0L},
                {"view_home", -60L},
                {"view_profile", -120L},
                {"logout", -180L},
                {"purchase", -240L},
                {"search", -300L},
                {"add_to_cart", -360L},
                {"view_product", -420L},
                {"update_settings", -480L},
                {"password_change", -540L}
        };


        for (Object[] activity : activities) {
            long offset = ((Number) activity[1]).longValue();
            Instant timestamp = baseTime.plusSeconds(offset);
            logger.logActivity(
                    userId,
                    (String) activity[0],
                    timestamp,
                    30
            );
        }


        UUID anotherUser = UUID.randomUUID();
        logger.logActivity(anotherUser, "login", baseTime.minusSeconds(600), 30);
        logger.logActivity(anotherUser, "view_home", baseTime.minusSeconds(590), 30);


        System.out.println("\nAll recent activities for main user:");
        List<Row> recentActivities = logger.getRecentActivities(userId, 5); // Retrieve top 5 activities
        for (Row row : recentActivities) {
            System.out.printf("user_id: %s, activity_id: %s, timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("timestamp"),
                    row.getString("activity_type"));
        }


        System.out.println("\nActivities for another user:");
        List<Row> anotherUserActivities = logger.getRecentActivities(anotherUser, 5); // Retrieve top 5 activities
        for (Row row : anotherUserActivities) {
            System.out.printf("user_id: %s, activity_id: %s, timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("timestamp"),
                    row.getString("activity_type"));
        }


        Instant startTime = baseTime.minusSeconds(300); // Start 5 minutes ago
        Instant endTime = baseTime.plusSeconds(60);    // End 1 minute in the future
        System.out.println("\nActivities for main user within time range:");
        List<Row> activitiesInRange = logger.getActivitiesInTimeRange(userId, startTime, endTime);
        for (Row row : activitiesInRange) {
            System.out.printf("user_id: %s, activity_id: %s, timestamp: %s, activity_type: %s%n",
                    row.getUuid("user_id"),
                    row.getUuid("activity_id"),
                    row.getInstant("timestamp"),
                    row.getString("activity_type"));
        }


        logger.close();
    }
}




