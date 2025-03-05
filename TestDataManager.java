import java.sql.*;
import java.util.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TestDataManager provides comprehensive functionality for managing 
 * test data in SQL databases for QA automation purposes.
 */
public class TestDataManager {
    private static final Logger LOGGER = Logger.getLogger(TestDataManager.class.getName());
    
    // Database connection properties
    private String url;
    private String username;
    private String password;
    private Connection connection;

    /**
     * Constructor to initialize database connection
     * @param url Database connection URL
     * @param username Database username
     * @param password Database password
     */
    public TestDataManager(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * Establishes a database connection
     * @throws SQLException if connection fails
     */
    public void connect() throws SQLException {
        try {
            connection = DriverManager.getConnection(url, username, password);
            LOGGER.info("Database connection established successfully");
        } catch (SQLException e) {
            LOGGER.severe("Failed to connect to database: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Closes the database connection
     */
    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
                LOGGER.info("Database connection closed");
            } catch (SQLException e) {
                LOGGER.warning("Error closing database connection: " + e.getMessage());
            }
        }
    }

    /**
     * Inserts test data into a specified table
     * @param tableName Name of the table
     * @param data Map of column names and values
     * @return Generated key (if applicable)
     * @throws SQLException if insertion fails
     */
    public int insertTestData(String tableName, Map<String, Object> data) throws SQLException {
        if (connection == null) {
            throw new SQLException("No active database connection");
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            columns.append(entry.getKey()).append(",");
            values.append("?,");
            params.add(entry.getValue());
        }

        columns.setLength(columns.length() - 1);
        values.setLength(values.length() - 1);

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                                   tableName, columns, values);

        try (PreparedStatement pstmt = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            pstmt.executeUpdate();

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1);
                }
            }
        } catch (SQLException e) {
            LOGGER.severe("Error inserting test data: " + e.getMessage());
            throw e;
        }

        return -1;
    }

    /**
     * Retrieves test data from a specified table based on conditions
     * @param tableName Name of the table
     * @param conditions Map of column names and values to filter
     * @return List of result maps
     * @throws SQLException if retrieval fails
     */
    public List<Map<String, Object>> retrieveTestData(String tableName, Map<String, Object> conditions) throws SQLException {
        if (connection == null) {
            throw new SQLException("No active database connection");
        }

        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            whereClause.append(entry.getKey()).append(" = ? AND ");
            params.add(entry.getValue());
        }

        if (whereClause.length() > 0) {
            whereClause.setLength(whereClause.length() - 5);
        }

        String sql = String.format("SELECT * FROM %s %s", 
                                   tableName, 
                                   whereClause.length() > 0 ? "WHERE " + whereClause : "");

        List<Map<String, Object>> results = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        } catch (SQLException e) {
            LOGGER.severe("Error retrieving test data: " + e.getMessage());
            throw e;
        }

        return results;
    }

    /**
     * Updates test data in a specified table
     * @param tableName Name of the table
     * @param updateData Map of columns to update
     * @param conditions Map of conditions for update
     * @return Number of rows updated
     * @throws SQLException if update fails
     */
    public int updateTestData(String tableName, Map<String, Object> updateData, 
                               Map<String, Object> conditions) throws SQLException {
        if (connection == null) {
            throw new SQLException("No active database connection");
        }

        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Build SET clause
        for (Map.Entry<String, Object> entry : updateData.entrySet()) {
            setClause.append(entry.getKey()).append(" = ?, ");
            params.add(entry.getValue());
        }
        setClause.setLength(setClause.length() - 2);

        // Build WHERE clause
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            whereClause.append(entry.getKey()).append(" = ? AND ");
            params.add(entry.getValue());
        }
        whereClause.setLength(whereClause.length() - 5);

        String sql = String.format("UPDATE %s SET %s WHERE %s", 
                                   tableName, setClause, whereClause);

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            return pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.severe("Error updating test data: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Deletes test data from a specified table
     * @param tableName Name of the table
     * @param conditions Map of conditions for deletion
     * @return Number of rows deleted
     * @throws SQLException if deletion fails
     */
    public int deleteTestData(String tableName, Map<String, Object> conditions) throws SQLException {
        if (connection == null) {
            throw new SQLException("No active database connection");
        }

        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();

        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            whereClause.append(entry.getKey()).append(" = ? AND ");
            params.add(entry.getValue());
        }

        if (whereClause.length() > 0) {
            whereClause.setLength(whereClause.length() - 5);
        }

        String sql = String.format("DELETE FROM %s %s", 
                                   tableName, 
                                   whereClause.length() > 0 ? "WHERE " + whereClause : "");

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            return pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.severe("Error deleting test data: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Loads database configuration from a properties file
     * @param configPath Path to the properties file
     * @return Properties object with database configuration
     * @throws IOException if file reading fails
     */
    public static Properties loadDatabaseConfig(String configPath) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath)) {
            props.load(fis);
        }
        return props;
    }

    /**
     * Example usage method demonstrating the functionality
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Load database configuration
            Properties dbConfig = loadDatabaseConfig("db.properties");

            // Create TestDataManager instance
            TestDataManager manager = new TestDataManager(
                dbConfig.getProperty("db.url"),
                dbConfig.getProperty("db.username"),
                dbConfig.getProperty("db.password")
            );

            // Establish connection
            manager.connect();

            // Example: Insert test data
            Map<String, Object> testData = new HashMap<>();
            testData.put("username", "testuser");
            testData.put("email", "testuser@example.com");
            int userId = manager.insertTestData("users", testData);

            // Example: Retrieve test data
            Map<String, Object> retrieveConditions = new HashMap<>();
            retrieveConditions.put("username", "testuser");
            List<Map<String, Object>> results = manager.retrieveTestData("users", retrieveConditions);

            // Example: Update test data
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("email", "updated@example.com");
            Map<String, Object> updateConditions = new HashMap<>();
            updateConditions.put("username", "testuser");
            manager.updateTestData("users", updateData, updateConditions);

            // Close connection
            manager.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
