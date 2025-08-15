package com.sdd;
import java.sql.*;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.*;
import java.nio.file.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import me.tongfei.progressbar.ProgressBar;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private DatabaseConfig config;

    // HikariCP DataSources
    private HikariDataSource sourceDataSource;
    private HikariDataSource targetDataSource;

    // Logging
    private PrintWriter logWriter;

    // Table configuration
    private Map<String, TableConfig> tableConfigs;

    public static void main(String[] args) {
        Main app = new Main();

        if(args.length == 0) {
            System.err.println("Usage: java -jar main.jar <config-file>");
            System.exit(1);
        }

        try {
            String configPath = args[0];
            app.initialize(configPath);
            app.performSync();
        } catch (Exception e) {
            System.err.println("Error during synchronization: " + e.getMessage());
            e.printStackTrace();
        } finally {
            app.cleanup();
        }
    }

    private void initialize(String configPath) throws Exception {
        loadConfiguration(configPath);

        String logFileName = "sync_log_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".log";
        logWriter = new PrintWriter(new FileWriter(logFileName, true));

        log("Initializing database synchronization...");
        log("Using configuration file: " + configPath);
        log("Target branch: " + config.targetBranch);

        // Register Oracle JDBC driver
        Class.forName("oracle.jdbc.driver.OracleDriver");

        // Initialize HikariCP DataSources
        initializeDataSources();

        initializeTableConfigsFromJson();

        log("Initialization completed successfully");
        log("Configured " + tableConfigs.size() + " tables for synchronization");
    }

    private void loadConfiguration(String configPath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        config = mapper.readValue(new File(configPath), DatabaseConfig.class);
    }

    private void initializeDataSources() {
        // Configure source database connection pool
        HikariConfig sourceConfig = new HikariConfig();
        sourceConfig.setJdbcUrl(config.sourceDb.url);
        sourceConfig.setUsername(config.sourceDb.username);
        sourceConfig.setPassword(config.sourceDb.password);
        sourceConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        sourceConfig.setMaximumPoolSize(config.threadPoolSize);
        sourceConfig.setMinimumIdle(2);
        sourceConfig.setConnectionTimeout(30000);
        sourceConfig.setIdleTimeout(600000);
        sourceConfig.setMaxLifetime(1800000);
        sourceConfig.setAutoCommit(false);
        sourceDataSource = new HikariDataSource(sourceConfig);

        // Configure target database connection pool
        HikariConfig targetConfig = new HikariConfig();
        targetConfig.setJdbcUrl(config.targetDb.url);
        targetConfig.setUsername(config.targetDb.username);
        targetConfig.setPassword(config.targetDb.password);
        targetConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        targetConfig.setMaximumPoolSize(config.threadPoolSize);
        targetConfig.setMinimumIdle(2);
        targetConfig.setConnectionTimeout(30000);
        targetConfig.setIdleTimeout(600000);
        targetConfig.setMaxLifetime(1800000);
        targetConfig.setAutoCommit(false);
        targetDataSource = new HikariDataSource(targetConfig);

        log("HikariCP connection pools initialized successfully");
    }

    private void initializeTableConfigsFromJson() {
        tableConfigs = new HashMap<>();

        for (TableConfigJson tableJson : config.tables) {
            TableConfig tableConfig = new TableConfig(
                    tableJson.tableName.toUpperCase(),
                    tableJson.primaryKey.toUpperCase(),
                    tableJson.branchField != null ? tableJson.branchField.toUpperCase() : null,
                    tableJson.hasBranchField,
                    tableJson.customQuery,
                    tableJson.enabled,
                    tableJson.batchSize != null ? tableJson.batchSize : 1000
            );

            if (tableConfig.enabled) {
                tableConfigs.put(tableJson.tableName, tableConfig);
            }
        }
    }

    private void performSync() throws SQLException {
        log("Starting synchronization for branch: " + config.targetBranch);

        int totalInserted = 0;
        int totalUpdated = 0;
        int totalErrors = 0;

        // Count enabled tables for progress tracking
        long enabledTablesCount = config.tables.stream().mapToLong(t -> t.enabled && tableConfigs.containsKey(t.tableName) ? 1 : 0).sum();
        
        try (ProgressBar tableProgressBar = new ProgressBar("Syncing Tables", enabledTablesCount)) {
            for (TableConfigJson tableJson : config.tables) {
                if (tableJson.enabled && tableConfigs.containsKey(tableJson.tableName)) {
                    TableConfig tableConfig = tableConfigs.get(tableJson.tableName);
                    
                    try {
                        SyncResult result = syncTable(tableJson.tableName, tableConfig);
                        totalInserted += result.inserted;
                        totalUpdated += result.updated;
                        totalErrors += result.errors;
                    } catch (Exception e) {
                        logError("Error processing table " + tableJson.tableName + ": " + e.getMessage());
                        totalErrors++;
                    }
                    tableProgressBar.step();
                }
            }
        }

        log("Synchronization completed:");
        log("Total records inserted: " + totalInserted);
        log("Total records updated: " + totalUpdated);
        log("Total errors: " + totalErrors);
    }

    private SyncResult syncTable(String tableName, TableConfig config) {
        SyncResult result = new SyncResult();
        Connection targetConn = null;

        try {
            log("Processing table: " + tableName);
            targetConn = targetDataSource.getConnection();

            List<Map<String, Object>> sourceData = getSourceData(tableName, config);
            log("Found " + sourceData.size() + " records in source table " + tableName);

            if (sourceData.isEmpty()) {
                log("No data found for table " + tableName + " - skipping");
                return result;
            }

            List<String> columns = getTableColumns(tableName, targetConn);

            int batchSize = config.batchSize;
            int totalBatches = (int) Math.ceil((double) sourceData.size() / batchSize);
            
            try (ProgressBar batchProgressBar = new ProgressBar("Processing " + tableName, totalBatches)) {
                for (int i = 0; i < sourceData.size(); i += batchSize) {
                    int endIndex = Math.min(i + batchSize, sourceData.size());
                    List<Map<String, Object>> batch = sourceData.subList(i, endIndex);

                    SyncResult batchResult = processBatch(tableName, columns, config.primaryKey, batch, targetConn);
                    result.inserted += batchResult.inserted;
                    result.updated += batchResult.updated;
                    result.errors += batchResult.errors;

                    targetConn.commit();

                    log("Processed batch " + (i/batchSize + 1) + " for table " + tableName +
                            " - Batch size: " + batch.size());
                    
                    batchProgressBar.step();
                }
            }

            log("Successfully processed table " + tableName +
                    " - Inserted: " + result.inserted + ", Updated: " + result.updated);

        } catch (Exception e) {
            try {
                if (targetConn != null) {
                    targetConn.rollback();
                }
            } catch (SQLException rollbackEx) {
                logError("Error during rollback: " + rollbackEx.getMessage());
            }
            logError("Error processing table " + tableName + ": " + e.getMessage());
            result.errors++;
        } finally {
            if (targetConn != null) {
                try {
                    targetConn.close();
                } catch (SQLException e) {
                    logError("Error closing target connection: " + e.getMessage());
                }
            }
        }

        return result;
    }

    private SyncResult processBatch(String tableName, List<String> columns, String primaryKey,
                                    List<Map<String, Object>> batch, Connection targetConn) throws SQLException {
        SyncResult result = new SyncResult();

        for (Map<String, Object> record : batch) {
            try {
                Object primaryKeyValue = record.get(primaryKey);
                
                if (primaryKeyValue == null) {
                    log("Skipping record in table " + tableName + " with null primary key (" + primaryKey + ")");
                    result.errors++;
                    continue;
                }
                
                if (recordExists(tableName, primaryKey, primaryKeyValue, targetConn)) {
                    updateRecord(tableName, columns, primaryKey, record, targetConn);
                    result.updated++;
                } else {
                    insertRecord(tableName, columns, record, targetConn);
                    result.inserted++;
                }
            } catch (SQLException e) {
                logError("Error processing record in table " + tableName +
                        " with " + primaryKey + "=" + record.get(primaryKey) + ": " + e.getMessage());
                result.errors++;

                throw e;
            }
        }

        return result;
    }

    private List<Map<String, Object>> getSourceData(String tableName, TableConfig config) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        Connection sourceConn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            sourceConn = sourceDataSource.getConnection();
            String query;

            if (config.customQuery != null && !config.customQuery.trim().isEmpty()) {
                query = config.customQuery + " ORDER BY " + config.primaryKey;
                stmt = sourceConn.prepareStatement(query);
                if (query.contains("?")) {
                    stmt.setString(1, this.config.targetBranch);
                }
            } else if (config.hasBranchField && config.branchField != null) {
                query = "SELECT * FROM " + tableName + " WHERE " + config.branchField + " = ?" + " ORDER BY " + config.primaryKey;
                stmt = sourceConn.prepareStatement(query);
                stmt.setString(1, this.config.targetBranch);
            } else {
                query = "SELECT * FROM " + tableName + " ORDER BY " + config.primaryKey;
                stmt = sourceConn.prepareStatement(query);
            }

            log("Executing query for table " + tableName + ": " + query.substring(0, Math.min(100, query.length())) + "...");

             stmt.setFetchSize(config.batchSize);
             
             rs = stmt.executeQuery();
             ResultSetMetaData metaData = rs.getMetaData();
             int columnCount = metaData.getColumnCount();

             while (rs.next()) {
                 Map<String, Object> record = new HashMap<>();
                 for (int i = 1; i <= columnCount; i++) {
                     String columnName = metaData.getColumnName(i);
                     Object value = rs.getObject(i);
                     record.put(columnName, value);
                 }
                 data.add(record);
             }
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (sourceConn != null) sourceConn.close();
        }

        return data;
    }

    private List<String> getTableColumns(String tableName, Connection targetConn) throws SQLException {
        List<String> columns = new ArrayList<>();

        String query = "SELECT COLUMN_NAME FROM USER_TAB_COLUMNS " +
                "WHERE TABLE_NAME = UPPER(?) " +
                "ORDER BY COLUMN_ID";

        PreparedStatement stmt = targetConn.prepareStatement(query);
        stmt.setString(1, tableName);
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            columns.add(rs.getString("COLUMN_NAME"));
        }

        rs.close();
        stmt.close();

        return columns;
    }

    private boolean recordExists(String tableName, String primaryKey, Object primaryKeyValue, Connection targetConn) throws SQLException {
        if (primaryKeyValue == null) return false;

        String query = "SELECT 1 FROM " + tableName + " WHERE " + primaryKey + " = ? AND ROWNUM = 1";
        PreparedStatement stmt = targetConn.prepareStatement(query);
        stmt.setObject(1, primaryKeyValue);
        ResultSet rs = stmt.executeQuery();

        boolean exists = rs.next();
        rs.close();
        stmt.close();

        return exists;
    }

    private void insertRecord(String tableName, List<String> columns, Map<String, Object> record, Connection targetConn) throws SQLException {
        StringBuilder query = new StringBuilder("INSERT INTO " + tableName + " (");
        StringBuilder values = new StringBuilder(" VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                query.append(", ");
                values.append(", ");
            }
            query.append(columns.get(i));
            values.append("?");
        }

        query.append(")").append(values).append(")");

        PreparedStatement stmt = targetConn.prepareStatement(query.toString());

        for (int i = 0; i < columns.size(); i++) {
            Object value = record.get(columns.get(i));
            stmt.setObject(i + 1, value);
        }

        stmt.executeUpdate();
        stmt.close();
    }

    private void updateRecord(String tableName, List<String> columns, String primaryKey,
                              Map<String, Object> record, Connection targetConn) throws SQLException {
        StringBuilder query = new StringBuilder("UPDATE " + tableName + " SET ");

        List<String> updateColumns = new ArrayList<>();
        for (String column : columns) {
            if (!column.equalsIgnoreCase(primaryKey)) {
                updateColumns.add(column);
            }
        }

        for (int i = 0; i < updateColumns.size(); i++) {
            if (i > 0) query.append(", ");
            query.append(updateColumns.get(i)).append(" = ?");
        }

        query.append(" WHERE ").append(primaryKey).append(" = ?");

        PreparedStatement stmt = targetConn.prepareStatement(query.toString());

        int paramIndex = 1;
        for (String column : updateColumns) {
            Object value = record.get(column);
            stmt.setObject(paramIndex++, value);
        }

        stmt.setObject(paramIndex, record.get(primaryKey));

        stmt.executeUpdate();
        stmt.close();
    }

    private void log(String message) {
        logger.info(message);
        if (logWriter != null) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String logMessage = "[" + timestamp + "] " + message;
            logWriter.println(logMessage);
            logWriter.flush();
        }
    }

    private void logError(String message) {
        logger.error(message);
        if (logWriter != null) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String logMessage = "[" + timestamp + "] ERROR: " + message;
            logWriter.println(logMessage);
            logWriter.flush();
        }
    }

    private void logDebug(String message) {
        logger.debug(message);
        if (logWriter != null) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String logMessage = "[" + timestamp + "] DEBUG: " + message;
            logWriter.println(logMessage);
            logWriter.flush();
        }
    }

    private void cleanup() {
        try {
            if (sourceDataSource != null) {
                sourceDataSource.close();
            }
            if (targetDataSource != null) {
                targetDataSource.close();
            }
            if (logWriter != null) {
                logWriter.close();
            }
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    public static class DatabaseConfig {
        public DatabaseConnection sourceDb;
        public DatabaseConnection targetDb;
        public String targetBranch;
        public int threadPoolSize = 5;
        public List<TableConfigJson> tables;
    }

    public static class DatabaseConnection {
        public String url;
        public String username;
        public String password;
    }

    public static class TableConfigJson {
        public String tableName;
        public String primaryKey;
        public String branchField;
        public boolean hasBranchField;
        public String customQuery;
        public boolean enabled = true;
        public Integer batchSize = 1000;
    }

    private static class TableConfig {
        String tableName;
        String primaryKey;
        String branchField;
        boolean hasBranchField;
        String customQuery;
        boolean enabled;
        int batchSize;

        public TableConfig(String tableName, String primaryKey, String branchField,
                           boolean hasBranchField, String customQuery, boolean enabled, int batchSize) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.branchField = branchField;
            this.hasBranchField = hasBranchField;
            this.customQuery = customQuery;
            this.enabled = enabled;
            this.batchSize = batchSize;
        }
    }

    private static class SyncResult {
        int inserted = 0;
        int updated = 0;
        int errors = 0;
    }
}