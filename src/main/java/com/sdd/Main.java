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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        sourceConfig.setMaximumPoolSize(config.threadPoolSize + 10);
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
        targetConfig.setMaximumPoolSize(config.threadPoolSize + 10);
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
                    tableJson.customQuery
            );


            tableConfigs.put(tableJson.tableName, tableConfig);

        }
    }

    private void performSync() throws SQLException {
        log("Starting synchronization for branch: " + config.targetBranch);

        int totalInserted = 0;
        int totalUpdated = 0;
        int totalErrors = 0;

        // Count enabled tables for progress tracking
        long enabledTablesCount = config.tables.stream().mapToLong(t -> tableConfigs.containsKey(t.tableName) ? 1 : 0).sum();
        
        try (ProgressBar tableProgressBar = new ProgressBar("Syncing Tables", enabledTablesCount)) {
            for (TableConfigJson tableJson : config.tables) {
                if (tableConfigs.containsKey(tableJson.tableName)) {
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

        try {
            log("Processing table: " + tableName);

            int totalCount = getSourceDataCount(tableName, config);
            log("Found " + totalCount + " records in source table " + tableName);

            if (totalCount == 0) {
                log("No data found for table " + tableName + " - skipping");
                return result;
            }

            Connection tempConn = targetDataSource.getConnection();
            List<String> columns = getTableColumns(tableName, tempConn);
            tempConn.close();

            int batchSize = this.config.batchSize;
            int totalBatches = (int) Math.ceil((double) totalCount / batchSize);
            

            result = processParallel(tableName, config, columns, totalCount, totalBatches);


            log("Successfully processed table " + tableName +
                    " - Inserted: " + result.inserted + ", Updated: " + result.updated + ", Errors: " + result.errors);

        } catch (Exception e) {
            logError("Error processing table " + tableName + ": " + e.getMessage());
            result.errors++;
        }

        return result;
    }
    

    private SyncResult processParallel(String tableName, TableConfig config, List<String> columns, int totalCount, int totalBatches) throws SQLException {
        SyncResult result = new SyncResult();
        AtomicInteger totalInserted = new AtomicInteger(0);
        AtomicInteger totalUpdated = new AtomicInteger(0);
        AtomicInteger totalErrors = new AtomicInteger(0);
        
        // Use thread pool from config, but limit to reasonable number
        int threadPoolSize = Math.min(this.config.threadPoolSize, Math.min(totalBatches, 10));
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        
        try (ProgressBar progressBar = new ProgressBar("Processing " + tableName, totalBatches)) {
            List<Future<SyncResult>> futures = new ArrayList<>();
            
            // Submit batch processing tasks
            for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
                final int offset = batchIndex * this.config.batchSize;
                final int limit = Math.min(this.config.batchSize, totalCount - offset);
                
                Future<SyncResult> future = executor.submit(() -> {
                    try {
                        return processBatchParallel(tableName, config, columns, offset, limit);
                    } catch (Exception e) {
                        logError("Error in parallel batch processing for table " + tableName + ": " + e.getMessage());
                        SyncResult errorResult = new SyncResult();
                        errorResult.errors = 1;
                        return errorResult;
                    }
                });
                
                futures.add(future);
            }
            
            // Collect results
            for (Future<SyncResult> future : futures) {
                try {
                    SyncResult batchResult = future.get();
                    totalInserted.addAndGet(batchResult.inserted);
                    totalUpdated.addAndGet(batchResult.updated);
                    totalErrors.addAndGet(batchResult.errors);
                    progressBar.step();
                } catch (Exception e) {
                    logError("Error getting batch result: " + e.getMessage());
                    totalErrors.incrementAndGet();
                    progressBar.step();
                }
            }
            
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        result.inserted = totalInserted.get();
        result.updated = totalUpdated.get();
        result.errors = totalErrors.get();
        
        return result;
    }
    
    private SyncResult processBatchParallel(String tableName, TableConfig config, List<String> columns, int offset, int limit) throws SQLException {
        SyncResult result = new SyncResult();
        Connection targetConn = null;
        
        try {
            // Get data for this specific batch
            List<Map<String, Object>> batchData = getSourceDataBatch(tableName, config, offset, limit);
            
            if (batchData.isEmpty()) {
                return result;
            }
            
            targetConn = targetDataSource.getConnection();
            targetConn.setAutoCommit(false);
            
            for (Map<String, Object> record : batchData) {
                try {
                    Object primaryKeyValue = record.get(config.primaryKey);
                    if (primaryKeyValue == null) {
                        logError("Skipping record with null primary key in table " + tableName);
                        result.errors++;
                        continue;
                    }
                    
                    if (recordExists(tableName, config.primaryKey, primaryKeyValue, targetConn)) {
                        updateRecord(tableName, columns, config.primaryKey, record, targetConn);
                        result.updated++;
                    } else {
                        insertRecord(tableName, columns, record, targetConn);
                        result.inserted++;
                    }
                } catch (SQLException e) {
                    logError("Error processing record in table " + tableName + ": " + e.getMessage());
                    result.errors++;
                }
            }
            
            targetConn.commit();
            logDebug("Completed batch for table " + tableName + " (offset: " + offset + ", limit: " + limit + ") - Inserted: " + result.inserted + ", Updated: " + result.updated + ", Errors: " + result.errors);
            
        } catch (Exception e) {
            if (targetConn != null) {
                try {
                    targetConn.rollback();
                } catch (SQLException rollbackEx) {
                    logError("Error during rollback in parallel batch: " + rollbackEx.getMessage());
                }
            }
            throw e;
        } finally {
            if (targetConn != null) {
                try {
                    targetConn.close();
                } catch (SQLException e) {
                    logError("Error closing target connection in parallel batch: " + e.getMessage());
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
        // Get total count first
        int totalCount = getSourceDataCount(tableName, config);
        log("Total records found for table " + tableName + ": " + totalCount);
        
        if (totalCount == 0) {
            return new ArrayList<>();
        }
        
        // For small datasets, use single-threaded approach
        if (totalCount <= this.config.batchSize * 2) {
            return getSourceDataBatch(tableName, config, 0, totalCount);
        }
        
        // For large datasets, we'll return empty list here and handle parallel processing in syncTable
        return new ArrayList<>();
    }
    
    private int getSourceDataCount(String tableName, TableConfig config) throws SQLException {
        Connection sourceConn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        try {
            sourceConn = sourceDataSource.getConnection();
            String countQuery;
            
            if (config.customQuery != null && !config.customQuery.trim().isEmpty()) {
                // Extract FROM clause from custom query for counting
                String baseQuery = config.customQuery;
                if (baseQuery.toUpperCase().contains("ORDER BY")) {
                    baseQuery = baseQuery.substring(0, baseQuery.toUpperCase().lastIndexOf("ORDER BY"));
                }
                countQuery = "SELECT COUNT(*) FROM (" + baseQuery + ")";
                stmt = sourceConn.prepareStatement(countQuery);
                if (config.customQuery.contains("?")) {
                    stmt.setString(1, this.config.targetBranch);
                }
            } else if (config.branchField != null) {
                countQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE " + config.branchField + " = ?";
                stmt = sourceConn.prepareStatement(countQuery);
                stmt.setString(1, this.config.targetBranch);
            } else {
                countQuery = "SELECT COUNT(*) FROM " + tableName;
                stmt = sourceConn.prepareStatement(countQuery);
            }
            
            rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (sourceConn != null) sourceConn.close();
        }
    }
    
    private List<Map<String, Object>> getSourceDataBatch(String tableName, TableConfig config, int offset, int limit) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        Connection sourceConn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            sourceConn = sourceDataSource.getConnection();
            String query;

            if (config.customQuery != null && !config.customQuery.trim().isEmpty()) {
                String baseQuery = config.customQuery;
                if (baseQuery.toUpperCase().contains("ORDER BY")) {
                    baseQuery = baseQuery.substring(0, baseQuery.toUpperCase().lastIndexOf("ORDER BY"));
                }
                query = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (" + baseQuery + " ORDER BY " + config.primaryKey + ") a WHERE ROWNUM <= ?) WHERE rnum > ?";
                stmt = sourceConn.prepareStatement(query);
                int paramIndex = 1;
                if (config.customQuery.contains("?")) {
                    stmt.setString(paramIndex++, this.config.targetBranch);
                }
                stmt.setInt(paramIndex++, offset + limit);
                stmt.setInt(paramIndex, offset);
            } else if (config.branchField != null) {
                query = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM " + tableName + " WHERE " + config.branchField + " = ? ORDER BY " + config.primaryKey + ") a WHERE ROWNUM <= ?) WHERE rnum > ?";
                stmt = sourceConn.prepareStatement(query);
                stmt.setString(1, this.config.targetBranch);
                stmt.setInt(2, offset + limit);
                stmt.setInt(3, offset);
            } else {
                query = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM " + tableName + " ORDER BY " + config.primaryKey + ") a WHERE ROWNUM <= ?) WHERE rnum > ?";
                stmt = sourceConn.prepareStatement(query);
                stmt.setInt(1, offset + limit);
                stmt.setInt(2, offset);
            }

            logDebug("Executing paginated query for table " + tableName + " (offset: " + offset + ", limit: " + limit + ")");

            // Set fetch size for cursor-based retrieval
            stmt.setFetchSize(Math.min(1000, limit));
             
            rs = stmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    // Skip the ROWNUM column we added for pagination
                    if (!"RNUM".equals(columnName.toUpperCase())) {
                        Object value = rs.getObject(i);
                        record.put(columnName, value);
                    }
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
        public int batchSize = 1000;
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
        public String customQuery;
    }

    private static class TableConfig {
        String tableName;
        String primaryKey;
        String branchField;
        String customQuery;

        public TableConfig(String tableName, String primaryKey, String branchField,
                            String customQuery) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
            this.branchField = branchField;
            this.customQuery = customQuery;
        }
    }

    private static class SyncResult {
        int inserted = 0;
        int updated = 0;
        int errors = 0;
    }
}