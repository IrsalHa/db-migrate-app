-- Database Optimization Script for db-merge Application
-- This script contains recommended indexes and optimizations for improved performance

-- =============================================================================
-- PRIMARY KEY INDEXES (Usually created automatically, but verify they exist)
-- =============================================================================

-- Example for common table structures - replace with your actual table names
-- ALTER TABLE your_table_name ADD CONSTRAINT pk_your_table PRIMARY KEY (id);

-- =============================================================================
-- COMPOSITE INDEXES FOR BATCH OPERATIONS
-- =============================================================================

-- If you have tables with frequent lookups on multiple columns during sync
-- Example: CREATE INDEX idx_table_composite ON your_table (primary_key, last_modified);

-- =============================================================================
-- INDEXES FOR EXISTENCE CHECKS (used by getExistingRecords method)
-- =============================================================================

-- These indexes optimize the IN clause queries used for batch existence checks
-- Replace 'your_table_name' and 'primary_key_column' with actual names

-- Example for a users table:
-- CREATE INDEX idx_users_id_lookup ON users (user_id);

-- Example for a products table:
-- CREATE INDEX idx_products_id_lookup ON products (product_id);

-- Example for an orders table:
-- CREATE INDEX idx_orders_id_lookup ON orders (order_id);

-- =============================================================================
-- COVERING INDEXES FOR SELECT OPERATIONS
-- =============================================================================

-- Covering indexes include all columns needed for a query to avoid table lookups
-- This is especially useful for the getSourceDataBatch method

-- Example covering index (replace with your actual columns):
-- CREATE INDEX idx_table_covering ON your_table (primary_key) 
--   INCLUDE (column1, column2, column3, last_modified);

-- =============================================================================
-- INDEXES FOR PAGINATION (OFFSET/LIMIT operations)
-- =============================================================================

-- These indexes optimize the pagination queries used in parallel processing
-- The order is crucial for OFFSET/LIMIT performance

-- Example for Oracle (using ROWNUM):
-- CREATE INDEX idx_table_pagination ON your_table (primary_key, ROWID);

-- Example for other databases:
-- CREATE INDEX idx_table_order ON your_table (primary_key ASC);

-- =============================================================================
-- STATISTICS UPDATE (Important for query optimizer)
-- =============================================================================

-- Oracle:
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('schema_name', 'table_name');

-- SQL Server:
-- UPDATE STATISTICS table_name;

-- MySQL:
-- ANALYZE TABLE table_name;

-- PostgreSQL:
-- ANALYZE table_name;

-- =============================================================================
-- PERFORMANCE MONITORING QUERIES
-- =============================================================================

-- Oracle - Check index usage:
-- SELECT * FROM V$SQL_PLAN WHERE OBJECT_NAME = 'your_index_name';

-- SQL Server - Check index usage:
-- SELECT * FROM sys.dm_db_index_usage_stats 
-- WHERE object_id = OBJECT_ID('your_table_name');

-- MySQL - Check index usage:
-- SHOW INDEX FROM your_table_name;

-- PostgreSQL - Check index usage:
-- SELECT * FROM pg_stat_user_indexes WHERE relname = 'your_table_name';

-- =============================================================================
-- ADDITIONAL OPTIMIZATIONS
-- =============================================================================

-- 1. Ensure AUTO_INCREMENT/SEQUENCE values are properly set
-- 2. Consider partitioning for very large tables
-- 3. Regular maintenance: REINDEX, UPDATE STATISTICS
-- 4. Monitor query execution plans
-- 5. Consider materialized views for complex aggregations

-- =============================================================================
-- NOTES FOR IMPLEMENTATION
-- =============================================================================

/*
To implement these optimizations:

1. Replace placeholder names with your actual table and column names
2. Test index creation on a development environment first
3. Monitor performance before and after index creation
4. Consider the trade-off: indexes speed up SELECT but slow down INSERT/UPDATE
5. For batch operations, the performance gain usually outweighs the cost
6. Use database-specific syntax for your target database system

Recommended approach:
1. Identify your most frequently synchronized tables
2. Create indexes for primary keys first
3. Add covering indexes for tables with many columns
4. Monitor performance and adjust as needed

The batch operations implemented in the Java code will benefit significantly
from proper indexing, especially:
- getExistingRecords() method uses IN clauses
- batchInsertRecords() and batchUpdateRecords() use primary key lookups
- Pagination queries in getSourceDataBatch() use OFFSET/LIMIT
*/