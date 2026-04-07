/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services; loss of use, data, or profits; or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ****************************************************************************/

package com.salesforce.mcg.datasync.tasklet;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Tasklet responsible for validating staging data and promoting it to the production table.
 * This includes data quality checks, deduplication, and atomic promotion of valid records.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SubscriberPortabilityDataPromotionTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            @NonNull ChunkContext chunkContext) throws Exception {

        log.info("🚀 Starting data validation and promotion process");

        long startTime = System.currentTimeMillis();

        try {
            // Step 1: Validate staging data quality
            validateStagingData(contribution);

            // Step 2: Create indexes on staging table for performance
            createStagingIndexes();

            // Step 3: Promote data from staging to production using UPSERT
            promoteDataToProduction(contribution);

            // Step 4: Cleanup staging table
            cleanupStagingTable();

            long endTime = System.currentTimeMillis();
            long durationSeconds = (endTime - startTime) / 1000;
            
            log.info("🏁 Data promotion completed successfully in {} seconds", durationSeconds);

        } catch (Exception e) {
            log.error("❌ Error during data promotion", e);
            throw e;
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * Validate the quality of data in the staging table
     */
    private void validateStagingData(StepContribution contribution) {
        log.info("🧐 Validating staging data quality...");

        // Count total records in staging
        Long totalRecords = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging", 
            Long.class);

        long qtyTotal = Objects.requireNonNullElse(totalRecords, 0L);
        
        log.info("ℹ️ Total records in staging: {}", qtyTotal);

        // Check for null phone numbers
        Long nullPhoneNumbers = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging WHERE phone_number IS NULL", 
            Long.class);

        long qtyNullPhoneNumbers = Objects.requireNonNullElse(nullPhoneNumbers, 0L);
        
        if (qtyNullPhoneNumbers > 0) {
            log.warn("⚠️ Found {} records with null phone numbers", qtyNullPhoneNumbers);
            // Remove invalid records
            int deletedRecords = jdbcTemplate.update(
                "DELETE FROM datasync_sch.subscriber_portability_staging WHERE phone_number IS NULL");
            log.info("🔧 Removed {} records with null phone numbers", deletedRecords);
        }

        // Check for null operators
        Long nullOperators = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging WHERE operator IS NULL OR operator = ''", 
            Long.class);

        long qtyNullOperators = Objects.requireNonNullElse(nullOperators, 0L);

        if (qtyNullOperators > 0) {
            log.warn("⚠️ Found {} records with null/empty operators", nullOperators);
            // Remove invalid records
            int deletedRecords = jdbcTemplate.update(
                "DELETE FROM datasync_sch.subscriber_portability_staging WHERE operator IS NULL OR operator = ''");
            log.info("🔧 Removed {} records with null/empty operators", deletedRecords);
        }

        // Portability date validation removed - dates are used for filtering only, not stored

        // Count valid records after cleanup
        Long validRecords = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging", 
            Long.class);
        
        log.info("✅ Valid records after cleanup: {}", validRecords);
        long qtyValid = Objects.requireNonNullElse(validRecords, 0L);
        contribution.incrementFilterCount(qtyTotal - qtyValid);
    }

    /**
     * Create indexes on staging table for better performance during promotion
     */
    private void createStagingIndexes() {
        log.info("🚀 Creating indexes on staging table for performance...");

        try {
            // Create index on phone_number for faster lookups during UPSERT
            jdbcTemplate.execute(
                "CREATE INDEX IF NOT EXISTS idx_staging_phone_number " +
                "ON datasync_sch.subscriber_portability_staging (phone_number)");
            
            log.info("✅ Staging indexes created successfully");
        } catch (Exception e) {
            log.warn("❌ Error creating staging indexes (may already exist): {}", e.getMessage());
        }
    }

    /**
     * Promote validated data from staging to production table using optimized bulk UPSERT
     */
    private void promoteDataToProduction(StepContribution contribution) {
        log.info("🚀 Promoting data from staging to production using PostgreSQL UPSERT...");

        long startTime = System.currentTimeMillis();

        // Check if this is first load (empty production table) for optimization
        Long productionCount = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM datasync_sch.subscriber_portability", Long.class);

        long qtyRecords = Objects.requireNonNullElse(productionCount, 0L);

        boolean isFirstLoad = qtyRecords == 0;
        
        if (isFirstLoad) {
            log.info("🚀 First load detected - using optimized INSERT-only approach");
            promoteFirstLoad(contribution, startTime);
        } else {
            log.info("🔄 Incremental load detected - using UPSERT approach");
            promoteIncrementalLoad(contribution, startTime);
        }
    }

    /**
     * Optimized promotion for first load (INSERT only, no conflicts)
     */
    private void promoteFirstLoad(StepContribution contribution, long startTime) {
        try {
            // First load optimizations
            log.info("🚀️ Applying first load optimizations...");
            
            // Only use parameters that are safe for Heroku PostgreSQL
            boolean workMemSet = false;
            boolean effectiveCacheSizeSet = false;
            
            try {
                // 1. Increase work_mem for better sorting/hashing (if allowed)
                log.info("Attempting to set work_mem to 256MB...");
                jdbcTemplate.execute("SET work_mem = '256MB'");
                workMemSet = true;
                log.info("✅ work_mem increased to 256MB");
            } catch (Exception e) {
                log.warn("⚠️ work_mem setting not allowed: {}", e.getMessage());
            }
            
            try {
                // 2. Set effective_cache_size for better query planning (if allowed)
                log.info("Attempting to set effective_cache_size to 1GB...");
                jdbcTemplate.execute("SET effective_cache_size = '1GB'");
                effectiveCacheSizeSet = true;
                log.info("✅ effective_cache_size set to 1GB");
            } catch (Exception e) {
                log.warn("⚠️ effective_cache_size setting not allowed: {}", e.getMessage());
            }
            
            // 4. Use simple INSERT for first load - no batching needed since no conflicts
            log.info("Step 4: Preparing INSERT operation...");
            
            // Check staging table count first
            Long stagingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging", 
                Long.class);

            long qtyStaging = Objects.requireNonNullElse(stagingCount, 0L);

            log.info("Staging table contains {} records", qtyStaging);
            
            if (qtyStaging == 0) {
                log.warn("⚠️ Staging table is empty - nothing to insert");
                return;
            }
            
            String insertSQL = """
                INSERT INTO datasync_sch.subscriber_portability (phone_number, operator)
                SELECT phone_number, operator
                FROM datasync_sch.subscriber_portability_staging
                """;
            
            log.info("Step 5: Executing INSERT operation...");
            log.info("INSERT SQL: {}", insertSQL);

            int insertedRecords;
            try {
                // Set timeout for INSERT operation (10 minutes for large initial load)
                jdbcTemplate.execute("SET statement_timeout = '600s'");
                log.info("Statement timeout set to 10 minutes");
                
                insertedRecords = jdbcTemplate.update(insertSQL);
                log.info("✅ First load INSERT completed: {} records processed", insertedRecords);
                
                // Reset timeout
                jdbcTemplate.execute("SET statement_timeout = '0'");
                log.info("Statement timeout reset");
                
            } catch (Exception insertError) {
                log.error("❌ INSERT operation failed: {}", insertError.getMessage());
                // Reset timeout even on error
                try {
                    jdbcTemplate.execute("SET statement_timeout = '0'");
                } catch (Exception ignored) {}
                throw insertError;
            }
            
            // Clean up session parameters
            log.info("🧹 Cleaning up session parameters...");
            if (workMemSet) {
                try {
                    jdbcTemplate.execute("SET work_mem = '4MB'"); // Reset to default
                    log.info("✅ work_mem reset to default");
                } catch (Exception e) {
                    log.warn("⚠️ Could not reset work_mem: {}", e.getMessage());
                }
            }
            
            if (effectiveCacheSizeSet) {
                try {
                    jdbcTemplate.execute("SET effective_cache_size = '128MB'"); // Reset to default
                    log.info("✅ effective_cache_size reset to default");
                } catch (Exception e) {
                    log.warn("⚠️ Could not reset effective_cache_size: {}", e.getMessage());
                }
            }
            
            long endTime = System.currentTimeMillis();
            long durationSeconds = (endTime - startTime) / 1000;
            
            log.info("🏁 First load completed: {} records in {} seconds", insertedRecords, durationSeconds);
            contribution.incrementWriteCount(insertedRecords);
            
            // Performance monitoring for first load
            if (durationSeconds < 15) {
                log.info("✅ First load performance: Excellent (under 15 seconds)");
            } else if (durationSeconds < 30) {
                log.info("✅ First load performance: Good (under 30 seconds)");
            } else {
                log.warn("⚠️ First load performance: May need optimization (over 30 seconds)");
            }
            
        } catch (Exception e) {
            // Clean up session parameters on error
            log.info("🧹 Cleaning up session parameters due to error...");
            try {
                jdbcTemplate.execute("SET work_mem = '4MB'");
                jdbcTemplate.execute("SET effective_cache_size = '128MB'");
                log.info("✅ Session parameters reset to defaults");
            } catch (Exception cleanupError) {
                log.warn("⚠️ Could not reset session parameters: {}", cleanupError.getMessage());
            }
            throw e;
        }
    }

    /**
     * Optimized promotion for incremental loads (UPSERT with conflicts)
     */
    private void promoteIncrementalLoad(StepContribution contribution, long startTime) {
        // Use PostgreSQL ON CONFLICT DO UPDATE for incremental loads
        log.info("🚀 Executing PostgreSQL UPSERT operation...");
        String upsertSQL = """
            INSERT INTO datasync_sch.subscriber_portability (phone_number, operator)
            SELECT phone_number, operator
            FROM datasync_sch.subscriber_portability_staging
            ON CONFLICT (phone_number)
            DO UPDATE SET
                operator = EXCLUDED.operator
            WHERE subscriber_portability.operator != EXCLUDED.operator
            """;
        
        int upsertedRecords = jdbcTemplate.update(upsertSQL);
        log.info("💾 UPSERT completed: {} records processed", upsertedRecords);
        
        long endTime = System.currentTimeMillis();
        long durationSeconds = (endTime - startTime) / 1000;
        
        log.info("🏁 Incremental load completed: {} records in {} seconds", upsertedRecords, durationSeconds);
        contribution.incrementWriteCount(upsertedRecords);
        
        // Performance monitoring for incremental load
        if (durationSeconds < 30) {
            log.info("✅ Incremental load performance: Excellent (under 30 seconds)");
        } else if (durationSeconds < 60) {
            log.info("✅ Incremental load performance: Good (under 1 minute)");
        } else {
            log.warn("⚠️ Incremental load performance: May need optimization (over 1 minute)");
        }
    }


    /**
     * Clean up the staging table after successful promotion
     */
    private void cleanupStagingTable() {
        log.info("🗑️ Cleaning up staging table...");

        try {
            // Drop indexes first
            jdbcTemplate.execute("DROP INDEX IF EXISTS datasync_sch.idx_staging_phone_number");
            
            // Truncate staging table
            jdbcTemplate.execute("TRUNCATE TABLE datasync_sch.subscriber_portability_staging");
            
            log.info("Staging table cleanup completed");
        } catch (Exception e) {
            log.warn("Error during staging cleanup: {}", e.getMessage());
        }
    }
}
