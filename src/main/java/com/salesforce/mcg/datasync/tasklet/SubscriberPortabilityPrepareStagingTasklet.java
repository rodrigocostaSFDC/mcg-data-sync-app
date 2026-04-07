package com.salesforce.mcg.datasync.newbatch.tasklet;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@Slf4j
@AllArgsConstructor
public class SubscriberPortabilityPrepareStagingTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            @NonNull ChunkContext chunkContext)
            throws Exception {

        log.info("🚀 Preparing staging table for bulk loading...");

        try {
            // Step 0: Kill any stuck queries on staging table
            log.info("👣 Step 0: Checking for stuck queries on staging table...");
            try {
                jdbcTemplate.execute("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = current_database()
                        AND state != 'idle'
                        AND query LIKE '%subscriber_portability_staging%'
                        AND pid != pg_backend_pid()
                        """);
                log.info("✅ Terminated any stuck queries on staging table");
            } catch (Exception killError) {
                log.warn("⚠️ Could not kill stuck queries: {}", killError.getMessage());
            }

            // Step 1: Check if staging table exists and is empty
            log.info("👣 Step 1: Checking existing staging table...");
            Long tableExists = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'subscriber_portability_staging' AND table_schema = 'datasync_sch'",
                    Long.class);
            long existsTable = Objects.requireNonNullElse(tableExists, 0L);

            if (existsTable > 0) {
                log.info("ℹ️ Staging table exists, checking if empty...");
                Long recordCount = jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM datasync_sch.subscriber_portability_staging",
                        Long.class);
                long qtyRecords = Objects.requireNonNullElse(recordCount, 0L);
                if (qtyRecords == 0) {
                    log.info("✅ Staging table exists and is empty - no need to recreate");
                } else {
                    log.info("ℹ️ Staging table has {} records, truncating...", recordCount);
                    try {
                        // Use TRUNCATE with timeout protection
                        jdbcTemplate.execute("SET statement_timeout = '30s'");
                        jdbcTemplate.execute("TRUNCATE TABLE datasync_sch.subscriber_portability_staging");
                        jdbcTemplate.execute("SET statement_timeout = '0'");
                        log.info("✅ Staging table truncated successfully");
                    } catch (Exception truncateError) {
                        log.warn("⚠️ TRUNCATE failed, trying DROP instead: {}", truncateError.getMessage());
                        try {
                            jdbcTemplate.execute("DROP TABLE datasync_sch.subscriber_portability_staging CASCADE");
                            log.info("✅ Staging table dropped successfully");
                            // Recreate the table
                            jdbcTemplate.execute("""
                                    CREATE UNLOGGED TABLE datasync_sch.subscriber_portability_staging (
                                        phone_number BIGINT NOT NULL PRIMARY KEY,
                                        operator VARCHAR(50) NOT NULL
                                    )
                                    """);
                            log.info("✅ Staging table recreated successfully");
                        } catch (Exception dropError) {
                            log.error("❌ Both TRUNCATE and DROP failed: {}", dropError.getMessage());
                            throw new RuntimeException("Cannot prepare staging table", dropError);
                        }
                    }
                }
            } else {
                // Step 2: Create unlogged table for better performance (no WAL logging)
                log.info("👣 Step 2: Creating new staging table...");
                jdbcTemplate.execute("""
                        CREATE UNLOGGED TABLE datasync_sch.subscriber_portability_staging (
                            phone_number BIGINT NOT NULL PRIMARY KEY,
                            operator VARCHAR(50) NOT NULL
                        )
                        """);
                log.info("✅ Staging table created successfully");
            }

            // Step 2.5: Primary key automatically creates index for phone_number
            log.info("👣 Step 2.5: Staging table has PRIMARY KEY on phone_number (automatic index)");

            // Step 3: Verify table is ready
            log.info("👣 Step 3: Verifying table is ready...");
            Long finalTableExists = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'subscriber_portability_staging' AND table_schema = 'datasync_sch'",
                    Long.class);
            existsTable = Objects.requireNonNullElse(finalTableExists, 0L);
            if (existsTable > 0) {
                log.info("✅ Staging table verified and ready for bulk loading");
            } else {
                throw new RuntimeException("❌ Failed to create staging table");
            }

        } catch (Exception e) {
            log.error("❌ Error preparing staging table: {}", e.getMessage());
            throw e;
        }

        return org.springframework.batch.repeat.RepeatStatus.FINISHED;
    };
}
