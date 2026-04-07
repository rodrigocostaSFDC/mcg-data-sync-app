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

@Component
@Slf4j
@AllArgsConstructor
public class SubscriberPortabilityCleanupStepTasklet implements Tasklet {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            @NonNull ChunkContext chunkContext) throws Exception {

        log.info("🚀 Performing final cleanup...");

        // Drop staging table
        jdbcTemplate.execute("DROP TABLE IF EXISTS datasync_sch.subscriber_portability_staging CASCADE");

        // Update table statistics for query optimizer
        jdbcTemplate.execute("ANALYZE datasync_sch.subscriber_portability");

        // Log final statistics
        Long totalRecords = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM datasync_sch.subscriber_portability", Long.class);

        log.info("✅ Job completed successfully. Total records in production: {}", totalRecords);

        return org.springframework.batch.repeat.RepeatStatus.FINISHED;
    }
}
