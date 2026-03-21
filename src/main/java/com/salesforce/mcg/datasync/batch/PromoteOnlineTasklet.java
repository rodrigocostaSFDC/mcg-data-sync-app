package com.salesforce.mcg.datasync.batch;// PromoteOnlineTasklet.java

import jakarta.annotation.Resource;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class PromoteOnlineTasklet implements Tasklet {

    @Resource
    private JdbcTemplate jdbc;

    @Override
    public RepeatStatus execute(StepContribution c, ChunkContext ctx) {
        String live = null;
        String candidate = null;
        try {
            Map<String, Object> parameters = ctx.getStepContext().getJobParameters();
            live = (String) parameters.get("live");
            candidate = (String) parameters.get("candidate");
        } catch (Exception e) {
            throw new RuntimeException("Failed to promote online tasklet. Unable to get live and candidate table names from Job execution context.", e);
        }

        String backup = live + "_old_" + System.currentTimeMillis();
        String sql = String.format("BEGIN; ALTER TABLE %S RENAME TO %S; ALTER TABLE %S RENAME TO %S; ANALYZE %s; COMMIT;",
                live, backup.split("\\.")[1],
                candidate, live.split("\\.")[1],
                live);
        jdbc.execute(sql);
        return RepeatStatus.FINISHED;
    }
}
