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
 *****************************************************************************/

package com.salesforce.mcg.datasync.newbatch.config;

import com.salesforce.mcg.datasync.batch.shorturlexport.ShortUrlExportCSVTasklet;
import com.salesforce.mcg.datasync.newbatch.tasklet.*;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Configuration class for Operator batch job.
 * Defines all necessary beans for reading, processing, and writing Operator records in a Spring Batch job.
 */
@Configuration
@RequiredArgsConstructor
public class JobConfig {

    public static final String SHEET_IMPORT_CSV_STEP = "sheet-import-csv-step";
    public static final String SHEET_IMPORT_CSV_JOB = "sheet-import-csv-job";

    public static final String OPERATOR_IMPORT_CSV_STEP = "operator-import-csv-step";
    public static final String OPERATOR_IMPORT_CSV_JOB = "operator-import-csv-job";

    public static final String SUBSCRIBER_SERIES_IMPORT_CSV_STEP = "subscriber-series-import-csv-step";
    public static final String SUBSCRIBER_SERIES_IMPORT_CSV_JOB =  "subscriber-series-import-csv-job";

    public static final String SHORT_URL_EXPORT_CSV_STEP = "short-url-export-csv-step";
    public static final String SHORT_URL_EXPORT_CSV_JOB = "short-url-export-csv-job";

    public static final String SUBSCRIBER_PORTABILITY_PREPARE_STEP = "subscriber-portability-prepare-staging-step";
    public static final String SUBSCRIBER_PORTABILITY_BULK_COPY_STEP = "subscriber-portability-bulk-copy-step";
    public static final String SUBSCRIBER_PORTABILITY_PROMOTION_STEP = "subscriber-portability-data-promotion-step";
    public static final String SUBSCRIBER_PORTABILITY_CLEANUP_STEP = "subscriber-portability-data-cleanup-step";

    public static final String SUBSCRIBER_PORTABILITY_PREPARE_JOB = "subscriber-portability-export-csv-job";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate jdbcTemplate;

    @Bean(name = "sheetImportCSVJob")
    public Job sheetImportCSVJob(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SheetImportCSVImportCSVTasklet tasklet
    ) {
        Step step = new StepBuilder(SHEET_IMPORT_CSV_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
        return new JobBuilder(SHEET_IMPORT_CSV_JOB, jobRepository)
                .start(step)
                .build();
    }

    @Bean(name = "operatorImportCSVJob")
    public Job operatorImportCSVJob(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            OperatorImportCSVImportCSVTasklet tasklet) {
        Step step = new StepBuilder(OPERATOR_IMPORT_CSV_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                //.listener(stepExecutionListener)
                .build();
        return new JobBuilder(OPERATOR_IMPORT_CSV_JOB, jobRepository)
                .start(step)
                .build();
    }

    @Bean(name = "subscriberSeriesImportCSVJob")
    public Job subscriberSeriesImportCSVJob(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SubscriberSeriesImportCSVTasklet tasklet) {
        Step step = new StepBuilder(SUBSCRIBER_SERIES_IMPORT_CSV_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                //.listener(stepExecutionListener)
                .build();
        return new JobBuilder(SUBSCRIBER_SERIES_IMPORT_CSV_JOB, jobRepository)
                .start(step)
                .build();
    }

    /**
     * Main job that processes subscriber portability data efficiently
     */
    @Bean(name = "subscriberPortabilityJobBean")
    public Job subscriberPortabilityJob(
            SubscriberPortabilityPrepareStagingTasklet prepareTasklet,
            SubscriberPortabilityPostgresCopyTasklet copyTasklet,
            SubscriberPortabilityDataPromotionTasklet promotionTasklet,
            SubscriberPortabilityCleanupStepTasklet cleanupTasklet) {

        Step prepareStep = new StepBuilder(SUBSCRIBER_PORTABILITY_PREPARE_STEP, jobRepository)
                .tasklet(prepareTasklet, transactionManager).build();
        Step copyStep = new StepBuilder(SUBSCRIBER_PORTABILITY_BULK_COPY_STEP, jobRepository)
                .tasklet(copyTasklet, transactionManager).build();
        Step promotionStep = new StepBuilder(SUBSCRIBER_PORTABILITY_PROMOTION_STEP, jobRepository)
                .tasklet(promotionTasklet, transactionManager).build();
        Step cleanupStep = new StepBuilder(SUBSCRIBER_PORTABILITY_CLEANUP_STEP, jobRepository)
                .tasklet(cleanupTasklet, transactionManager).build();

        return new JobBuilder(SUBSCRIBER_PORTABILITY_PREPARE_JOB, jobRepository)
                .start(prepareStep)
                .next(copyStep)
                .next(promotionStep)
                .next(cleanupStep)
                .build();
    }

    /**
     * Step 1: Prepare staging table for bulk loading
     */
    @Bean
    public Step subscriberPortabilityPrepareStagingStep(SubscriberPortabilityPrepareStagingTasklet tasklet) {
        return new StepBuilder("subscriber-portability-prepare-staging-step", jobRepository)
                .tasklet(tasklet, transactionManager).build();
    }

    /**
     * Step 2: Bulk copy data from SFTP directly to PostgreSQL using COPY command
     */
    @Bean
    public Step subscriberPortabilityBulkCopyStep(SubscriberPortabilityPrepareStagingTasklet tasklet) {
        return new StepBuilder("subscriber-portability-bulk-copy-step", jobRepository)
                .tasklet(tasklet, transactionManager).build();
    }

    /**
     * Step 3: Validate and promote data from staging to production
     */
    @Bean
    public Step subscriberPortabilityDataPromotionStep(SubscriberPortabilityDataPromotionTasklet tasklet) {
        return new StepBuilder("subscriber-portability-data-promotion-step", jobRepository)
                .tasklet(tasklet, transactionManager).build();
    }

    /**
     * Step 3: Validate and promote data from staging to production
     */
    @Bean
    public Step subscriberPortabilityCleanupStep() {
        return new StepBuilder("subscriber-portability-data-cleanup-step", jobRepository)
                .tasklet(new SubscriberPortabilityCleanupStepTasklet(jdbcTemplate),
                        transactionManager)
                .build();
    }

    @Bean(name = "shortUrlExportCSVJob")
    public Job shortUrlExportCSVJob(ShortUrlExportCSVTasklet tasklet) {
        Step step = new StepBuilder(SHORT_URL_EXPORT_CSV_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                //.listener(stepExecutionListener)
                .build();
        return new JobBuilder(SHORT_URL_EXPORT_CSV_JOB, jobRepository)
                .start(step)
                .build();
    }

}