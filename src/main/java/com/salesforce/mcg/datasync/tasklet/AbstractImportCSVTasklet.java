package com.salesforce.mcg.datasync.newbatch.tasklet;


import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.datasync.newbatch.data.ImportContext;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import jakarta.annotation.Resource;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class AbstractImportCSVTasklet<T> implements Tasklet {

    public static final String FILE_NAME = "fileName";
    public static final String SFTP = "sftp";
    public static final String COMMA = ",";

    public static final String READ_COUNT = "readCount";
    public static final String WRITE_COUNT = "writeCount";
    public static final String PROCESS_SKIP_COUNT = "skipCount";

    @Resource
    private Session session;

    @Resource
    private DataSource dataSource;

    @Resource
    private SftpPropertyContext propertyContext;

    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            ChunkContext chunkContext)
            throws Exception {

        // Get file names from job parameters (comma-separated list)
        var fileNamesParam = getFileNameFromContext(chunkContext);
        var props = propertyContext.getPropertiesForActiveCompany();

        var files = fileNamesParam.split(COMMA);

        log.info("🚀 Starting grouped job for {} files: {}", files.length, fileNamesParam);

        long startTime = System.currentTimeMillis();

        ImportContext context = new ImportContext(
                new AtomicLong(0),
                new AtomicLong(0),
                new AtomicLong(0),
                new AtomicLong(0)
        );

        ChannelSftp sftp = null;
        try (Connection connection = dataSource.getConnection()) {

            sftp = (ChannelSftp) session.openChannel(SFTP);
            sftp.connect();
            connection.setAutoCommit(false);

            // Process each file sequentially
            for (String fileName : files) {
                var trimmedFileName = fileName.trim();
                log.info("📂 Processing file: {}", trimmedFileName);

                // Handle both full paths and relative filenames
                var remoteDir = props.outputDir();
                var filePath = trimmedFileName.startsWith("/") ? trimmedFileName : remoteDir + "/" + trimmedFileName;

                try (InputStream sftpStream = sftp.get(filePath);

                     BufferedReader reader = new BufferedReader(new InputStreamReader(
                             sftpStream,
                             StandardCharsets.UTF_8));

                     PreparedStatement ps = connection.prepareStatement(insertSQL())) {

                    String line;
                    AtomicInteger batchSize = new AtomicInteger(0);
                    final int BATCH_COMMIT_SIZE = 1000;
                    boolean isFirstLine = true;
                    Map<String, Integer> columnMap = null;

                    while ((line = reader.readLine()) != null) {

                        // Parse header line to create column mapping
                        if (isFirstLine) {
                            isFirstLine = false;
                            columnMap = parseHeader(line);
                            log.info("ℹ️ Header line for {}: {}", trimmedFileName, line);
                            log.info("ℹ️ Column mapping for {}: {}", trimmedFileName, columnMap);
                            continue;
                        }

                        // Only count data lines, not header lines
                        context.totalRecord().incrementAndGet();

                        try {
                            T operator = parseLine(
                                    fileName,
                                    line,
                                    context.totalRecord().get(),
                                    columnMap);

                            if (operator != null) {
                                prepareStatement(operator, ps, batchSize);
                                context.processedRecord().incrementAndGet();
                                if (batchSize.get() >= BATCH_COMMIT_SIZE) {
                                    commitBatch(ps, connection, context, batchSize);
                                    batchSize = new AtomicInteger(0);
                                    log.debug("💾 Committed batch of {} records", BATCH_COMMIT_SIZE);
                                }
                            } else {
                                context.skippedRecords().incrementAndGet();
                            }
                        } catch (Exception e) {
                            context.errorRecords().incrementAndGet();
                            log.warn("❌ Error processing line {}: {} - Error: {}",
                                    context.totalRecord().get(), line, e.getMessage());
                        }
                    }

                    // Execute remaining batch
                    if (batchSize.get() > 0) {
                        commitBatch(ps, connection, context, batchSize);
                        log.debug("ℹ️ Committed final batch of {} records", batchSize);
                    }

                    log.info("🏁 Completed processing file: {} - Records: {}",
                            trimmedFileName, context.processedRecord().get());

                } catch (Exception e) {
                    log.error("❌ Error processing file: {}", trimmedFileName, e);
                    connection.rollback();
                    throw e;
                }
            }
        } finally {
            if (sftp != null && sftp.isConnected()) {
                sftp.disconnect();
            }
        }

        long endTime = System.currentTimeMillis();
        long durationSeconds = (endTime - startTime) / 1000;

        log.info("ℹ️ Grouped job completed in {} seconds ({} minutes)",
                durationSeconds, durationSeconds / 60);
        log.info("ℹ️ Total records: {}, Processed: {}, Skipped: {}, Errors: {}",
                context.totalRecord().get(),
                context.processedRecord().get(),
                context.skippedRecords().get(),
                context.errorRecords().get());

        updateStatistics(contribution, context);

        return RepeatStatus.FINISHED;
    }

    private void commitBatch(
            PreparedStatement ps,
            Connection connection,
            ImportContext context,
            AtomicInteger batchSize) throws SQLException {
        ps.executeBatch();
        connection.commit();
        context.processedRecord().addAndGet(batchSize.get());
        batchSize = new AtomicInteger(0);
    }

    public abstract String insertSQL();

    public abstract T parseLine(
            String fileName,
            String line,
            long lineNumber,
            Map<String, Integer> columnMap) throws Exception;

    public abstract void prepareStatement(
            T record,
            PreparedStatement ps,
            AtomicInteger batchSize) throws SQLException ;

    public static void updateStatistics(
            @NonNull StepContribution contribution,
            ImportContext context){

        // Update step contribution with statistics efficiently
        try {
            // Use reflection to set the counts directly for better performance
            Field readCountField = contribution.getClass().getDeclaredField(READ_COUNT);
            readCountField.setAccessible(true);
            readCountField.set(contribution, context.errorRecords().get());

            Field writeCountField = contribution.getClass().getDeclaredField(WRITE_COUNT);
            writeCountField.setAccessible(true);
            writeCountField.set(contribution, context.errorRecords().get());

            Field processSkipCountField = contribution.getClass().getDeclaredField(PROCESS_SKIP_COUNT);
            processSkipCountField.setAccessible(true);
            processSkipCountField.set(contribution, context.skippedRecords().get());

            log.info("ℹ️ Updated Spring Batch statistics: Read={}, Write={}, ProcessSkip={}",
                    context.processedRecord().get(),
                    context.totalRecord().get(),
                    context.skippedRecords().get());

        } catch (Exception reflectionError) {

            log.warn("❌ Could not update Spring Batch statistics via reflection: {}", reflectionError.getMessage());
            // Fallback to increment methods (less efficient but functional)
            contribution.incrementWriteCount(context.processedRecord().get());
            log.info("❌ Fallback statistics update: Write={}, Actual Read={}, ProcessSkip={}",
                    context.processedRecord().get(),
                    context.totalRecord().get(),
                    context.skippedRecords().get());
        }
    }

    /**
     * Parse CSV header line to create column name to index mapping
     */
    public static  Map<String, Integer> parseHeader(String headerLine) {
        Map<String, Integer> columnMap = new HashMap<>();
        String[] headers = headerLine.split(COMMA, -1);

        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i].trim().toUpperCase();
            columnMap.put(columnName, i);
        }

        return columnMap;
    }

    /**
     * Get column value by index from parsed CSV values, handling null/empty values
     */
    public static String getColumnValue(String[] values, Integer index) {
        if (index == null || index >= values.length) {
            return null;
        }
        String value = values[index];
        return (value == null || value.trim().isEmpty()) ? null : value.trim();
    }

    public static String getFileNameFromContext(ChunkContext context){
        return  context
                .getStepContext()
                .getJobParameters()
                .get(FILE_NAME)
                .toString();
    }

}
