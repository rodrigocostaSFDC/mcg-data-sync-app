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

package com.salesforce.mcg.datasync.batch.shorturlexport;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.salesforce.mcg.datasync.helper.DateFormatterHelper.format;
/**
 * Tasklet that exports Short URL records incrementally to a CSV file on SFTP.
 * <p>
 * Export Format (CSV with comma delimiter):
 * EMAIL_SMS, USERNAME, APIKEY_ACORTADOR, IDSHORTURL, URL, CREATIONDATE,
 * ID_TRANSACCION_MC, TIPO_ENVIO, ID_TRANSACCION_MIL, ID_TRANSACCION_FEC, CLICKS
 * </p>
 * <p>
 * Export Modes:
 * - Mode 1 (CREATED_ONLY): Export records created since last export (default)
 * - Mode 2 (CREATED_OR_UPDATED): Export records created OR updated since last export
 *   (controlled by EXPORT_UPDATES env var)
 * - Mode 3 (DATE_RANGE): Export records for a specific date/date range (CLI parameter)
 *   Date is specified in Mexico City timezone and converted to UTC for query
 * - Mode 4 (CAMPAIGN_DAY): Export records for a specific campaign (api_key) on a specific day
 *   Uses composite index on (api_key, creation_date) for performant filtering
 * </p>
 * <p>
 * Performance Optimizations:
 * - Streams data directly from database to SFTP without loading into memory
 * - Processes records in batches with progress logging
 * - Handles large datasets (millions of records) efficiently
 * </p>
 * Filename Format: shorturl_export_YYYYMMDD_HHmmssSSSSSS.csv
 */
@Component
@Slf4j
public class ShortUrlExportCSVTasklet implements Tasklet {

    /**
     * Export modes for the Short URL export job.
     */
    public enum ExportMode {
        /** Mode 1: Export records created since last export (default) */
        CREATED_ONLY,
        /** Mode 2: Export records created OR updated since last export */
        CREATED_OR_UPDATED,
        /** Mode 3: Export records for a specific date/date range */
        DATE_RANGE,
        /** Mode 4: Export records for a specific campaign (api_key) on a specific day */
        CAMPAIGN_DAY
    }

    private final DataSource dataSource;
    private final Session sftpSession;
    private final SftpPropertyContext context;

    @Value("${sftp.data.export-dir:/exports}")
    private String exportDirectory;

    @Value("${shorturl.export.batch-size:10000}")
    private int batchSize;
    
    @Value("${EXPORT_UPDATES:false}")
    private boolean exportUpdates;

    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSSSSS");
    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT_OLD = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final ZoneId MEXICO_CITY_ZONE = ZoneId.of("America/Mexico_City");
    private static final String FILE_PREFIX = "shorturl_export_";
    private static final String FILE_PREFIX_DATERANGE = "shorturl_daterange_";
    private static final String FILE_PREFIX_CAMPAIGN = "shorturl_campaign_";
    private static final String FILE_EXTENSION = ".csv";
    private static final String PROD_APP_NAME = "mcg-data-sync-app";
    private static final String STAGING_EXPORT_DIR = "/exports_staging";
    private static final String PROD_EXPORT_DIR = "/exports";
    private static final String SFTP_CHANNEL = "sftp";
    private static final String EXPORT_DATE_START_PARAM = "exportDateStart";
    private static final String EXPORT_DATE_END_PARAM = "exportDateEnd";
    private static final String EXPORT_API_KEY_PARAM = "exportApiKey";
    private static final String DATE_PARAM_PATTERN = "yyyy-MM-dd";
    private static final String TEMP_FILE_PREFIX = FILE_PREFIX + "temp_";
    private static final String DATE_RANGE_SEPARATOR = "_to_";
    private static final int CAMPAIGN_KEY_MAX_LENGTH = 50;
    private static final String DELIMITER = ",";
    
    // CSV headers for export
    private static final String CSV_HEADER = String.join(DELIMITER,
        "EMAIL_SMS", "USERNAME", "APIKEY_ACORTADOR", "IDSHORTURL", "URL", 
        "CREATIONDATE", "ID_TRANSACCION_MC", "TIPO_ENVIO", 
        "ID_TRANSACCION_MIL", "ID_TRANSACCION_FEC", "CLICKS"
    );
    
    public ShortUrlExportCSVTasklet(
            DataSource dataSource,
            Session sftpSession,
            SftpPropertyContext context) {
        this.dataSource = dataSource;
        this.sftpSession = sftpSession;
        this.context = context;
    }

    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            @NonNull ChunkContext chunkContext) throws Exception {

        log.info("🚀 Starting Short URL CSV export (streaming mode)...");
        ExportRequest request = buildExportRequest(chunkContext);
        logExportConfiguration(request);

        LocalDateTime lastExportTime = shouldUseIncrementalLookup(request.mode())
                ? findLastExportTimestamp(exportDirectory)
                : null;

        AtomicLong recordCount = new AtomicLong(0);
        AtomicReference<Timestamp> lastRecordTimestamp = new AtomicReference<>();
        long startTime = System.currentTimeMillis();

        String tempFileName = TEMP_FILE_PREFIX + System.currentTimeMillis() + FILE_EXTENSION;
        String tempFullPath = buildSftpPath(exportDirectory, tempFileName);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            PreparedStatement statement = createPreparedStatement(
                    connection,
                    request.mode(),
                    lastExportTime,
                    request.exportDateStart(),
                    request.exportDateEnd(),
                    request.exportApiKey());

            try (ResultSet rs = statement.executeQuery();
                 PipedOutputStream pipedOut = new PipedOutputStream();
                 PipedInputStream pipedIn = new PipedInputStream(pipedOut, 1024 * 1024)) {

                Thread writerThread = new Thread(() -> {
                    try (PrintWriter writer = new PrintWriter(
                            new OutputStreamWriter(pipedOut, StandardCharsets.UTF_8))) {

                        writer.println(CSV_HEADER);

                        long batchCount = 0;
                        Timestamp lastCreationDate = null;

                        while (rs.next()) {
                            Timestamp creationDate = rs.getTimestamp("creation_date");
                            if (creationDate != null) {
                                lastCreationDate = creationDate;
                                lastRecordTimestamp.set(creationDate);
                            }
                            
                            writer.println(formatCsvRow(rs));
                            recordCount.incrementAndGet();
                            batchCount++;

                            if (batchCount % batchSize == 0) {
                                log.info("Exported {} records so far... (last record timestamp: {})",
                                        recordCount.get(),
                                        lastCreationDate != null ? lastCreationDate.toLocalDateTime() : "N/A");
                                writer.flush();
                            }
                        }

                        writer.flush();
                        log.info("🏁 Finished reading {} records from database. Last record timestamp: {}",
                                recordCount.get(),
                                lastCreationDate != null ? lastCreationDate.toLocalDateTime() : "N/A");

                    } catch (Exception e) {
                        log.error("❌ Error writing CSV data. Error: {}", e.getMessage());
                        throw new RuntimeException("Failed to write CSV data", e);
                    }
                }, "csv-writer-thread");
                
                writerThread.start();
                uploadStreamToSftp(tempFullPath, pipedIn);
                writerThread.join();

                if (recordCount.get() == 0) {
                    log.warn("No new records found since last export");
                    deleteTempFile(tempFullPath);
                    return RepeatStatus.FINISHED;
                }

                String finalFileName = resolveFinalFileName(request, lastRecordTimestamp.get(), tempFileName);
                String finalFullPath = buildSftpPath(exportDirectory, finalFileName);
                renameFileOnSftp(tempFullPath, finalFullPath);

                long duration = System.currentTimeMillis() - startTime;
                log.info("✅ Successfully exported {} records to SFTP: {} (took {} ms, {} records/sec)",
                        recordCount.get(), finalFullPath, duration,
                        duration > 0 ? (recordCount.get() * 1000 / duration) : 0);

                contribution.incrementWriteCount(recordCount.get());
            }
        }

        return RepeatStatus.FINISHED;
    }

    private ExportRequest buildExportRequest(ChunkContext chunkContext) {
        var jobParams = chunkContext.getStepContext().getStepExecution().getJobParameters();
        var exportDateStart = jobParams.getString(EXPORT_DATE_START_PARAM);
        var exportDateEnd = jobParams.getString(EXPORT_DATE_END_PARAM);
        var exportApiKey = jobParams.getString(EXPORT_API_KEY_PARAM);
        var mode = determineExportMode(exportDateStart, exportApiKey);
        return new ExportRequest(mode, exportDateStart, exportDateEnd, exportApiKey);
    }

    private void logExportConfiguration(ExportRequest request) {
        String dateRange = request.exportDateStart() != null
                ? request.exportDateStart() + " to " + request.exportDateEnd()
                : "none";
        String apiKey = request.exportApiKey() != null ? request.exportApiKey() : "none";

        log.info("ℹ️ Export mode: {} (EXPORT_UPDATES={}, dateRange={}, apiKey={})",
                request.mode(), exportUpdates, dateRange, apiKey);
    }

    private boolean shouldUseIncrementalLookup(ExportMode mode) {
        return mode != ExportMode.DATE_RANGE && mode != ExportMode.CAMPAIGN_DAY;
    }

    private String resolveFinalFileName(ExportRequest request, Timestamp lastRecordTimestamp, String tempFileName) {

        if (request.mode() == ExportMode.CAMPAIGN_DAY) {
            String fileName = generateCampaignDayFilename(
                    request.exportApiKey(),
                    request.exportDateStart(),
                    request.exportDateEnd());
            log.info("ℹ️ Campaign day export file: {}", fileName);
            return fileName;
        }

        if (request.mode() == ExportMode.DATE_RANGE) {
            String fileName = generateDateRangeFilename(request.exportDateStart(), request.exportDateEnd());
            log.info("ℹ️ Date range export file: {}", fileName);
            return fileName;
        }

        if (lastRecordTimestamp != null) {
            LocalDateTime lastRecordTime = lastRecordTimestamp.toLocalDateTime();
            String fileTimestamp = lastRecordTime.format(FILE_TIMESTAMP_FORMAT);
            String fileName = FILE_PREFIX + fileTimestamp + FILE_EXTENSION;
            log.info("ℹ️ Renaming export file to reflect last record timestamp ({}): {} -> {}",
                    lastRecordTime, tempFileName, fileName);
            return fileName;
        }

        log.warn("No record timestamps found, using current UTC time for filename");
        LocalDateTime now = LocalDateTime.now(UTC_ZONE);
        String fileTimestamp = now.format(FILE_TIMESTAMP_FORMAT);
        return FILE_PREFIX + fileTimestamp + FILE_EXTENSION;
    }

    private String buildSftpPath(String directory, String fileName) {
        return directory + "/" + fileName;
    }
    
    /**
     * Format a single ResultSet row as a CSV row with comma delimiter.
     * 
     * Columns:
     * EMAIL_SMS, USERNAME, APIKEY_ACORTADOR, IDSHORTURL, URL, CREATIONDATE,
     * ID_TRANSACCION_MC, TIPO_ENVIO, ID_TRANSACCION_MIL, ID_TRANSACCION_FEC, CLICKS
     * 
     * Note: EMAIL_SMS, USERNAME, ID_TRANSACCION_MIL, and ID_TRANSACCION_FEC are
     * wrapped in quotes to ensure they are treated as text in CSV/Excel.
     */
    private String formatCsvRow(ResultSet rs) throws Exception {
        var mobileNumber = rs.getString("mobile_number");
        var email = rs.getString("email");
        var subscriberKey = rs.getString("subscriber_key");
        var apiKey = rs.getString("api_key");
        var shortUrl = rs.getString("short_url");
        var originalUrl = rs.getString("original_url");
        var messageType = rs.getString("message_type");
        var redirectCount = rs.getInt("redirect_count");
        var transactionDate = rs.getString("transaction_date");
        var transactionTime = rs.getString("transaction_time");
        var creationDate = rs.getTimestamp("creation_date");
        var transactionId = rs.getString("transaction_id");
        var emailSmsValue = resolveEmailSmsValue(mobileNumber, email, subscriberKey);

        String idTransaccionMc = Objects.requireNonNullElse(transactionId, Strings.EMPTY);
        String idTransaccionFec = extractFirst14Digits(transactionDate);

        return String.join(DELIMITER,
                asText(emailSmsValue),
                asText(emailSmsValue),
                Objects.requireNonNullElse(apiKey, Strings.EMPTY),
                Objects.requireNonNullElse(shortUrl, Strings.EMPTY),
                Objects.requireNonNullElse(originalUrl, Strings.EMPTY),
                format(creationDate),
                idTransaccionMc,
                extractTipoEnvio(messageType),
                asText(transactionTime),
                asText(idTransaccionFec),
                String.valueOf(redirectCount)
        );
    }

    /**
     * Resolves EMAIL_SMS/USERNAME value for export.
     * At least one of mobileNumber or email must be present (validated at create time).
     * When both are present, subscriberKey indicates the channel: if it equals email use email,
     * if it equals mobileNumber use mobile; otherwise use subscriberKey.
     */
    private String resolveEmailSmsValue(String mobileNumber, String email, String subscriberKey) {
        boolean hasMobile = mobileNumber != null && !mobileNumber.isBlank();
        boolean hasEmail = email != null && !email.isBlank();
        if (hasMobile && !hasEmail) {
            return mobileNumber;
        }
        if (hasEmail && !hasMobile) {
            return email;
        }
        if (hasMobile && hasEmail) {
            String sk = Objects.requireNonNullElse(subscriberKey, Strings.EMPTY).trim();
            String em = Objects.requireNonNullElse(email, Strings.EMPTY).trim();
            String mob = Objects.requireNonNullElse(mobileNumber, Strings.EMPTY).trim();
            if (sk.equalsIgnoreCase(em)) {
                return email;
            }
            if (sk.equals(mob)) {
                return mobileNumber;
            }
            return subscriberKey != null ? subscriberKey : "";
        }
        return "";
    }
    
    /**
     * Extract TIPO_ENVIO: first character of message_type
     * "SMS" -> "S", "Email" -> "E", etc.
     */
    private String extractTipoEnvio(String messageType) {
        String value = Objects.requireNonNullElse(messageType, Strings.EMPTY);
        if (value.isEmpty()) {
            return Strings.EMPTY;
        }
        return value.substring(0, 1).toUpperCase();
    }

    private String extractFirst14Digits(String transactionDate) {
        if (transactionDate == null || transactionDate.isEmpty()) {
            return Strings.EMPTY;
        }
        int dashIndex = transactionDate.indexOf('-');
        if (dashIndex > 0) {
            return transactionDate.substring(0, Math.min(dashIndex, 14));
        }
        return transactionDate.length() > 14 ? transactionDate.substring(0, 14) : transactionDate;
    }
    
//    /**
//     * Safe string - returns empty string if null
//     */
//    private String safe(String value) {
//        return value != null ? value : "";
//    }
    
    /**
     * Formats a value as explicit text for CSV export.
     * Wraps the value in double quotes for proper CSV formatting.
     * 
     * @param value the value to format as text
     * @return the value wrapped in double quotes, or empty quotes if null
     */
    private String asText(String value) {
        if (value == null || value.isEmpty()) {
            return "\"\"";
        }
        // Escape any existing double quotes by doubling them (CSV standard)
        String escaped = value.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }
    
    /**
     * Generates a filename for date range exports (Mode 3).
     * Uses a different prefix than incremental exports to avoid confusion.
     * 
     * Examples:
     * - Single day: shorturl_daterange_20241205.csv
     * - Date range: shorturl_daterange_20241201_to_20241205.csv
     * 
     * @param startDate start date in format yyyy-MM-dd
     * @param endDate end date in format yyyy-MM-dd (can be same as startDate for single day)
     * @return the generated filename
     */
    private String generateDateRangeFilename(String startDate, String endDate) {
        String startCompact = compactDate(startDate);
        String endCompact = (endDate != null && !endDate.isBlank())
                ? compactDate(endDate)
                : startCompact;

        if (startCompact.equals(endCompact)) {
            return FILE_PREFIX_DATERANGE + startCompact + FILE_EXTENSION;
        } else {
            return FILE_PREFIX_DATERANGE + startCompact + DATE_RANGE_SEPARATOR + endCompact + FILE_EXTENSION;
        }
    }
    
    /**
     * Generates a filename for campaign day exports (Mode 4).
     * Includes the api_key (sanitized) and date for easy identification.
     * 
     * Example: shorturl_campaign_myApiKey_20260319.csv
     * 
     * @param apiKey the campaign api_key
     * @param startDate start date in format yyyy-MM-dd
     * @param endDate end date in format yyyy-MM-dd (can be same as startDate for single day)
     * @return the generated filename
     */
    private String generateCampaignDayFilename(String apiKey, String startDate, String endDate) {
        String sanitizedKey = sanitizeApiKeyForFilename(apiKey);
        if (sanitizedKey.length() > CAMPAIGN_KEY_MAX_LENGTH) {
            sanitizedKey = sanitizedKey.substring(0, CAMPAIGN_KEY_MAX_LENGTH);
        }

        String startCompact = compactDate(startDate);
        String endCompact = (endDate != null && !endDate.isBlank())
                ? compactDate(endDate)
                : startCompact;

        if (startCompact.equals(endCompact)) {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + FILE_EXTENSION;
        } else {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + DATE_RANGE_SEPARATOR + endCompact + FILE_EXTENSION;
        }
    }
    
    /**
     * Determines the export mode based on parameters and environment variables.
     * Priority: CAMPAIGN_DAY (if apiKey + date) > DATE_RANGE (if dates) > CREATED_OR_UPDATED > CREATED_ONLY
     */
    private ExportMode determineExportMode(String exportDateStart, String exportApiKey) {
        if (exportApiKey != null && !exportApiKey.isBlank() 
                && exportDateStart != null && !exportDateStart.isBlank()) {
            return ExportMode.CAMPAIGN_DAY;
        }
        if (exportDateStart != null && !exportDateStart.isBlank()) {
            return ExportMode.DATE_RANGE;
        }
        if (exportUpdates) {
            return ExportMode.CREATED_OR_UPDATED;
        }
        return ExportMode.CREATED_ONLY;
    }
    
    /**
     * Creates a PreparedStatement based on the export mode.
     * 
     * @param connection the database connection
     * @param mode the export mode
     * @param lastExportTime the last export timestamp (for Mode 1 and 2)
     * @param exportDateStart the start date for date range export (Mexico City timezone, format: yyyy-MM-dd)
     * @param exportDateEnd the end date for date range export (Mexico City timezone, format: yyyy-MM-dd)
     * @param exportApiKey the api_key to filter by (for Mode 4 CAMPAIGN_DAY)
     * @return the configured PreparedStatement
     */
    private PreparedStatement createPreparedStatement(Connection connection, ExportMode mode, 
            LocalDateTime lastExportTime, String exportDateStart, String exportDateEnd,
            String exportApiKey) throws Exception {

        PreparedStatement statement;

        switch (mode) {
            case CAMPAIGN_DAY -> {
                LocalDateTime[] utcRange = convertMexicoCityDateRangeToUtc(exportDateStart, exportDateEnd);
                LocalDateTime startUtc = utcRange[0];
                LocalDateTime endUtc = utcRange[1];

                log.info("Campaign day export: api_key={}, Mexico City {} to {} -> UTC {} to {}",
                        exportApiKey, exportDateStart, exportDateEnd, startUtc, endUtc);

                statement = prepareForwardOnlyReadOnlyStatement(connection, """
                        SELECT
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        WHERE api_key = ? AND creation_date >= ? AND creation_date < ?
                        ORDER BY creation_date DESC
                        """);
                statement.setString(1, exportApiKey);
                statement.setTimestamp(2, Timestamp.valueOf(startUtc));
                statement.setTimestamp(3, Timestamp.valueOf(endUtc));
            }
            case DATE_RANGE -> {
                LocalDateTime[] utcRange = convertMexicoCityDateRangeToUtc(exportDateStart, exportDateEnd);
                LocalDateTime startUtc = utcRange[0];
                LocalDateTime endUtc = utcRange[1];

                log.info("Date range export: Mexico City {} to {} -> UTC {} to {}",
                        exportDateStart, exportDateEnd, startUtc, endUtc);

                statement = prepareForwardOnlyReadOnlyStatement(connection, """
                        SELECT
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        WHERE creation_date >= ? AND creation_date < ?
                        ORDER BY creation_date
                        """);
                statement.setTimestamp(1, Timestamp.valueOf(startUtc));
                statement.setTimestamp(2, Timestamp.valueOf(endUtc));
            }
            case CREATED_OR_UPDATED -> {
                if (lastExportTime != null) {
                    log.info("Last export found at: {}. Exporting records created OR updated since then.", lastExportTime);
                    statement = prepareForwardOnlyReadOnlyStatement(connection, """
                            SELECT
                                mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                                message_type, redirect_count, transaction_date, transaction_time,
                                transaction_id
                            FROM shorturl_sch.short_url
                            WHERE creation_date > ? OR last_accessed_date > ?
                            ORDER BY creation_date
                            """);
                    statement.setTimestamp(1, Timestamp.valueOf(lastExportTime));
                    statement.setTimestamp(2, Timestamp.valueOf(lastExportTime));
                } else {
                    log.info("No previous export found. Exporting ALL records from the beginning.");
                    statement = prepareForwardOnlyReadOnlyStatement(connection, """
                            SELECT
                                mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                                message_type, redirect_count, transaction_date, transaction_time,
                                transaction_id
                            FROM shorturl_sch.short_url
                            ORDER BY creation_date
                            """);
                }
            }
            default -> {
                if (lastExportTime != null) {
                    log.info("ℹ️ Last export found at: {}. Exporting records created since then.", lastExportTime);
                    statement = prepareForwardOnlyReadOnlyStatement(connection, """
                            SELECT
                                mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                                message_type, redirect_count, transaction_date, transaction_time,
                                transaction_id
                            FROM shorturl_sch.short_url
                            WHERE creation_date > ?
                            ORDER BY creation_date
                            """);
                    statement.setTimestamp(1, Timestamp.valueOf(lastExportTime));
                } else {
                    log.info("ℹ️ No previous export found. Exporting ALL records from the beginning.");
                    statement = prepareForwardOnlyReadOnlyStatement(connection, """
                            SELECT
                                mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                                message_type, redirect_count, transaction_date, transaction_time,
                                transaction_id
                            FROM shorturl_sch.short_url
                            ORDER BY creation_date
                            """);
                }
            }
        }

        return statement;
    }
    
    /**
     * Converts Mexico City date range to UTC timestamps.
     * Input dates are in Mexico City timezone (e.g., "2024-12-05").
     * Returns start of startDate (00:00:00) and end of endDate (23:59:59.999) in UTC.
     * If endDate is null, it defaults to the same as startDate (single day export).
     * 
     * @param startDateStr start date in format yyyy-MM-dd (Mexico City timezone)
     * @param endDateStr end date in format yyyy-MM-dd (Mexico City timezone), can be null
     * @return array of [startUtc, endUtc] as LocalDateTime
     */
    private LocalDateTime[] convertMexicoCityDateRangeToUtc(String startDateStr, String endDateStr) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PARAM_PATTERN);

        LocalDate startDate;
        LocalDate endDate;

        try {
            startDate = LocalDate.parse(startDateStr, dateFormatter);
            endDate = (endDateStr != null && !endDateStr.isBlank())
                    ? LocalDate.parse(endDateStr, dateFormatter)
                    : startDate;
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format. Expected yyyy-MM-dd, got: "
                    + startDateStr + " / " + endDateStr, e);
        }

        ZonedDateTime startMx = startDate.atStartOfDay(MEXICO_CITY_ZONE);
        LocalDateTime startUtc = startMx.withZoneSameInstant(UTC_ZONE).toLocalDateTime();

        ZonedDateTime endMx = endDate.plusDays(1).atStartOfDay(MEXICO_CITY_ZONE);
        LocalDateTime endUtc = endMx.withZoneSameInstant(UTC_ZONE).toLocalDateTime();

        return new LocalDateTime[] { startUtc, endUtc };
    }

    private PreparedStatement prepareForwardOnlyReadOnlyStatement(Connection connection, String sql) throws Exception {
        PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(batchSize);
        return statement;
    }

    private String compactDate(String dateValue) {
        return dateValue.replace("-", "");
    }

    private String sanitizeApiKeyForFilename(String apiKey) {
        return Objects.requireNonNullElse(apiKey, "").replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private String getExportDirectory() {
        String herokuAppName = System.getenv("HEROKU_APP_NAME");
        if (herokuAppName == null || herokuAppName.isBlank()) {
            return STAGING_EXPORT_DIR;
        }
        if (PROD_APP_NAME.equals(herokuAppName)) {
            return PROD_EXPORT_DIR;
        }
        return STAGING_EXPORT_DIR;
    }

    private String extractDirectory(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash > 0 ? path.substring(0, lastSlash) : getExportDirectory();
    }

    private String extractFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash > 0 ? path.substring(lastSlash + 1) : path;
    }
    
    /**
     * Find the timestamp of the last export by looking for existing export files on SFTP
     */
    private LocalDateTime findLastExportTimestamp(String exportDirectory) {
        ChannelSftp channelSftp = null;
        
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel(SFTP_CHANNEL);
            channelSftp.connect();
            
            log.debug("Connected to SFTP to check for previous exports in directory: {}", exportDirectory);
            
            try {
                channelSftp.cd(exportDirectory);
            } catch (SftpException e) {
                log.info("Export directory doesn't exist yet ({}), this is the first export", exportDirectory);
                return null;
            }
            
            // List all files that match our export pattern
            @SuppressWarnings("unchecked")
            java.util.Vector<ChannelSftp.LsEntry> files = channelSftp.ls(FILE_PREFIX + "*" + FILE_EXTENSION);
            
            if (files == null || files.isEmpty()) {
                return null;
            }
            
            LocalDateTime latestTimestamp = null;
            String latestFileName = null;
            
            for (ChannelSftp.LsEntry entry : files) {
                String filename = entry.getFilename();
                
                if (entry.getAttrs().isDir() || filename.equals(".") || filename.equals("..")) {
                    continue;
                }
                
                try {
                    String timestampStr = filename
                        .substring(FILE_PREFIX.length())
                        .replace(FILE_EXTENSION, "");
                    
                    LocalDateTime fileTimestamp = null;
                    boolean isOldFormat = false;
                    
                    try {
                        fileTimestamp = LocalDateTime.parse(timestampStr, FILE_TIMESTAMP_FORMAT);
                    } catch (Exception e1) {
                        try {
                            fileTimestamp = LocalDateTime.parse(timestampStr, FILE_TIMESTAMP_FORMAT_OLD);
                            fileTimestamp = fileTimestamp.withNano(999000000);
                            isOldFormat = true;
                        } catch (Exception e2) {
                            throw new Exception("❌ Failed to parse with both formats", e2);
                        }
                    }
                    
                    if (latestTimestamp == null || fileTimestamp.isAfter(latestTimestamp)) {
                        latestTimestamp = fileTimestamp;
                        latestFileName = filename;
                        if (isOldFormat) {
                            log.debug("❌ Using old format file as latest: {} (adjusted timestamp: {})", filename, latestTimestamp);
                        }
                    }
                } catch (Exception e) {
                    log.warn("❌ Could not parse timestamp from filename: {}. Skipping.", filename);
                }
            }
            
            if (latestTimestamp != null) {
                log.info("📁 Found latest export file: {} with timestamp: {}", latestFileName, latestTimestamp);
            }
            
            return latestTimestamp;
            
        } catch (Exception e) {
            log.warn("❌ Error checking for previous exports: {}. Proceeding with full export.", e.getMessage());
            return null;
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }
    
    /**
     * Upload stream to SFTP
     */
    private void uploadStreamToSftp(String remotePath, PipedInputStream inputStream) throws JSchException, SftpException, IOException {

        ChannelSftp channelSftp = null;
        
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel(SFTP_CHANNEL);
            channelSftp.connect();

            var props = context.getPropertiesForActiveCompany();
            
            log.info("📡 Connected to SFTP server: {}", props.host());
            
            String directory = extractDirectory(remotePath);
            ensureDirectoryExists(channelSftp, directory);
            
            log.info("📡 Starting streaming upload to: {}", remotePath);
            channelSftp.put(inputStream, remotePath);
            
            log.info("📡 File uploaded successfully to: {}", remotePath);
            
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }
    
    /**
     * Ensure directory exists on SFTP
     */
    private void ensureDirectoryExists(ChannelSftp channelSftp, String path) {
        try {
            channelSftp.cd(path);
        } catch (SftpException e) {
            try {
                channelSftp.mkdir(path);
                log.info("✅ Directory created: {}", path);
            } catch (SftpException ex) {
                log.warn("❌ Could not create directory {}: {}", path, ex.getMessage());
            }
        }
    }
    
    /**
     * Rename a file on SFTP
     */
    private void renameFileOnSftp(String oldPath, String newPath) throws JSchException, SftpException {
        ChannelSftp channelSftp = null;
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel(SFTP_CHANNEL);
            channelSftp.connect();
            
            String directory = extractDirectory(oldPath);
            String oldFileName = extractFileName(oldPath);
            String newFileName = extractFileName(newPath);
            
            channelSftp.cd(directory);
            channelSftp.rename(oldFileName, newFileName);
            
            log.info("✅ Renamed file on SFTP: {} ➡️ {}", oldFileName, newFileName);
            
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }
    
    /**
     * Delete a temporary file on SFTP
     */
    private void deleteTempFile(String filePath) {
        ChannelSftp channelSftp = null;
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel(SFTP_CHANNEL);
            channelSftp.connect();
            channelSftp.cd(extractDirectory(filePath));
            channelSftp.rm(extractFileName(filePath));
            log.info("🗑️ Deleted temporary file: {}", filePath);
        } catch (Exception e) {
            log.error("❌️ Could not delete temporary file {}. Error: {}", filePath, e.getMessage());
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }

    private record ExportRequest(
            ExportMode mode,
            String exportDateStart,
            String exportDateEnd,
            String exportApiKey
    ) {
    }
}
