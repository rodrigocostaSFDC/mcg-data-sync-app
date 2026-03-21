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
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tasklet that exports Short URL records incrementally to a CSV file on SFTP.
 * 
 * Export Format (CSV with comma delimiter):
 * EMAIL_SMS, USERNAME, APIKEY_ACORTADOR, IDSHORTURL, URL, CREATIONDATE,
 * ID_TRANSACCION_MC, TIPO_ENVIO, ID_TRANSACCION_MIL, ID_TRANSACCION_FEC, CLICKS
 * 
 * Export Modes:
 * - Mode 1 (CREATED_ONLY): Export records created since last export (default)
 * - Mode 2 (CREATED_OR_UPDATED): Export records created OR updated since last export
 *   (controlled by EXPORT_UPDATES env var)
 * - Mode 3 (DATE_RANGE): Export records for a specific date/date range (CLI parameter)
 *   Date is specified in Mexico City timezone and converted to UTC for query
 * - Mode 4 (CAMPAIGN_DAY): Export records for a specific campaign (api_key) on a specific day
 *   Uses composite index on (api_key, creation_date) for performant filtering
 * 
 * Performance Optimizations:
 * - Streams data directly from database to SFTP without loading into memory
 * - Processes records in batches with progress logging
 * - Handles large datasets (millions of records) efficiently
 * 
 * Filename Format: shorturl_export_YYYYMMDD_HHmmssSSSSSS.csv
 *
 * @author AI Generated
 * @since 2024-11-24
 */
@Component
@Slf4j
public class ShortUrlExportTasklet implements Tasklet {

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
    
    @Value("${sftp.data.export-dir:/exports}")
    private String exportDir;
    
    @Value("${sftp.data.host}")
    private String sftpHost;
    
    @Value("${shorturl.export.batch-size:10000}")
    private int batchSize;
    
    @Value("${EXPORT_UPDATES:false}")
    private boolean exportUpdates;
    
    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSSSSS");
    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT_OLD = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter CSV_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final ZoneId MEXICO_CITY_ZONE = ZoneId.of("America/Mexico_City");
    private static final String FILE_PREFIX = "shorturl_export_";
    private static final String FILE_PREFIX_DATERANGE = "shorturl_daterange_";
    private static final String FILE_PREFIX_CAMPAIGN = "shorturl_campaign_";
    private static final String FILE_EXTENSION = ".csv";
    private static final String PROD_APP_NAME = "mcg-data-sync-app";
    private static final String STAGING_EXPORT_DIR = "/exports_staging";
    private static final String PROD_EXPORT_DIR = "/exports";
    private static final String DELIMITER = ",";
    
    // CSV headers for export
    private static final String CSV_HEADER = String.join(DELIMITER,
        "EMAIL_SMS", "USERNAME", "APIKEY_ACORTADOR", "IDSHORTURL", "URL", 
        "CREATIONDATE", "ID_TRANSACCION_MC", "TIPO_ENVIO", 
        "ID_TRANSACCION_MIL", "ID_TRANSACCION_FEC", "CLICKS"
    );
    
    public ShortUrlExportTasklet(DataSource dataSource, Session sftpSession) {
        this.dataSource = dataSource;
        this.sftpSession = sftpSession;
    }
    
    /**
     * Determines the export directory based on Heroku app name.
     * Production (mcg-data-sync-app) uses /exports, staging uses /exports_staging.
     */
    private String getExportDirectory() {
        String herokuAppName = System.getenv("HEROKU_APP_NAME");
        if (herokuAppName == null || herokuAppName.trim().isEmpty()) {
            log.info("HEROKU_APP_NAME not set, defaulting to staging export directory for safety");
            return STAGING_EXPORT_DIR;
        }
        
        if (PROD_APP_NAME.equals(herokuAppName)) {
            log.info("Detected production environment (Heroku app: {}). Using production export directory: {}", 
                    herokuAppName, PROD_EXPORT_DIR);
            return PROD_EXPORT_DIR;
        } else {
            log.info("Detected staging/sandbox environment (Heroku app: {}). Using staging export directory: {}", 
                    herokuAppName, STAGING_EXPORT_DIR);
            return STAGING_EXPORT_DIR;
        }
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("Starting Short URL CSV export (streaming mode)...");
        
        // Get job parameters for date range (Mode 3) and campaign filter (Mode 4)
        JobParameters jobParams = chunkContext.getStepContext().getStepExecution().getJobParameters();
        String exportDateStart = jobParams.getString("exportDateStart");
        String exportDateEnd = jobParams.getString("exportDateEnd");
        String exportApiKey = jobParams.getString("exportApiKey");
        
        // Determine export mode
        ExportMode mode = determineExportMode(exportDateStart, exportApiKey);
        log.info("Export mode: {} (EXPORT_UPDATES={}, dateRange={}, apiKey={})", 
                mode, exportUpdates, 
                exportDateStart != null ? exportDateStart + " to " + exportDateEnd : "none",
                exportApiKey != null ? exportApiKey : "none");
        
        String actualExportDir = getExportDirectory();
        String herokuAppName = System.getenv("HEROKU_APP_NAME");
        log.info("Using export directory: {} (Heroku app: {})", actualExportDir, 
                herokuAppName != null ? herokuAppName : "not set");
        
        // Find the last export timestamp by checking for existing files (for Mode 1 and 2 only)
        LocalDateTime lastExportTime = null;
        if (mode != ExportMode.DATE_RANGE && mode != ExportMode.CAMPAIGN_DAY) {
            lastExportTime = findLastExportTimestamp(actualExportDir);
        }
        
        AtomicLong recordCount = new AtomicLong(0);
        java.util.concurrent.atomic.AtomicReference<Timestamp> lastRecordTimestamp = 
                new java.util.concurrent.atomic.AtomicReference<>();
        long startTime = System.currentTimeMillis();
        
        // Use temporary filename during upload
        String tempFileName = FILE_PREFIX + "temp_" + System.currentTimeMillis() + FILE_EXTENSION;
        String tempFullPath = actualExportDir + "/" + tempFileName;
        
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            
            PreparedStatement statement = createPreparedStatement(
                    connection, mode, lastExportTime, exportDateStart, exportDateEnd, exportApiKey);
            
            // Stream CSV directly to SFTP using piped streams
            try (ResultSet rs = statement.executeQuery();
                 PipedOutputStream pipedOut = new PipedOutputStream();
                 PipedInputStream pipedIn = new PipedInputStream(pipedOut, 1024 * 1024)) {
                
                // Start writer thread
                Thread writerThread = new Thread(() -> {
                    try (PrintWriter writer = new PrintWriter(
                            new OutputStreamWriter(pipedOut, StandardCharsets.UTF_8))) {
                        
                        // Write CSV header
                        writer.println(CSV_HEADER);
                        
                        // Stream records
                        long batchCount = 0;
                        Timestamp lastCreationDate = null;
                        
                        while (rs.next()) {
                            Timestamp creationDate = rs.getTimestamp("creation_date");
                            if (creationDate != null) {
                                lastCreationDate = creationDate;
                                lastRecordTimestamp.set(creationDate);
                            }
                            
                            writer.println(formatCsvRow(rs, creationDate));
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
                        log.info("Finished reading {} records from database. Last record timestamp: {}", 
                                recordCount.get(), 
                                lastCreationDate != null ? lastCreationDate.toLocalDateTime() : "N/A");
                        
                    } catch (Exception e) {
                        log.error("Error writing CSV data", e);
                        throw new RuntimeException("Failed to write CSV data", e);
                    }
                }, "csv-writer-thread");
                
                writerThread.start();
                
                // Upload CSV stream to SFTP
                uploadStreamToSftp(tempFullPath, pipedIn);
                
                writerThread.join();
                
                if (recordCount.get() == 0) {
                    log.warn("No new records found since last export");
                    deleteTempFile(tempFullPath);
                    return RepeatStatus.FINISHED;
                }
                
                // Rename file based on export mode
                String finalFileName;
                String finalFullPath;
                
                if (mode == ExportMode.CAMPAIGN_DAY) {
                    finalFileName = generateCampaignDayFilename(exportApiKey, exportDateStart, exportDateEnd);
                    finalFullPath = actualExportDir + "/" + finalFileName;
                    log.info("Campaign day export file: {}", finalFileName);
                    renameFileOnSftp(tempFullPath, finalFullPath);
                } else if (mode == ExportMode.DATE_RANGE) {
                    finalFileName = generateDateRangeFilename(exportDateStart, exportDateEnd);
                    finalFullPath = actualExportDir + "/" + finalFileName;
                    log.info("Date range export file: {}", finalFileName);
                    renameFileOnSftp(tempFullPath, finalFullPath);
                } else {
                    // Mode 1 & 2: Use last record's timestamp for incremental tracking
                    Timestamp lastTimestamp = lastRecordTimestamp.get();
                    if (lastTimestamp != null) {
                        LocalDateTime lastRecordTime = lastTimestamp.toLocalDateTime();
                        String fileTimestamp = lastRecordTime.format(FILE_TIMESTAMP_FORMAT);
                        finalFileName = FILE_PREFIX + fileTimestamp + FILE_EXTENSION;
                        finalFullPath = actualExportDir + "/" + finalFileName;
                        
                        log.info("Renaming export file to reflect last record timestamp ({}): {} -> {}", 
                                lastRecordTime, tempFileName, finalFileName);
                        renameFileOnSftp(tempFullPath, finalFullPath);
                    } else {
                        log.warn("No record timestamps found, using current UTC time for filename");
                        LocalDateTime now = LocalDateTime.now(UTC_ZONE);
                        String fileTimestamp = now.format(FILE_TIMESTAMP_FORMAT);
                        finalFileName = FILE_PREFIX + fileTimestamp + FILE_EXTENSION;
                        finalFullPath = actualExportDir + "/" + finalFileName;
                        renameFileOnSftp(tempFullPath, finalFullPath);
                    }
                }
                
                long duration = System.currentTimeMillis() - startTime;
                log.info("Successfully exported {} records to SFTP: {} (took {} ms, {} records/sec)", 
                        recordCount.get(), finalFullPath, duration, 
                        duration > 0 ? (recordCount.get() * 1000 / duration) : 0);
                
                contribution.incrementWriteCount(recordCount.get());
            }
        }
        
        return RepeatStatus.FINISHED;
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
    private String formatCsvRow(ResultSet rs, Timestamp creationDate) throws Exception {
        String mobileNumber = rs.getString("mobile_number");
        String email = rs.getString("email");
        String subscriberKey = rs.getString("subscriber_key");
        String apiKey = rs.getString("api_key");
        String shortUrl = rs.getString("short_url");
        String originalUrl = rs.getString("original_url");
        String messageType = rs.getString("message_type");
        int redirectCount = rs.getInt("redirect_count");
        String transactionDate = rs.getString("transaction_date");
        String transactionTime = rs.getString("transaction_time");
        String transactionId = rs.getString("transaction_id");

        String emailSmsValue = resolveEmailSmsValue(mobileNumber, email, subscriberKey, messageType);

        // CREATIONDATE: convert from UTC to Mexico City time for CSV display
        String formattedDate = "";
        if (creationDate != null) {
            LocalDateTime utcTime = creationDate.toLocalDateTime();
            LocalDateTime mexicoTime = utcTime.atZone(UTC_ZONE)
                    .withZoneSameInstant(MEXICO_CITY_ZONE)
                    .toLocalDateTime();
            formattedDate = mexicoTime.format(CSV_DATETIME_FORMAT);
        }

        // ID_TRANSACCION_MC comes directly from DB
        String idTransaccionMc = safe(transactionId);

        // TIPO_ENVIO: first character of message_type (e.g., "SMS" -> "S")
        String tipoEnvio = extractTipoEnvio(messageType);

        // ID_TRANSACCION_MIL: from DB transaction_time (as text)
        String idTransaccionMil = transactionTime;

        // ID_TRANSACCION_FEC: first 14 digits from transaction_date (before the dash) (as text)
        String idTransaccionFec = extractFirst14Digits(transactionDate);

        return String.join(DELIMITER,
            asText(emailSmsValue),      // EMAIL_SMS (as text)
            asText(emailSmsValue),      // USERNAME (as text)
            safe(apiKey),               // APIKEY_ACORTADOR
            safe(shortUrl),             // IDSHORTURL
            safe(originalUrl),          // URL
            formattedDate,              // CREATIONDATE (Mexico City time)
            idTransaccionMc,            // ID_TRANSACCION_MC
            tipoEnvio,                  // TIPO_ENVIO
            asText(idTransaccionMil),   // ID_TRANSACCION_MIL (as text)
            asText(idTransaccionFec),   // ID_TRANSACCION_FEC (as text)
            String.valueOf(redirectCount) // CLICKS
        );
    }

    /**
     * Resolves EMAIL_SMS/USERNAME value for export.
     * At least one of mobileNumber or email must be present (validated at create time).
     * When both are present, subscriberKey indicates the channel: if it equals email use email,
     * if it equals mobileNumber use mobile; otherwise use subscriberKey.
     */
    private String resolveEmailSmsValue(String mobileNumber, String email, String subscriberKey, String messageType) {
        boolean hasMobile = mobileNumber != null && !mobileNumber.isBlank();
        boolean hasEmail = email != null && !email.isBlank();
        if (hasMobile && !hasEmail) {
            return mobileNumber;
        }
        if (hasEmail && !hasMobile) {
            return email;
        }
        if (hasMobile && hasEmail) {
            String sk = subscriberKey != null ? subscriberKey.trim() : "";
            String em = email != null ? email.trim() : "";
            String mob = mobileNumber != null ? mobileNumber.trim() : "";
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
        if (messageType == null || messageType.isEmpty()) {
            return "";
        }
        return messageType.substring(0, 1).toUpperCase();
    }
    
    /**
     * Extract first 14 digits from transaction_date (before the dash "-")
     * Example: "12345678901234-xyz" -> "12345678901234"
     */
    private String extractFirst14Digits(String transactionDate) {
        if (transactionDate == null || transactionDate.isEmpty()) {
            return "";
        }
        int dashIndex = transactionDate.indexOf('-');
        if (dashIndex > 0) {
            return transactionDate.substring(0, Math.min(dashIndex, 14));
        }
        return transactionDate.length() > 14 ? transactionDate.substring(0, 14) : transactionDate;
    }
    
    /**
     * Safe string - returns empty string if null
     */
    private String safe(String value) {
        return value != null ? value : "";
    }
    
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
        // Convert yyyy-MM-dd to yyyyMMdd for compact filename
        String startCompact = startDate.replace("-", "");
        String endCompact = (endDate != null && !endDate.isBlank()) 
                ? endDate.replace("-", "") 
                : startCompact;
        
        if (startCompact.equals(endCompact)) {
            // Single day export
            return FILE_PREFIX_DATERANGE + startCompact + FILE_EXTENSION;
        } else {
            // Date range export
            return FILE_PREFIX_DATERANGE + startCompact + "_to_" + endCompact + FILE_EXTENSION;
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
        String sanitizedKey = apiKey.replaceAll("[^a-zA-Z0-9_-]", "_");
        if (sanitizedKey.length() > 50) {
            sanitizedKey = sanitizedKey.substring(0, 50);
        }
        
        String startCompact = startDate.replace("-", "");
        String endCompact = (endDate != null && !endDate.isBlank()) 
                ? endDate.replace("-", "") 
                : startCompact;
        
        if (startCompact.equals(endCompact)) {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + FILE_EXTENSION;
        } else {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + "_to_" + endCompact + FILE_EXTENSION;
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
        
        String sql;
        PreparedStatement statement;
        
        switch (mode) {
            case CAMPAIGN_DAY -> {
                LocalDateTime[] utcRange = convertMexicoCityDateRangeToUtc(exportDateStart, exportDateEnd);
                LocalDateTime startUtc = utcRange[0];
                LocalDateTime endUtc = utcRange[1];
                
                log.info("Campaign day export: api_key={}, Mexico City {} to {} -> UTC {} to {}", 
                        exportApiKey, exportDateStart, exportDateEnd, startUtc, endUtc);
                
                sql = """
                    SELECT 
                        mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                        message_type, redirect_count, transaction_date, transaction_time,
                        transaction_id
                    FROM shorturl_sch.short_url
                    WHERE api_key = ? AND creation_date >= ? AND creation_date < ?
                    ORDER BY creation_date DESC
                    """;
                statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(batchSize);
                statement.setString(1, exportApiKey);
                statement.setTimestamp(2, Timestamp.valueOf(startUtc));
                statement.setTimestamp(3, Timestamp.valueOf(endUtc));
            }
            case DATE_RANGE -> {
                // Mode 3: Export by date range (dates are in Mexico City timezone, DB stores UTC)
                LocalDateTime[] utcRange = convertMexicoCityDateRangeToUtc(exportDateStart, exportDateEnd);
                LocalDateTime startUtc = utcRange[0];
                LocalDateTime endUtc = utcRange[1];
                
                log.info("Date range export: Mexico City {} to {} -> UTC {} to {}", 
                        exportDateStart, exportDateEnd, startUtc, endUtc);
                
                sql = """
                    SELECT 
                        mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                        message_type, redirect_count, transaction_date, transaction_time,
                        transaction_id
                    FROM shorturl_sch.short_url
                    WHERE creation_date >= ? AND creation_date < ?
                    ORDER BY creation_date
                    """;
                statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(batchSize);
                statement.setTimestamp(1, Timestamp.valueOf(startUtc));
                statement.setTimestamp(2, Timestamp.valueOf(endUtc));
            }
            case CREATED_OR_UPDATED -> {
                // Mode 2: Export records created OR updated since last export
                if (lastExportTime != null) {
                    log.info("Last export found at: {}. Exporting records created OR updated since then.", lastExportTime);
                    sql = """
                        SELECT 
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        WHERE creation_date > ? OR last_accessed_date > ?
                        ORDER BY creation_date
                        """;
                    statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(batchSize);
                    statement.setTimestamp(1, Timestamp.valueOf(lastExportTime));
                    statement.setTimestamp(2, Timestamp.valueOf(lastExportTime));
                } else {
                    log.info("No previous export found. Exporting ALL records from the beginning.");
                    sql = """
                        SELECT 
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        ORDER BY creation_date
                        """;
                    statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(batchSize);
                }
            }
            default -> {
                // Mode 1 (CREATED_ONLY): Export records created since last export
                if (lastExportTime != null) {
                    log.info("Last export found at: {}. Exporting records created since then.", lastExportTime);
                    sql = """
                        SELECT 
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        WHERE creation_date > ?
                        ORDER BY creation_date
                        """;
                    statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(batchSize);
                    statement.setTimestamp(1, Timestamp.valueOf(lastExportTime));
                } else {
                    log.info("No previous export found. Exporting ALL records from the beginning.");
                    sql = """
                        SELECT 
                            mobile_number, email, subscriber_key, api_key, short_url, original_url, creation_date,
                            message_type, redirect_count, transaction_date, transaction_time,
                            transaction_id
                        FROM shorturl_sch.short_url
                        ORDER BY creation_date
                        """;
                    statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    statement.setFetchSize(batchSize);
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
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
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
        
        // Create start of day in Mexico City, then convert to UTC
        ZonedDateTime startMx = startDate.atStartOfDay(MEXICO_CITY_ZONE);
        LocalDateTime startUtc = startMx.withZoneSameInstant(UTC_ZONE).toLocalDateTime();
        
        // Create end of day (next day 00:00:00) in Mexico City, then convert to UTC
        // Using < endDate+1 day instead of <= endDate 23:59:59 for cleaner queries
        ZonedDateTime endMx = endDate.plusDays(1).atStartOfDay(MEXICO_CITY_ZONE);
        LocalDateTime endUtc = endMx.withZoneSameInstant(UTC_ZONE).toLocalDateTime();
        
        return new LocalDateTime[] { startUtc, endUtc };
    }
    
    /**
     * Find the timestamp of the last export by looking for existing export files on SFTP
     */
    private LocalDateTime findLastExportTimestamp(String exportDirectory) {
        ChannelSftp channelSftp = null;
        
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
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
                log.info("No previous export files found");
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
                            throw new Exception("Failed to parse with both formats", e2);
                        }
                    }
                    
                    if (latestTimestamp == null || fileTimestamp.isAfter(latestTimestamp)) {
                        latestTimestamp = fileTimestamp;
                        latestFileName = filename;
                        if (isOldFormat) {
                            log.debug("Using old format file as latest: {} (adjusted timestamp: {})", filename, latestTimestamp);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Could not parse timestamp from filename: {}. Skipping.", filename);
                }
            }
            
            if (latestTimestamp != null) {
                log.info("Found latest export file: {} with timestamp: {}", latestFileName, latestTimestamp);
            }
            
            return latestTimestamp;
            
        } catch (Exception e) {
            log.warn("Error checking for previous exports: {}. Proceeding with full export.", e.getMessage());
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
            channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
            channelSftp.connect();
            
            log.info("Connected to SFTP server: {}", sftpHost);
            
            int lastSlash = remotePath.lastIndexOf('/');
            String directory = lastSlash > 0 ? remotePath.substring(0, lastSlash) : getExportDirectory();
            ensureDirectoryExists(channelSftp, directory);
            
            log.info("Starting streaming upload to: {}", remotePath);
            channelSftp.put(inputStream, remotePath);
            
            log.info("File uploaded successfully to: {}", remotePath);
            
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
                log.info("Creating directory: {}", path);
                channelSftp.mkdir(path);
            } catch (SftpException ex) {
                log.warn("Could not create directory {}: {}", path, ex.getMessage());
            }
        }
    }
    
    /**
     * Rename a file on SFTP
     */
    private void renameFileOnSftp(String oldPath, String newPath) throws JSchException, SftpException {
        ChannelSftp channelSftp = null;
        try {
            channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
            channelSftp.connect();
            
            int lastSlash = oldPath.lastIndexOf('/');
            String directory = lastSlash > 0 ? oldPath.substring(0, lastSlash) : getExportDirectory();
            String oldFileName = lastSlash > 0 ? oldPath.substring(lastSlash + 1) : oldPath;
            
            lastSlash = newPath.lastIndexOf('/');
            String newFileName = lastSlash > 0 ? newPath.substring(lastSlash + 1) : newPath;
            
            channelSftp.cd(directory);
            channelSftp.rename(oldFileName, newFileName);
            
            log.info("Renamed file on SFTP: {} -> {}", oldFileName, newFileName);
            
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
            channelSftp = (ChannelSftp) sftpSession.openChannel("sftp");
            channelSftp.connect();
            
            int lastSlash = filePath.lastIndexOf('/');
            String directory = lastSlash > 0 ? filePath.substring(0, lastSlash) : getExportDirectory();
            String fileName = lastSlash > 0 ? filePath.substring(lastSlash + 1) : filePath;
            
            channelSftp.cd(directory);
            channelSftp.rm(fileName);
            
            log.info("Deleted temporary file: {}", fileName);
            
        } catch (Exception e) {
            log.warn("Could not delete temporary file {}: {}", filePath, e.getMessage());
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }
}
