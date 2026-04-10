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

import com.salesforce.mcg.datasync.aspect.MexicoCityTimezone;
import com.salesforce.mcg.datasync.aspect.TimezoneContext;
import com.salesforce.mcg.datasync.common.AppConstants;
import com.salesforce.mcg.datasync.helper.DateFormatterHelper;
import com.salesforce.mcg.datasync.repository.impl.JobExecutionHistoryJdbcRepository;
import com.salesforce.mcg.datasync.service.SftpService;
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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Tasklet that exports Short URL records incrementally to a CSV file on SFTP.
 * <p>
 * Export Format (CSV with comma delimiter):
 * EMAIL_SMS, USERNAME, APIKEY_ACORTADOR, TCODE, COMPANY, IDSHORTURL, URL,
 * ID_TRANSACCION_MC, TIPO_ENVIO, ID_TRANSACCION_DATE, CLICKS
 * </p>
 * <p>
 * Export Modes (all dates interpreted in America/Mexico_City timezone):
 * - Default (PREVIOUS_DAY): Export records from yesterday (00:00 – 24:00 Mexico City time)
 * - Mode 1 (DAILY): Export records for a specific day (--date)
 * - Mode 2 (DATE_RANGE): Export records for a date range (--startDate + --endDate)
 * - Mode 3 (CAMPAIGN_DAY): Export records for a specific campaign on a day (--apiKey + --date)
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

    private static final String DATE_PARAM_PATTERN = "yyyy-MM-dd";
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DATE_PARAM_PATTERN);

    private static final String FILE_PREFIX = "shorturl_export_";
    private static final String FILE_PREFIX_DATERANGE = "shorturl_daterange_";
    private static final String FILE_PREFIX_CAMPAIGN = "shorturl_campaign_";
    private static final String FILE_EXTENSION = ".csv";

//    private static final String PROD_APP_NAME = "mcg-data-sync-app";
//    private static final String STAGING_EXPORT_DIR = "/exports_staging";
//    private static final String PROD_EXPORT_DIR = "/exports";

    private static final String EXPORT_DATE_PARAM = "date";
    private static final String EXPORT_DATE_START_PARAM = "startDate";
    private static final String EXPORT_DATE_END_PARAM = "endDate";
    private static final String EXPORT_API_KEY_PARAM = "apiKey";

    private static final String TEMP_FILE_PREFIX = FILE_PREFIX + "temp_";
    private static final String DATE_RANGE_SEPARATOR = "_to_";
    private static final int CAMPAIGN_KEY_MAX_LENGTH = 50;

    // CSV headers for export
    private static final String CSV_HEADER = String.join(
            AppConstants.Strings.COMMA,
            "EMAIL_SMS",
            "USERNAME",
            "APIKEY_ACORTADOR",
            "TCODE",
            "COMPANY",
            "IDSHORTURL",
            "URL",
            "ID_TRANSACCION_MC",
            "TIPO_ENVIO",
            "ID_TRANSACCION_DATE",
            "CLICKS"
    );

    public static final String SELECT_PREFIX_SQL = """
        SELECT
            phone_number,
            email,
            mobile_number,
            api_key,
            short_url,
            original_url,
            message_type,
            redirect_count,
            transaction_date,
            transaction_id,
            tcode,
            company,
            creation_date
        FROM shorturl_sch.short_url
        """;

    private final DataSource dataSource;
    private final SftpService sftpService;
    private final JobExecutionHistoryJdbcRepository jobHistRepository;
    private final SftpPropertyContext context;

    @Value("${sftp.data.export-dir:/exports_staging/datasync_qa}")
    private String exportDirectory;

    @Value("${shorturl.export.batch-size:10000}")
    private int batchSize;


    public ShortUrlExportCSVTasklet(
            DataSource dataSource,
            SftpPropertyContext context,
            SftpService sftpService,
            JobExecutionHistoryJdbcRepository jobHistRepository) {
        this.dataSource = dataSource;
        this.context = context;
        this.sftpService = sftpService;
        this.jobHistRepository = jobHistRepository;
    }

    @MexicoCityTimezone("Short URL CSV Export")
    @Override
    public RepeatStatus execute(
            @NonNull StepContribution contribution,
            @NonNull ChunkContext chunkContext) throws Exception {

        log.info("🚀 Starting Short URL CSV export (streaming mode)...");

        AtomicLong recordCount = new AtomicLong(0);
        AtomicReference<Timestamp> lastRecordTimestamp = new AtomicReference<>();
        long startTime = System.currentTimeMillis();

        /* -- File names -- */
        String tempFileName = TEMP_FILE_PREFIX + System.currentTimeMillis() + FILE_EXTENSION;
        String tempFullPath = buildSftpPath(exportDirectory, tempFileName);

        /* -- Export variables -- */
        var jobParams = chunkContext.getStepContext().getStepExecution().getJobParameters();
        var exportDate = jobParams.getString(EXPORT_DATE_PARAM);
        var exportDateStart = jobParams.getString(EXPORT_DATE_START_PARAM);
        var exportDateEnd = jobParams.getString(EXPORT_DATE_END_PARAM);
        var exportApiKey = jobParams.getString(EXPORT_API_KEY_PARAM);

        /* -- Default export date (previous day in active timezone) -- */
        var previousDay = TimezoneContext.today().minusDays(1);
        var previousDayStr = dateFormatter.format(previousDay);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            PreparedStatement statement = createPreparedStatement(
                    connection,
                    exportDate,
                    exportDateStart,
                    exportDateEnd);

            try (ResultSet rs = statement.executeQuery();
                 PipedOutputStream pipedOut = new PipedOutputStream();
                 PipedInputStream pipedIn = new PipedInputStream(pipedOut, 1024 * 1024);
                 SftpService.SftpChannel sftp = sftpService.openChannel()) {

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
                                log.info("ℹ️ Exported {} records so far... (last record timestamp MX: {})",
                                        recordCount.get(),
                                        lastCreationDate != null ? DateFormatterHelper.format(lastCreationDate) : "N/A");
                                writer.flush();
                            }
                        }

                        writer.flush();
                        log.info("🏁 Finished reading {} records from database. Last record timestamp MX: {}",
                                recordCount.get(),
                                lastCreationDate != null ? DateFormatterHelper.format(lastCreationDate) : "N/A");

                    } catch (Exception e) {
                        log.error("❌ Error writing CSV data. Error: {}", e.getMessage());
                        throw new RuntimeException("❌ Failed to write CSV data", e);
                    }
                }, "csv-writer-thread");

                writerThread.start();
                sftp.upload(tempFullPath, pipedIn);
                writerThread.join();

                if (recordCount.get() == 0) {
                    log.warn("⚠️ No new records found since last export");
                    sftp.delete(tempFullPath);
                    return RepeatStatus.FINISHED;
                }
                var finalFileName = resolveFinalFileName(
                        exportApiKey,
                        exportDate,
                        exportDateStart,
                        exportDateEnd,
                        previousDayStr);
                var finalFullPath = buildSftpPath(exportDirectory, finalFileName);
                try {
                    sftp.rename(tempFullPath, finalFullPath);
                } catch (Exception renameEx) {
                    log.error("❌ Failed to rename temp file {} → {}. Cleaning up temp file.",
                            tempFullPath, finalFullPath);
                    sftp.delete(tempFullPath);
                    throw renameEx;
                }
                long duration = System.currentTimeMillis() - startTime;
                log.info("🏁 Successfully exported {} records to SFTP: {} (took {} ms, {} records/sec)",
                        recordCount.get(), finalFullPath, duration,
                        duration > 0 ? (recordCount.get() * 1000 / duration) : 0);

                contribution.incrementWriteCount(recordCount.get());
            }
        }

        return RepeatStatus.FINISHED;
    }

    private String resolveFinalFileName(
            String apiKey,
            String date,
            String startDate,
            String endDate,
            String lastExecution
            ) {
        if (Strings.isNotBlank(apiKey) && Strings.isNotBlank(date)){
            var fileName = generateCampaignDayFilename(apiKey, date);
            log.info("ℹ️ Campaign day export file: {}", fileName);
            return fileName;
        }
        if (Strings.isNotBlank(startDate) && Strings.isNotBlank(endDate)){
            var fileName = generateDateRangeFilename(startDate, endDate);
            log.info("ℹ️ Date range export file: {}", fileName);
            return fileName;
        }

        return FILE_PREFIX + lastExecution + FILE_EXTENSION;
    }

    private String buildSftpPath(String directory, String fileName) {
        return directory + "/" + fileName;
    }
    
    /**
     * Format a single ResultSet row as a CSV row with comma delimiter.
     * <p>
     * Columns:
     * EMAIL_SMS, USERNAME, APIKEY_ACORTADOR, TCODE, COMPANY, IDSHORTURL, URL,
     * ID_TRANSACCION_MC, TIPO_ENVIO, ID_TRANSACCION_DATE, CLICKS
     * </p>
     * Note: EMAIL_SMS, USERNAME, ID_TRANSACCION_MIL, and ID_TRANSACCION_FEC are
     * wrapped in quotes to ensure they are treated as text in CSV/Excel.
     * {@code email} is read for future EMAIL_SMS resolution (see commented {@code resolveEmailSmsValue}).
     */
    @SuppressWarnings("unused")
    private String formatCsvRow(ResultSet rs) throws Exception {
        var phoneNumber = rs.getString("phone_number");
        var email = rs.getString("email");
        var mobileNumber = rs.getString("mobile_number");//celular
        var apiKey = rs.getString("api_key");
        var shortUrl = rs.getString("short_url");
        var originalUrl = rs.getString("original_url");
        var messageType = rs.getString("message_type");
        var redirectCount = rs.getInt("redirect_count");
        var transactionDate = rs.getString("transaction_date");
        var transactionId = rs.getString("transaction_id");
        var tcode = rs.getString("tcode");
        var company = rs.getString("company");

        String idTransaccionMc = Objects.requireNonNullElse(transactionId, Strings.EMPTY);
        String idTransaccionFec = extractTransactionDate(transactionDate);

        return String.join(AppConstants.Strings.COMMA,
                asText(mobileNumber),
                asText(phoneNumber).replaceAll("^52",""),
                Objects.requireNonNullElse(apiKey, Strings.EMPTY),
                Objects.requireNonNullElse(tcode, Strings.EMPTY),
                Objects.requireNonNullElse(company, Strings.EMPTY),
                Objects.requireNonNullElse(shortUrl, Strings.EMPTY),
                Objects.requireNonNullElse(originalUrl, Strings.EMPTY),
                idTransaccionMc,
                extractTipoEnvio(messageType),
                asText(idTransaccionFec),
                String.valueOf(redirectCount)
        );
    }

//    /**
//     * Resolves EMAIL_SMS/USERNAME value for export.
//     * At least one of mobileNumber or email must be present (validated at create time).
//     * When both are present, subscriberKey indicates the channel: if it equals email use email,
//     * if it equals mobileNumber use mobile; otherwise use subscriberKey.
//     */
//    private String resolveEmailSmsValue(String mobileNumber, String email, String subscriberKey) {
//        /* -- Adjust content -- */
//        var subscriberKeyAdj = Objects.requireNonNullElse(subscriberKey, Strings.EMPTY).trim();
//        var emailAdj = Objects.requireNonNullElse(email, Strings.EMPTY).trim();
//        var mobileNumberAdj = Objects.requireNonNullElse(mobileNumber, Strings.EMPTY).trim();
//
//        boolean hasMobileNumber = Strings.isNotBlank(mobileNumberAdj);
//        boolean hasEmail = Strings.isNotBlank(emailAdj);
//
//        if (hasMobileNumber && hasEmail) {
//            if (subscriberKeyAdj.equalsIgnoreCase(emailAdj)) {
//                return emailAdj;
//            }
//            if (subscriberKeyAdj.equals(mobileNumberAdj)) {
//                return mobileNumberAdj;
//            }
//            return Objects.nonNull(subscriberKey) ? subscriberKeyAdj : Strings.EMPTY;
//        }
//
//        if (hasMobileNumber) return mobileNumberAdj;
//        if (hasEmail) return emailAdj;
//
//        return AppConstants.Strings.EMPTY;
//    }
    
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

    /**
     * Extracts the full 17-digit transaction date (YYYYMMDDHHmmssSSS) including milliseconds.
     * Legacy format: 17 digits then dash and suffix (e.g. {@code 20260407123456789-xyz}) → first 17 digits.
     * ISO dates (e.g. {@code 2026-04-07}) have '-' earlier; return the full trimmed value.
     * Undelimited strings longer than 17 characters → first 17 characters.
     */
    private String extractTransactionDate(String transactionDate) {
        if (transactionDate == null || transactionDate.isEmpty()) {
            return Strings.EMPTY;
        }
        String trimmed = transactionDate.trim();
        int dashIndex = trimmed.indexOf('-');
        if (dashIndex == 17 && trimmed.length() >= 17) {
            String prefix = trimmed.substring(0, 17);
            if (prefix.chars().allMatch(Character::isDigit)) {
                return prefix;
            }
        }
        if (dashIndex < 0 && trimmed.length() > 17) {
            return trimmed.substring(0, 17);
        }
        return trimmed;
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
     * <p>
     * Examples:
     * - Single day: shorturl_daterange_20241205.csv
     * - Date range: shorturl_daterange_20241201_to_20241205.csv
     * </p>
     * @param startDate start date in format yyyy-MM-dd
     * @param endDate end date in format yyyy-MM-dd (can be same as startDate for single day)
     * @return the generated filename
     */
    private String generateDateRangeFilename(@NonNull String startDate, @NonNull String endDate) {
        String startCompact = compactDate(startDate);
        String endCompact = compactDate(endDate);
        if (startCompact.equals(endCompact)) {
            return FILE_PREFIX_DATERANGE + startCompact + FILE_EXTENSION;
        } else {
            return FILE_PREFIX_DATERANGE + startCompact + DATE_RANGE_SEPARATOR + endCompact + FILE_EXTENSION;
        }
    }
    
    /**
     * Generates a filename for campaign day exports (Mode 4).
     * Includes the api_key (sanitized) and date for easy identification.
     * <p>
     * Example: shorturl_campaign_myApiKey_20260319.csv
     * </p>
     * @param apiKey the campaign api_key
     * @param date start date in format yyyy-MM-dd
     * @return the generated filename
     */
    private String generateCampaignDayFilename(@NonNull String apiKey, @NonNull String date) {
        String sanitizedKey = sanitizeApiKeyForFilename(apiKey);
        if (sanitizedKey.length() > CAMPAIGN_KEY_MAX_LENGTH) {
            sanitizedKey = sanitizedKey.substring(0, CAMPAIGN_KEY_MAX_LENGTH);
        }
        String startCompact = compactDate(date);
        String endCompact = compactDate(date);
        if (startCompact.equals(endCompact)) {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + FILE_EXTENSION;
        } else {
            return FILE_PREFIX_CAMPAIGN + sanitizedKey + "_" + startCompact + DATE_RANGE_SEPARATOR + endCompact + FILE_EXTENSION;
        }
    }

    /**
     * Creates a PreparedStatement based on the export mode.
     * @param connection the database connection
     * @param exportDateStart the start date for date range export (Mexico City timezone, format: yyyy-MM-dd)
     * @param exportDateEnd the end date for date range export (Mexico City timezone, format: yyyy-MM-dd)
     * @param exportApiKey the api_key to filter by (for Mode 4 CAMPAIGN_DAY)
     *
     * @throws Exception exception thrown by method
     */
    private PreparedStatement createPreparedStatement(
            Connection connection,
            String exportDate,
            String exportDateStart,
            String exportDateEnd) throws Exception {

        PreparedStatement statement;
        ZonedDateTime startMx;
        ZonedDateTime endMx;

        if (Strings.isNotBlank(exportDate)){
            log.info("🚀 Exporting Mode: Daily ({})", TimezoneContext.zone());
            LocalDate date = LocalDate.parse(exportDate, dateFormatter);
            startMx = TimezoneContext.startOfDay(date);
            endMx = TimezoneContext.startOfNextDay(date);

        } else if (Strings.isNotBlank(exportDateStart)
                && Strings.isNotBlank(exportDateEnd)){
            log.info("🚀 Exporting Mode: Range ({})", TimezoneContext.zone());
            LocalDate start = LocalDate.parse(exportDateStart, dateFormatter);
            LocalDate end = LocalDate.parse(exportDateEnd, dateFormatter);
            startMx = TimezoneContext.startOfDay(start);
            endMx = TimezoneContext.startOfNextDay(end);

        } else {
            log.info("🚀 Exporting Mode: Previous Day ({})", TimezoneContext.zone());
            LocalDate yesterday = TimezoneContext.today().minusDays(1);
            startMx = TimezoneContext.startOfDay(yesterday);
            endMx = TimezoneContext.startOfNextDay(yesterday);
        }

        log.info("ℹ️ Query window: {} → {} (Mexico City)", startMx, endMx);

        statement = prepareForwardOnlyReadOnlyStatement(connection, SELECT_PREFIX_SQL + """
                    WHERE
                        (creation_date >= ? AND creation_date < ?)
                    OR
                        (last_accessed_date >= ? AND last_accessed_date < ?)
                    ORDER BY creation_date DESC
                    """);
        Timestamp tsStart = Timestamp.from(startMx.toInstant());
        Timestamp tsEnd = Timestamp.from(endMx.toInstant());
        statement.setTimestamp(1, tsStart);
        statement.setTimestamp(2, tsEnd);
        statement.setTimestamp(3, tsStart);
        statement.setTimestamp(4, tsEnd);

        return statement;
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

}
