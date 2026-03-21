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

package com.salesforce.mcg.datasync.batch.sheet;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.datasync.model.Sheet;
import jakarta.annotation.Resource;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.HashMap;

/**
 * Multi-file tasklet for processing multiple sheet files (catalogo plantillas) as a single grouped job.
 * This tasklet processes all sheet files (1 per company) sequentially within one job execution.
 */
@Slf4j
public class MultiFileSheetTasklet implements Tasklet {

    @Resource
    private Session session;

    @Resource
    private DataSource dataSource;

    @Value("${sftp.data.remote-dir}")
    private String remoteDir;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Get file names from job parameters (comma-separated list)
        String fileNamesParam = chunkContext.getStepContext().getJobParameters().get("fileName").toString();
        String[] files = fileNamesParam.split(",");
        
        log.info("Starting grouped sheet job for {} files: {}", files.length, fileNamesParam);
        
        long startTime = System.currentTimeMillis();
        AtomicLong totalRecords = new AtomicLong(0);
        AtomicLong processedRecords = new AtomicLong(0);
        AtomicLong skippedRecords = new AtomicLong(0);
        AtomicLong errorRecords = new AtomicLong(0);

        // SQL for upserting sheet records - updated to match the correct 5-column schema
        String sql = """
            INSERT INTO datasync_sch.sheet (company, api_key, short_code, main_api_key, sms_name) 
            VALUES (?, ?, ?, ?, ?) 
            ON CONFLICT (company, api_key, short_code) 
            DO UPDATE SET main_api_key = excluded.main_api_key, sms_name = excluded.sms_name
            """;

        ChannelSftp sftp = null;
        try (Connection connection = dataSource.getConnection()) {
            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            connection.setAutoCommit(false);
            
            // Process each file sequentially
            for (String fileName : files) {
                String trimmedFileName = fileName.trim();
                log.info("Processing sheet file: {}", trimmedFileName);
                
                // Extract company name from filename
                String company = extractCompanyFromFilename(trimmedFileName);
                
                // Handle both full paths and relative filenames
                String filePath = trimmedFileName.startsWith("/") ? trimmedFileName : remoteDir + "/" + trimmedFileName;
                
                try (InputStream sftpStream = sftp.get(filePath);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(sftpStream, "UTF-8"));
                     PreparedStatement ps = connection.prepareStatement(sql)) {
                    
                    String line;
                    int batchSize = 0;
                    final int BATCH_COMMIT_SIZE = 1000;
                    boolean isFirstLine = true;
                    Map<String, Integer> columnMap = null;
                    
                    while ((line = reader.readLine()) != null) {
                        // Parse header line to create column mapping
                        if (isFirstLine) {
                            isFirstLine = false;
                            columnMap = parseHeader(line);
                            log.info("Header line for {}: {}", trimmedFileName, line);
                            log.info("Column mapping for {}: {}", trimmedFileName, columnMap);
                            continue;
                        }
                        
                        // Only count data lines, not header lines
                        totalRecords.incrementAndGet();
                        
                        try {
                            List<Sheet> sheets = processLine(line, company, totalRecords.get(), columnMap);
                            if (sheets != null && !sheets.isEmpty()) {
                                for (Sheet sheet : sheets) {
                                    ps.setString(1, sheet.getCompany());
                                    ps.setString(2, sheet.getApiKey());
                                    ps.setString(3, sheet.getShortCode());
                                    ps.setString(4, sheet.getMainApiKey());
                                    ps.setString(5, sheet.getSmsName());
                                    ps.addBatch();
                                    batchSize++;
                                }
                                
                                if (batchSize >= BATCH_COMMIT_SIZE) {
                                    ps.executeBatch();
                                    connection.commit();
                                    processedRecords.addAndGet(batchSize);
                                    batchSize = 0;
                                    log.debug("Committed batch of {} sheet records", BATCH_COMMIT_SIZE);
                                }
                            } else {
                                skippedRecords.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errorRecords.incrementAndGet();
                            log.warn("Error processing sheet line {}: {} - Error: {}", 
                                    totalRecords.get(), line, e.getMessage());
                        }
                    }
                    
                    // Execute remaining batch
                    if (batchSize > 0) {
                        ps.executeBatch();
                        connection.commit();
                        processedRecords.addAndGet(batchSize);
                        log.debug("Committed final batch of {} sheet records", batchSize);
                    }
                    
                    log.info("Completed processing sheet file: {} - Records: {}", 
                            trimmedFileName, processedRecords.get());
                    
                } catch (Exception e) {
                    log.error("Error processing sheet file: {}", trimmedFileName, e);
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
        
        log.info("Grouped sheet job completed in {} seconds ({} minutes)", 
                durationSeconds, durationSeconds / 60);
        log.info("Total records: {}, Processed: {}, Skipped: {}, Errors: {}", 
                totalRecords.get(), processedRecords.get(), skippedRecords.get(), errorRecords.get());
        
        // Update step contribution with statistics efficiently
        try {
            // Use reflection to set the counts directly for better performance
            java.lang.reflect.Field readCountField = contribution.getClass().getDeclaredField("readCount");
            readCountField.setAccessible(true);
            readCountField.set(contribution, totalRecords.get());
            
            java.lang.reflect.Field writeCountField = contribution.getClass().getDeclaredField("writeCount");
            writeCountField.setAccessible(true);
            writeCountField.set(contribution, processedRecords.get());
            
            java.lang.reflect.Field processSkipCountField = contribution.getClass().getDeclaredField("processSkipCount");
            processSkipCountField.setAccessible(true);
            processSkipCountField.set(contribution, skippedRecords.get());
            
            log.info("Updated Spring Batch statistics: Read={}, Write={}, ProcessSkip={}", 
                    totalRecords.get(), processedRecords.get(), skippedRecords.get());
                    
        } catch (Exception reflectionError) {
            log.warn("Could not update Spring Batch statistics via reflection: {}", reflectionError.getMessage());
            // Fallback to increment methods (less efficient but functional)
            contribution.incrementWriteCount(processedRecords.get());
            log.info("Fallback statistics update: Write={}, Actual Read={}, ProcessSkip={}", 
                    processedRecords.get(), totalRecords.get(), skippedRecords.get());
        }

        return RepeatStatus.FINISHED;
    }

    /**
     * Process a single CSV line and return list of Sheet objects, or null if the line should be skipped.
     * Each line creates 3 Sheet objects with different shortCodes (89992, 35000, 90120).
     * Updated to use dynamic column mapping based on header names.
     */
    private List<Sheet> processLine(String line, String company, long lineNumber, Map<String, Integer> columnMap) throws Exception {
        String[] values = line.split(",", -1);

        // Get column indexes dynamically based on header names (handle BOM and case variations)
        Integer apiKey89992Index = getColumnIndex(columnMap, "APIKEY_SOLOTELCEL_89992", "APIKEY_SOLOTELCEL_89992");
        Integer smsName89992Index = getColumnIndex(columnMap, "NOMBRESMS_89992", "NOMBRESMS_89992");
        Integer apiKey35000Index = getColumnIndex(columnMap, "APIKEY_NOTELCEL_35000", "APIKEY_NOTELCEL_35000");
        Integer smsName35000Index = getColumnIndex(columnMap, "NOMBRESMS_35000", "NOMBRESMS_35000");
        Integer apiKey90120Index = getColumnIndex(columnMap, "APIKEY_NOTELCEL_90120", "APIKEY_NOTELCEL_90120");
        Integer smsName90120Index = getColumnIndex(columnMap, "NOMBRESMS_90120", "NOMBRESMS_90120");
        
        // Validate required columns exist
        if (apiKey89992Index == null || smsName89992Index == null || 
            apiKey35000Index == null || smsName35000Index == null ||
            apiKey90120Index == null || smsName90120Index == null) {
            throw new IllegalArgumentException("Missing required columns. Expected: ApiKey_SoloTelcel_89992, NombreSMS_89992, ApiKey_NoTelcel_35000, NombreSMS_35000, ApiKey_NoTelcel_90120, NombreSMS_90120");
        }
        
        // Validate we have enough columns for all required indexes
        int maxRequiredIndex = Math.max(Math.max(Math.max(apiKey89992Index, smsName89992Index), 
                                                Math.max(apiKey35000Index, smsName35000Index)),
                                       Math.max(apiKey90120Index, smsName90120Index));
        
        if (values.length <= maxRequiredIndex) {
            log.warn("Skipping sheet line {} - insufficient columns: {} (expected at least {})", 
                    lineNumber, values.length, maxRequiredIndex + 1);
            return null;
        }

        try {
            List<Sheet> sheets = new ArrayList<>();
            
            // Get API keys for related sheets
            String apiKey89992 = values[apiKey89992Index];
            String apiKey35000 = values[apiKey35000Index];
            String apiKey90120 = values[apiKey90120Index];
            
            // The main API key should be the one for the 89992 template (first key in CSV)
            String mainApiKey = apiKey89992;
            
            // Create Sheet for template 89992
            Sheet tmpl_89992 = Sheet.builder()
                    .company(company)
                    .apiKey(apiKey89992)
                    .shortCode("89992")
                    .mainApiKey(mainApiKey) // Use the 89992 API key as main for all templates
                    .smsName(values[smsName89992Index])
                    .build();
            sheets.add(tmpl_89992);
            
            // Create Sheet for template 35000
            Sheet tmpl_35000 = Sheet.builder()
                    .company(company)
                    .apiKey(apiKey35000)
                    .shortCode("35000")
                    .mainApiKey(mainApiKey) // Use the 89992 API key as main for all templates
                    .smsName(values[smsName35000Index])
                    .build();
            sheets.add(tmpl_35000);
            
            // Create Sheet for template 90120
            Sheet tmpl_90120 = Sheet.builder()
                    .company(company)
                    .apiKey(apiKey90120)
                    .shortCode("90120")
                    .mainApiKey(mainApiKey) // Use the 89992 API key as main for all templates
                    .smsName(values[smsName90120Index])
                    .build();
            sheets.add(tmpl_90120);
            
            return sheets;
        } catch (Exception e) {
            log.warn("Skipping sheet line {} - processing error: {} - Exception: {}", lineNumber, line, e.getMessage());
            return null;
        }
    }

    /**
     * Get column index by name, handling BOM and case variations
     */
    private Integer getColumnIndex(Map<String, Integer> columnMap, String... possibleNames) {
        for (String name : possibleNames) {
            Integer index = columnMap.get(name);
            if (index != null) {
                return index;
            }
        }
        return null;
    }

    /**
     * Parse CSV header line to create column name to index mapping
     */
    private Map<String, Integer> parseHeader(String headerLine) {
        Map<String, Integer> columnMap = new HashMap<>();
        String[] headers = headerLine.split(",", -1);
        
        for (int i = 0; i < headers.length; i++) {
            // Remove BOM character and trim
            String columnName = headers[i].trim();
            if (columnName.startsWith("\uFEFF")) {
                columnName = columnName.substring(1);
            }
            columnName = columnName.toUpperCase();
            columnMap.put(columnName, i);
        }
        
        return columnMap;
    }

    /**
     * Extract company name from filename.
     * Assumes filename format contains company identifier.
     */
    private String extractCompanyFromFilename(String filename) {
        // Extract company from filename - matches the pattern used in other jobs
        String lowerFilename = filename.toLowerCase();
        if (lowerFilename.startsWith("telnor") || lowerFilename.contains("telnor")) {
            return "telnor";
        } else if (lowerFilename.startsWith("telmex") || lowerFilename.contains("telmex")) {
            return "telmex";
        } else {
            // Default fallback to telmex for backward compatibility
            log.warn("Could not determine company from filename: {}, defaulting to telmex", filename);
            return "telmex";
        }
    }

    /**
     * Check if a string is not null, not empty, and not just whitespace.
     */
    private boolean isNotBlank(String str) {
        return str != null && !str.trim().isEmpty();
    }
}
