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

package com.salesforce.mcg.datasync.batch.operator;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.datasync.model.Operator;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.HashMap;

/**
 * Multi-file tasklet for processing multiple operator files as a single grouped job.
 * This tasklet processes all operator files (1 per company) sequentially within one job execution.
 */
@Slf4j
public class MultiFileOperatorTasklet implements Tasklet {

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
        
        log.info("Starting grouped operator job for {} files: {}", files.length, fileNamesParam);
        
        long startTime = System.currentTimeMillis();
        AtomicLong totalRecords = new AtomicLong(0);
        AtomicLong processedRecords = new AtomicLong(0);
        AtomicLong skippedRecords = new AtomicLong(0);
        AtomicLong errorRecords = new AtomicLong(0);

        // SQL for upserting operator records - updated to include all columns
        String sql = """
            INSERT INTO datasync_sch.operator (
                operator, name, type, rao, master_account, additional_master_account,
                virtual_operator, dual_operator, master_account_11, master_account_16,
                master_account_telnor, id_rvta_mayorista, idx
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (operator) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                rao = EXCLUDED.rao,
                master_account = EXCLUDED.master_account,
                additional_master_account = EXCLUDED.additional_master_account,
                virtual_operator = EXCLUDED.virtual_operator,
                dual_operator = EXCLUDED.dual_operator,
                master_account_11 = EXCLUDED.master_account_11,
                master_account_16 = EXCLUDED.master_account_16,
                master_account_telnor = EXCLUDED.master_account_telnor,
                id_rvta_mayorista = EXCLUDED.id_rvta_mayorista,
                idx = EXCLUDED.idx
            """;

        ChannelSftp sftp = null;
        try (Connection connection = dataSource.getConnection()) {
            sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            connection.setAutoCommit(false);
            
            // Process each file sequentially
            for (String fileName : files) {
                String trimmedFileName = fileName.trim();
                log.info("Processing operator file: {}", trimmedFileName);
                
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
                            Operator operator = processLine(line, totalRecords.get(), columnMap);
                            if (operator != null) {
                                ps.setString(1, operator.getOperator());
                                ps.setString(2, operator.getName());
                                ps.setString(3, operator.getType());
                                ps.setString(4, operator.getRao());
                                ps.setString(5, operator.getMasterAccount());
                                ps.setString(6, operator.getAdditionalMasterAccount());
                                ps.setString(7, operator.getVirtualOperator());
                                ps.setString(8, operator.getDualOperator());
                                ps.setString(9, operator.getMasterAccount11());
                                ps.setString(10, operator.getMasterAccount16());
                                ps.setString(11, operator.getMasterAccountTelnor());
                                ps.setString(12, operator.getIdRvtaMayorista());
                                ps.setString(13, operator.getIdx());
                                ps.addBatch();
                                batchSize++;
                                
                                if (batchSize >= BATCH_COMMIT_SIZE) {
                                    ps.executeBatch();
                                    connection.commit();
                                    processedRecords.addAndGet(batchSize);
                                    batchSize = 0;
                                    log.debug("Committed batch of {} operator records", BATCH_COMMIT_SIZE);
                                }
                            } else {
                                skippedRecords.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errorRecords.incrementAndGet();
                            log.warn("Error processing operator line {}: {} - Error: {}", 
                                    totalRecords.get(), line, e.getMessage());
                        }
                    }
                    
                    // Execute remaining batch
                    if (batchSize > 0) {
                        ps.executeBatch();
                        connection.commit();
                        processedRecords.addAndGet(batchSize);
                        log.debug("Committed final batch of {} operator records", batchSize);
                    }
                    
                    log.info("Completed processing operator file: {} - Records: {}", 
                            trimmedFileName, processedRecords.get());
                    
                } catch (Exception e) {
                    log.error("Error processing operator file: {}", trimmedFileName, e);
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
        
        log.info("Grouped operator job completed in {} seconds ({} minutes)", 
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
     * Process a single CSV line and return Operator object, or null if the line should be skipped.
     * Updated to use dynamic column mapping based on header names and extract all columns.
     */
    private Operator processLine(String line, long lineNumber, Map<String, Integer> columnMap) throws Exception {
        String[] v = line.split(",", -1);

        // Get column indexes dynamically based on header names
        Integer operadorIndex = columnMap.get("OPERADOR");
        Integer nombreOperadorIndex = columnMap.get("NOMBRE_OPERADOR");
        Integer tipoOperadorIndex = columnMap.get("TIPO_OPERADOR");
        Integer raoIndex = columnMap.get("RAO");
        Integer ctaMaestraIndex = columnMap.get("CTA_MAESTRA");
        Integer ctaMaestraAdicionalIndex = columnMap.get("CTA_MAESTRA_ADICIONAL");
        Integer operadorVirtualIndex = columnMap.get("OPERADOR_VIRTUAL");
        Integer operadorDualIndex = columnMap.get("OPERADOR_DUAL");
        Integer ctaMaestra11Index = columnMap.get("CTA_MAESTRA_11");
        Integer ctaMaestra16Index = columnMap.get("CTA_MAESTRA_16");
        Integer ctaMaestraTelnorIndex = columnMap.get("CTA_MAESTRA_TELNOR");
        Integer idRvtaMayoristaIndex = columnMap.get("ID_RVTA_MAYORISTA");
        Integer idxIndex = columnMap.get("IDX");
        
        // Validate required columns exist
        if (operadorIndex == null || nombreOperadorIndex == null) {
            throw new IllegalArgumentException("Missing required columns. Expected: OPERADOR, NOMBRE_OPERADOR");
        }
        
        // Validate we have enough columns for the required indexes
        int maxRequiredIndex = Math.max(operadorIndex, nombreOperadorIndex);
        
        if (v.length <= maxRequiredIndex) {
            log.warn("Skipping operator line {} - insufficient columns: {} (expected at least {})", 
                    lineNumber, v.length, maxRequiredIndex + 1);
            return null;
        }

        try {
            String operatorId = v[operadorIndex];
            String operatorName = v[nombreOperadorIndex];

            if (operatorName == null || operatorName.trim().isEmpty()) {
                log.warn("Skipping operator line {} - empty operator name: {}", lineNumber, line);
                return null;
            }

            Operator operator = new Operator();
            operator.setOperator(operatorId);
            operator.setName(operatorName.trim());
            
            // Set all other fields, handling null/empty values gracefully
            operator.setType(getColumnValue(v, tipoOperadorIndex));
            operator.setRao(getColumnValue(v, raoIndex));
            operator.setMasterAccount(getColumnValue(v, ctaMaestraIndex));
            operator.setAdditionalMasterAccount(getColumnValue(v, ctaMaestraAdicionalIndex));
            operator.setVirtualOperator(getColumnValue(v, operadorVirtualIndex));
            operator.setDualOperator(getColumnValue(v, operadorDualIndex));
            operator.setMasterAccount11(getColumnValue(v, ctaMaestra11Index));
            operator.setMasterAccount16(getColumnValue(v, ctaMaestra16Index));
            operator.setMasterAccountTelnor(getColumnValue(v, ctaMaestraTelnorIndex));
            operator.setIdRvtaMayorista(getColumnValue(v, idRvtaMayoristaIndex));
            operator.setIdx(getColumnValue(v, idxIndex));
            
            return operator;
        } catch (Exception e) {
            log.warn("Skipping operator line {} - processing error: {} - Exception: {}", lineNumber, line, e.getMessage());
            return null;
        }
    }

    /**
     * Get column value by index from parsed CSV values, handling null/empty values
     */
    private String getColumnValue(String[] values, Integer index) {
        if (index == null || index >= values.length) {
            return null;
        }
        String value = values[index];
        return (value == null || value.trim().isEmpty()) ? null : value.trim();
    }

    /**
     * Parse CSV header line to create column name to index mapping
     */
    private Map<String, Integer> parseHeader(String headerLine) {
        Map<String, Integer> columnMap = new HashMap<>();
        String[] headers = headerLine.split(",", -1);
        
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i].trim().toUpperCase();
            columnMap.put(columnName, i);
        }
        
        return columnMap;
    }
}
