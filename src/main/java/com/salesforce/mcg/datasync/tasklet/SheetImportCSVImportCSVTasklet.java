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

package com.salesforce.mcg.datasync.newbatch.tasklet;

import com.salesforce.mcg.datasync.newbatch.data.Sheet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Multi-file tasklet for processing multiple sheet files (catalogo plantillas) as a single grouped job.
 * This tasklet processes all sheet files (1 per company) sequentially within one job execution.
 */
@Component
@Slf4j
public class SheetImportCSVImportCSVTasklet extends AbstractImportCSVTasklet<List<Sheet>> {

    private static final Pattern COMPANY_PATTERN =
            Pattern.compile(
                    ".*?(telmex)?.*?(telnor)?.*",
                    Pattern.CASE_INSENSITIVE);

    private static final String INSERT_SQL = """
            INSERT INTO datasync_sch.sheet (
                company,
                api_key,
                short_code,
                main_api_key,
                sms_name
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (company, api_key, short_code)
            DO UPDATE SET main_api_key = excluded.main_api_key, sms_name = excluded.sms_name
        """;

    /**
     * Process a single CSV line and return list of Sheet objects, or null if the line should be skipped.
     * Each line creates 3 Sheet objects with different shortCodes (89992, 35000, 90120).
     * Updated to use dynamic column mapping based on header names.
     */
    public List<Sheet> parseLine(
            String fileName,
            String line,
            long lineNumber,
            Map<String, Integer> columnMap) throws Exception {

        var company = extractCompanyFromFilename(fileName);

        String[] values = line.split(COMMA, -1);

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
            log.warn("⚠️ Skipping sheet line {} - insufficient columns: {} (expected at least {})",
                    lineNumber, values.length, maxRequiredIndex + 1);
            return null;
        }

        try {

            // Get API keys for related sheets
            var apiKey89992 = values[apiKey89992Index];
            var apiKey35000 = values[apiKey35000Index];
            var apiKey90120 = values[apiKey90120Index];
            
            var sheetList = List.of(
                new Sheet(
                    company,
                    apiKey89992,
                    "89992",
                    apiKey89992, // Use the 89992 API key as main for all templates
                    values[smsName89992Index]),
                new Sheet(
                    company,
                    apiKey35000,
                    "35000",
                    apiKey89992, // Use the 89992 API key as main for all templates
                    values[smsName35000Index]),
                new Sheet(
                    company,
                    apiKey90120,
                    "90120",
                    apiKey89992, // Use the 89992 API key as main for all templates
                    values[smsName90120Index]));
            return new ArrayList<>(sheetList);
        } catch (Exception e) {
            log.warn("⚠️ Skipping line {} - processing error: {} - Exception: {}", lineNumber, line, e.getMessage());
            return null;
        }
    }

    @Override
    public void prepareStatement(
            List<Sheet> sheets,
            PreparedStatement ps,
            AtomicInteger batchSize) throws SQLException {
        if (sheets != null) {
            for (Sheet sheet : sheets) {
                ps.setString(1, sheet.company());
                ps.setString(2, sheet.apiKey());
                ps.setString(3, sheet.shortCode());
                ps.setString(4, sheet.mainApiKey());
                ps.setString(5, sheet.smsName());
                ps.addBatch();
                batchSize.incrementAndGet();
            }
        }
    }

    @Override
    public String insertSQL(){
        return INSERT_SQL;
    }

    /**
     * Get column index by name.
     */
    private Integer getColumnIndex(Map<String, Integer> columnMap, String... possibleNames) {
        return Arrays.stream(possibleNames)
                .map(columnMap::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /**
     * Extract company name from filename.
     */
    private String extractCompanyFromFilename(String filename) {
        String value = filename == null ? "" : filename.toLowerCase();
        Matcher matcher = COMPANY_PATTERN.matcher(value);

        if (!matcher.matches()) {
            log.warn("Could not determine company from filename: {}, defaulting to telmex", filename);
            return "telmex";
        }

        String telmexGroup = matcher.group(1);
        String telnorGroup = matcher.group(2);

        if (telmexGroup == null && telnorGroup == null) {
            log.warn("Could not determine company from filename: {}, defaulting to telmex", filename);
            return "telmex";
        }

        if (telmexGroup != null) {
            return "telmex";
        }

        return "telnor";
    }


}


