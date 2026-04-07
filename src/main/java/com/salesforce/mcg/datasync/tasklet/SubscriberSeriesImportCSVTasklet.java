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

package com.salesforce.mcg.datasync.newbatch.tasklet;

import com.salesforce.mcg.datasync.newbatch.data.SubscriberSeries;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tasklet that processes multiple subscriber series files as a single grouped job.
 * This allows the 3 series files to be processed together as one logical unit.
 */

@Slf4j
@Component
public class SubscriberSeriesImportCSVTasklet extends AbstractImportCSVTasklet<SubscriberSeries> {

    public static final String SPACE = "\\s";
    public static final String AREA = "AREA";
    public static final String SERIE_INICIAL = "SERIE_INICIAL";
    public static final String SERIE_FINAL = "SERIE_FINAL";
    public static final String OPERADOR = "OPERADOR";
    public static final String IDX = "IDX";

    public static final String UPSERT_SQL =
            "insert into datasync_sch.subscriber_series (series_start, series_end, operator, virtual_operator) values (?, ?, ?, ?) " +
                    "on conflict (series_start, series_end) do update set operator = excluded.operator, virtual_operator = excluded.virtual_operator";

    /**
     * Process a single CSV line and return SubscriberSeries object, or null if the line should be skipped.
     * Updated to use dynamic column mapping based on header names.
     */
    private SubscriberSeries processLine(
            String line,
            long lineNumber,
            Map<String, Integer> columnMap) throws Exception {
        String[] v = line.replaceAll(SPACE, Strings.EMPTY).split(COMMA, -1);

        // Get column indexes dynamically based on header names
        Integer areaIndex = columnMap.get(AREA);
        Integer serieInicialIndex = columnMap.get(SERIE_INICIAL);
        Integer serieFinalIndex = columnMap.get(SERIE_FINAL);
        Integer operadorIndex = columnMap.get(OPERADOR);
        Integer virtualOperatorIndex = columnMap.get(IDX);
        
        // Validate required columns exist
        if (Objects.nonNull(areaIndex)
                || Objects.nonNull(serieInicialIndex)
                || Objects.nonNull(serieFinalIndex)
                || Objects.nonNull(operadorIndex)) {
            throw new IllegalArgumentException("❌ Missing required columns. Expected: AREA, SERIE_INICIAL, SERIE_FINAL, OPERADOR");
        }
        
        // Validate we have enough columns for all required indexes
        int maxRequiredIndex = Math.max(Math.max(areaIndex, serieInicialIndex),
                                       Math.max(serieFinalIndex, operadorIndex));
        
        // Include optional columns in max index calculation if they exist
        if (Objects.nonNull(virtualOperatorIndex)) {
            maxRequiredIndex = Math.max(maxRequiredIndex, virtualOperatorIndex);
        }
        
        if (v.length <= maxRequiredIndex) {
            throw new IllegalArgumentException(String.format("Line [%s] Invalid number of columns: %d (expected at least %d) - SKIPPED", 
                    lineNumber, v.length, maxRequiredIndex + 1));
        }

        long seriesStart;
        long seriesEnd;
        String operator;

        // Format phone numbers with country code 52
        String area = v[areaIndex];
        String serieInicial = v[serieInicialIndex];
        String countryCode = "52";
        String formattedRangeStart = String.format("%s%s%s", countryCode, area, serieInicial);

        try {
            seriesStart = Long.parseLong(formattedRangeStart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Line [%s] Invalid series start - SKIPPED", lineNumber));
        }

        String serieFinal = v[serieFinalIndex];
        String formattedRangeEnd = String.format("%s%s%s", countryCode, area, serieFinal);
        try {
            seriesEnd = Long.parseLong(formattedRangeEnd);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Line [%s] Invalid series end - SKIPPED", lineNumber));
        }

        // Format operator with zero-padding to 3 digits
        try {
            operator = String.format("%03d", Long.parseLong(v[operadorIndex]));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Line [%s] Invalid operator - SKIPPED", lineNumber));
        }

        // Get idx and virtual operator from CSV, with fallback defaults
        String idx = "";
        String virtualOperator = "";
        if (virtualOperatorIndex != null && virtualOperatorIndex < v.length) {
            String idxValue = v[virtualOperatorIndex].trim();
            if (!idxValue.isEmpty()) {
                idx = idxValue;
                virtualOperator = idxValue; // Use same value for both idx and virtualOperator
            }
        } else {
            log.debug("IDX column not found or out of bounds. Index: {}, Array length: {}", virtualOperatorIndex, v.length);
        }
        

        return SubscriberSeries.builder()
                .seriesStart(seriesStart)
                .seriesEnd(seriesEnd)
                .operator(operator)
                .virtualOperator(virtualOperator)
                .build();
    }

    @Override
    public String insertSQL() {
        return UPSERT_SQL;
    }

    @Override
    public SubscriberSeries parseLine(String fileName, String line, long lineNumber, Map<String, Integer> columnMap) throws Exception {
        return null;
    }

    @Override
    public void prepareStatement(
            SubscriberSeries series,
            PreparedStatement ps,
            AtomicInteger batchSize) throws SQLException {
        if (Objects.nonNull(series)) {
            ps.setLong(1, series.getSeriesStart());
            ps.setLong(2, series.getSeriesEnd());
            ps.setString(3, series.getOperator());
            ps.setString(4, series.getVirtualOperator());
            ps.addBatch();
            batchSize.incrementAndGet();
        }
    }

}
