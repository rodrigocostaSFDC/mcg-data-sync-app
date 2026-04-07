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

package com.salesforce.mcg.datasync.tasklet;

import com.salesforce.mcg.datasync.data.Operator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multi-file tasklet for processing multiple operator files as a single grouped job.
 * This tasklet processes all operator files (1 per company) sequentially within one job execution.
 */
@Component
@Slf4j
public class OperatorImportCSVImportCSVTasklet extends AbstractImportCSVTasklet<Operator> {

    public static final String INSERT_SQL = """
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
			
    public static final String FILE_NAME = "fileName";
    public static final String SFTP = "sftp";
    public static final String COMMA = ",";
	
    public static final String OPERADOR = "OPERADOR";
    public static final String NOMBRE_OPERADOR = "NOMBRE_OPERADOR";
    public static final String TIPO_OPERADOR = "TIPO_OPERADOR";
    public static final String RAO = "RAO";
    public static final String CTA_MAESTRA = "CTA_MAESTRA";
    public static final String CTA_MAESTRA_ADICIONAL = "CTA_MAESTRA_ADICIONAL";
    public static final String OPERADOR_VIRTUAL = "OPERADOR_VIRTUAL";
    public static final String OPERADOR_DUAL = "OPERADOR_DUAL";
    public static final String CTA_MAESTRA_11 = "CTA_MAESTRA_11";
    public static final String CTA_MAESTRA_16 = "CTA_MAESTRA_16";
    public static final String CTA_MAESTRA_TELNOR = "CTA_MAESTRA_TELNOR";
    public static final String ID_RVTA_MAYORISTA = "ID_RVTA_MAYORISTA";

    /**
     * Process a single CSV line and return Operator object, or null if the line should be skipped.
     * Updated to use dynamic column mapping based on header names and extract all columns.
     */
    @Override
    public Operator parseLine(
            String fileName,
            String line,
            long lineNumber,
            Map<String, Integer> columnMap) throws Exception {

        var v = line.split(COMMA, -1);

        // Get column indexes dynamically based on header names
        var operadorIndex = columnMap.get(OPERADOR);
        var nombreOperadorIndex = columnMap.get(NOMBRE_OPERADOR);
        var tipoOperadorIndex = columnMap.get(TIPO_OPERADOR);
        var raoIndex = columnMap.get(RAO);
        var ctaMaestraIndex = columnMap.get(CTA_MAESTRA);
        var ctaMaestraAdicionalIndex = columnMap.get(CTA_MAESTRA_ADICIONAL);
        var operadorVirtualIndex = columnMap.get(OPERADOR_VIRTUAL);
        var operadorDualIndex = columnMap.get(OPERADOR_DUAL);
        var ctaMaestra11Index = columnMap.get(CTA_MAESTRA_11);
        var ctaMaestra16Index = columnMap.get(CTA_MAESTRA_16);
        var ctaMaestraTelnorIndex = columnMap.get(CTA_MAESTRA_TELNOR);
        var idRvtaMayoristaIndex = columnMap.get(ID_RVTA_MAYORISTA);
        var idxIndex = columnMap.get("IDX");

        // Validate required columns exist
        if (operadorIndex == null || nombreOperadorIndex == null) {
            throw new IllegalArgumentException("❌ Missing required columns. Expected: OPERADOR, NOMBRE_OPERADOR");
        }

        // Validate we have enough columns for the required indexes
        int maxRequiredIndex = Math.max(operadorIndex, nombreOperadorIndex);

        if (v.length <= maxRequiredIndex) {
            log.warn("⚠️ Skipping operator line {} - insufficient columns: {} (expected at least {})",
                    lineNumber, v.length, maxRequiredIndex + 1);
            return null;
        }

        try {
            var operatorId = v[operadorIndex];
            var operatorName = v[nombreOperadorIndex];

            if (operatorName == null || operatorName.trim().isEmpty()) {
                log.warn("⚠️ Skipping operator line {} - empty operator name: {}", lineNumber, line);
                return null;
            }

            return new Operator(
                    operatorId,
                    operatorName.trim(),
                    getColumnValue(v, tipoOperadorIndex),
                    getColumnValue(v, raoIndex),
                    getColumnValue(v, ctaMaestraIndex),
                    getColumnValue(v, ctaMaestraAdicionalIndex),
                    getColumnValue(v, operadorVirtualIndex),
                    getColumnValue(v, operadorDualIndex),
                    getColumnValue(v, ctaMaestra11Index),
                    getColumnValue(v, ctaMaestra16Index),
                    getColumnValue(v, ctaMaestraTelnorIndex),
                    getColumnValue(v, idRvtaMayoristaIndex),
                    getColumnValue(v, idxIndex));

        } catch (Exception e) {
            log.warn("⚠️ Skipping line {} - processing error: {} - Exception: {}", lineNumber, line, e.getMessage());
            return null;
        }
    }

    @Override
    public void prepareStatement(
            Operator operator,
            PreparedStatement ps,
            AtomicInteger batchSize) throws SQLException {
        if (Objects.nonNull(operator)) {
            ps.setString(1, operator.operator());
            ps.setString(2, operator.name());
            ps.setString(3, operator.type());
            ps.setString(4, operator.rao());
            ps.setString(5, operator.masterAccount());
            ps.setString(6, operator.additionalMasterAccount());
            ps.setString(7, operator.virtualOperator());
            ps.setString(8, operator.dualOperator());
            ps.setString(9, operator.masterAccount11());
            ps.setString(10, operator.masterAccount16());
            ps.setString(11, operator.masterAccountTelnor());
            ps.setString(12, operator.idRvtaMayorista());
            ps.setString(13, operator.idx());
            ps.addBatch();
            batchSize.incrementAndGet();
        }
    }

    @Override
    public String insertSQL(){
        return INSERT_SQL;
    }

}
