package com.salesforce.mcg.datasync.service;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;

//@Service
public class PostgresCopyService {

    //@Resource
    private DataSource dataSource;

    /**
     * Copia um CSV para a tabela alvo, usando COPY FROM STDIN.
     * Retorna o número de linhas inseridas.
     */
    public long copyCsv(InputStream in, String table, String columnsCsv) throws Exception {
        Connection con = DataSourceUtils.getConnection(dataSource);
        boolean auto = con.getAutoCommit();
        try {
            con.setAutoCommit(false);

            PGConnection pg = con.unwrap(PGConnection.class);
            CopyManager cm = pg.getCopyAPI();

            String sql = "COPY " + table +
                    (columnsCsv != null && !columnsCsv.isBlank() ? "(" + columnsCsv + ")" : "") +
                    " FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"')";

            long rows = cm.copyIn(sql, in);
            con.commit();
            return rows;
        } catch (Exception e) {
            con.rollback();
            throw e;
        } finally {
            con.setAutoCommit(auto);
            DataSourceUtils.releaseConnection(con, dataSource);
            if (in != null) try {
                in.close();
            } catch (Exception ignore) {
            }
        }
    }
}