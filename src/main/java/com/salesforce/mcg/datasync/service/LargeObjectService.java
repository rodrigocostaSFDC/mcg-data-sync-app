package com.salesforce.mcg.datasync.service;

import jakarta.annotation.Resource;
import org.postgresql.PGConnection;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;

@Service
public class LargeObjectService {

    @Resource
    private DataSource ds;

    /**
     * Store an InputStream as a new Large Object. Return its OID.
     */
    public long store(InputStream in) throws Exception {
        Connection c = DataSourceUtils.getConnection(ds);
        boolean auto = c.getAutoCommit();
        try {
            c.setAutoCommit(false);
            PGConnection pg = c.unwrap(PGConnection.class);
            LargeObjectManager lom = pg.getLargeObjectAPI();
            long oid = lom.createLO(LargeObjectManager.WRITE);
            try (LargeObject lo = lom.open(oid, LargeObjectManager.WRITE);
                 OutputStream os = lo.getOutputStream()) {
                byte[] buf = new byte[1 << 20]; // 1 MiB
                int n;
                while ((n = in.read(buf)) >= 0) os.write(buf, 0, n);
            }
            c.commit();
            return oid;
        } catch (Exception e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(auto);
            DataSourceUtils.releaseConnection(c, ds);
            try {
                in.close();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Open an InputStream to an existing LO (read-only). Caller closes it.
     */
    public InputStream open(long oid) throws Exception {
        Connection c = DataSourceUtils.getConnection(ds);
        boolean auto = c.getAutoCommit();
        c.setAutoCommit(false); // LO needs a txn
        PGConnection pg = c.unwrap(PGConnection.class);
        LargeObjectManager lom = pg.getLargeObjectAPI();
        LargeObject lo = lom.open(oid, LargeObjectManager.READ);
        return new java.io.FilterInputStream(lo.getInputStream()) {
            @Override
            public void close() throws java.io.IOException {
                super.close();
                try {
                    lo.close();
                } catch (Exception ignore) {
                }
                try {
                    c.commit();
                } catch (Exception ignore) {
                }
                try {
                    c.setAutoCommit(auto);
                } catch (Exception ignore) {
                }
                DataSourceUtils.releaseConnection(c, ds);
            }
        };
    }

    /**
     * Optionally remove LO after processing to free space.
     */
    public void delete(long oid) throws Exception {
        Connection c = DataSourceUtils.getConnection(ds);
        boolean auto = c.getAutoCommit();
        try {
            c.setAutoCommit(false);
            PGConnection pg = c.unwrap(PGConnection.class);
            pg.getLargeObjectAPI().delete(oid);
            c.commit();
        } catch (Exception e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(auto);
            DataSourceUtils.releaseConnection(c, ds);
        }
    }
}

