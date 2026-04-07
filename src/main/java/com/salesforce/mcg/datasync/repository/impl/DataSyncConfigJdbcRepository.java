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

package com.salesforce.mcg.datasync.repository.impl;

import com.salesforce.mcg.datasync.data.DataSyncConfig;
import com.salesforce.mcg.datasync.repository.DataSyncConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JDBC-based implementation of the {@link DataSyncConfigRepository} interface.
 * Provides methods for inserting, reading, and updating {@link DataSyncConfig}
 * entries from the database.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class DataSyncConfigJdbcRepository implements DataSyncConfigRepository {

    /**
     * Maps database rows to {@link DataSyncConfig} objects.
     */
    public static final RowMapper<DataSyncConfig> MAPPER = (rs, rowNum) -> {
        DataSyncConfig config = new DataSyncConfig();
        int retentionTime = rs.getInt("cleanup_retention_time_in_days");
        config.setSyncSchedulerCronExpression(rs.getString("sync_scheduler_cron_expression"));
        config.setCleanUpSchedulerCronExpression(rs.getString("cleanup_scheduler_cron_expression"));
        config.setMoveAfterCopy(rs.getBoolean("move_after_copy"));
        config.setMovePath(rs.getString("move_path"));
        config.setKeepLastFileQuantity(rs.getInt("keep_last_file_quantity"));
        config.setCleanUpRetentionTimeInDays(rs.wasNull() ? null : retentionTime);
        return config;
    };
    private final JdbcTemplate jdbc;

    /**
     * Reads the configuration from the database.
     *
     * @return an {@link Optional} containing the {@link DataSyncConfig} if present, otherwise empty
     */
    @Override
    public Optional<DataSyncConfig> read() {
        try {
            DataSyncConfig config = jdbc.queryForObject(
                    "SELECT * FROM datasync_sch.datasync_config WHERE id=1",
                    MAPPER
            );
            log.debug("Read Config: {}", config);
            return Optional.ofNullable(config);
        } catch (EmptyResultDataAccessException e) {
            log.debug("No Config row found (id=1).");
            return Optional.empty();
        }
    }

    /**
     * Inserts the given {@link DataSyncConfig} into the database.
     *
     * @param config the configuration to insert
     * @return the number of rows affected by the insert
     */
    @Override
    public int insert(DataSyncConfig config) {
        log.debug("Inserting Config: {}", config);
        return jdbc.update(
                """
                        INSERT INTO datasync_sch.datasync_config (
                            id,
                            sync_scheduler_cron_expression,
                            cleanup_scheduler_cron_expression,
                            cleanup_retention_time_in_days,
                            move_after_copy,
                            move_path,
                            keep_last_file_quantity
                        ) VALUES (1, ?, ?, ?, ?, ? , ?)
                        """,
                config.getSyncSchedulerCronExpression(),
                config.getCleanUpSchedulerCronExpression(),
                config.getCleanUpRetentionTimeInDays(),
                config.getMoveAfterCopy(),
                config.getMovePath(),
                config.getKeepLastFileQuantity()
        );
    }

    /**
     * Updates the existing {@link DataSyncConfig} in the database.
     *
     * @param config the configuration with updated values
     * @return the number of rows affected by the update
     */
    @Override
    public int update(DataSyncConfig config) {
        log.debug("Updating DataSyncConfig: {}", config);
        return jdbc.update(
                """
                        UPDATE datasync_sch.datasync_config
                        SET
                            sync_scheduler_cron_expression = ?,
                            cleanup_scheduler_cron_expression = ?,
                            cleanup_retention_time_in_days = ?,
                            move_after_copy = ?,
                            move_path = ?,
                            keep_last_file_quantity = ?
                        WHERE id = 1
                        """,
                config.getSyncSchedulerCronExpression(),
                config.getCleanUpSchedulerCronExpression(),
                config.getCleanUpRetentionTimeInDays(),
                config.getMoveAfterCopy(),
                config.getMovePath(),
                config.getKeepLastFileQuantity()
        );
    }
}

