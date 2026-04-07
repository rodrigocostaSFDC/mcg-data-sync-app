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

import com.salesforce.mcg.datasync.repository.JobExecutionHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * JDBC repository for querying Spring Batch standard metadata tables.
 */
@Repository
@RequiredArgsConstructor
public class JobExecutionHistoryJdbcRepository implements JobExecutionHistoryRepository {

    static final String LAST_SUCCESSFUL_EXECUTION_SQL = """
        SELECT MAX(je.END_TIME)
        FROM datasync_sch.BATCH_JOB_EXECUTION je
        INNER JOIN datasync_sch.BATCH_JOB_INSTANCE ji
            ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID
        WHERE ji.JOB_NAME = ?
          AND je.STATUS = 'COMPLETED'
          AND je.END_TIME IS NOT NULL
       """;

    private final JdbcTemplate jdbcTemplate;

    @Override
    public Optional<LocalDateTime> findLastSuccessfulExecutionTime(String jobName) {
        if (jobName == null || jobName.isBlank()) {
            throw new IllegalArgumentException("jobName must not be blank");
        }

        Timestamp result = jdbcTemplate.queryForObject(
                LAST_SUCCESSFUL_EXECUTION_SQL,
                Timestamp.class,
                jobName
        );

        return Optional.ofNullable(result).map(Timestamp::toLocalDateTime);
    }
}
