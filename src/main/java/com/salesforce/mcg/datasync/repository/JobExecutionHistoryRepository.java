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

package com.salesforce.mcg.datasync.repository;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Repository for reading Spring Batch execution history.
 */
public interface JobExecutionHistoryRepository {

    /**
     * Returns the latest successful execution end time for the given Spring Batch job name.
     *
     * @param jobName value from {@code BATCH_JOB_INSTANCE.JOB_NAME}
     * @return latest successful execution time if any, otherwise empty
     */
    Optional<LocalDateTime> findLastSuccessfulExecutionTime(String jobName);
}
