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
 ***************************************************************************/

package com.salesforce.mcg.datasync.listener;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.datasync.repository.DataSyncConfigRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import java.util.ArrayList;
import java.util.List;

import static com.salesforce.mcg.datasync.common.AppConstants.Sftp;

/**
 * Step execution and skip event listener for batch processing subscriber portability imports.
 * <p>
 * Handles skip notifications during read, process, and write phases, logs skips,
 * and manages important job step events including before and after execution logic.
 * </p>
 */
@Slf4j
public class DataSyncStepExecutionListener implements StepExecutionListener {

    private final Session session;
    private final List<String> skips = new ArrayList<>();
    private final DataSyncConfigRepository configRepository;

    /**
     * Constructs an DataSyncStepExecutionListener with required repository and SFTP channel.
     *
     * @param session    the SFTP channel for file operations
     */
    public DataSyncStepExecutionListener(Session session, DataSyncConfigRepository configRepository) {
        this.session = session;
        this.configRepository = configRepository;
    }

    /**
     * Invoked before the step execution. Puts the cutoff date in the job's execution context
     * if the step name matches the expected suffix.
     *
     * @param stepExecution current step context
     */
    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {

    }

    /**
     * Invoked after the step execution. When the step has completed and matches expected name,
     * attempts to move a processed file in SFTP.
     *
     * @param stepExecution current step context
     * @return ExitStatus - null if not custom exit
     */
    @Override
    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
        try {
            var config = configRepository.read().get();
            if (shouldMoveRemoteFile(stepExecution) && config.getMoveAfterCopy()) {
                var remoteSrc = stepExecution.getJobParameters().getString("AA");
                var remoteDst = String.format("%s/%s_%s", config.getMovePath(), remoteSrc, System.currentTimeMillis());
                var sftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
                sftp.connect();
                sftp.rename(remoteSrc, remoteDst);
                sftp.disconnect();
                log.info("✅ Successfully moved file {} to {}", remoteSrc, remoteDst);
            }
            return ExitStatus.COMPLETED;
        } catch (Exception e) {
            log.error("❌ Error moving file due error: {}", e.getMessage(), e);
            return ExitStatus.FAILED;
        }
    }

    private boolean shouldMoveRemoteFile(@NonNull StepExecution stepExecution) {
        return stepExecution.getStepName().endsWith("ImportStep")
                && stepExecution.getStatus() == BatchStatus.COMPLETED;
    }

}