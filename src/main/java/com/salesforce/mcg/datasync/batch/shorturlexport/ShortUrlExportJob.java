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

package com.salesforce.mcg.datasync.batch.shorturlexport;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Configuration class for Short URL Export batch job.
 * Exports previous day's short URL records from PostgreSQL to CSV on SFTP.
 *
 * @author AI Generated
 * @since 2024-11-24
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class ShortUrlExportJob {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final ShortUrlExportTasklet shortUrlExportTasklet;

    /**
     * Creates the Job bean for Short URL export.
     * Exports previous day's records to CSV file on SFTP.
     *
     * @param stepExecutionListener the step execution listener
     * @return the configured Job
     */
    @Bean(name = "shortUrlExportJobBean")
    public Job shortUrlExportJob(StepExecutionListener stepExecutionListener) {

        Step exportStep = new StepBuilder("shortUrlExportStep", jobRepository)
                .tasklet(shortUrlExportTasklet, transactionManager)
                .listener(stepExecutionListener)
                .build();

        return new JobBuilder("shortUrlExportJob", jobRepository)
                .start(exportStep)
                .build();
    }
}

