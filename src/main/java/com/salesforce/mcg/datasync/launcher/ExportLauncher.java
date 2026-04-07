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

package com.salesforce.mcg.datasync.launcher;

import com.salesforce.mcg.datasync.service.SftpService;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Application Runner implementation that lists data files from a remote SFTP directory
 * and launches configured Spring Batch jobs to process these files.
 * Now supports individual files (CSV, TXT, GZ, DAT) instead of ZIP files.
 * <p>
 * Exception handling is performed to log and propagate critical errors.
 * This class is designed to run as a standalone application or Heroku dyno.
 */
@Component
@ConditionalOnProperty(value = "app.mode", havingValue = "export")
@RequiredArgsConstructor
@Slf4j
public class ExportLauncher implements ApplicationRunner {

    @Resource(name = "dataSyncSftpService")
    private SftpService sftpService;

    @Resource
    private SftpPropertyContext propertyContext;

    @Value("${app.auto-shutdown:true}")
    private boolean autoShutdown;

    private final ApplicationContext applicationContext;
    private final JobLauncher jobLauncher;

    private final AtomicInteger jobsProcessed = new AtomicInteger(0);
    private final AtomicInteger jobsSuccessful = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);

    /**
     * Runs the Short URL Export job (no file dependency).
     * Exports short URL records to CSV on SFTP.
     * <p></p>
     * Supports four modes:
     * - Mode 1 (default): Export records created since last export
     * - Mode 2: Export records created OR updated since last export (EXPORT_UPDATES=true)
     * - Mode 3: Export records for a specific date range (--exportDate or --exportDateStart/--exportDateEnd)
     * - Mode 4: Export records for a specific campaign on a specific day (--exportApiKey + --exportDate)
     * </p>
     */
    @Override
    public void run(ApplicationArguments args) {

        log.info("🚀 Starting Short URL Export job...");

        try {
            Job job = applicationContext.getBean("shortUrlExportCSVJob", Job.class);
            JobParametersBuilder paramsBuilder = new JobParametersBuilder();
            paramsBuilder.addLong("timestamp", System.currentTimeMillis());
            var exportDateStart = getArgValueOrElseNull("startDate", args);
            if (Objects.nonNull(exportDateStart)) paramsBuilder.addString("startDate", exportDateStart);
            var exportDateEnd = getArgValueOrElseNull("endDate", args);
            if (Objects.nonNull(exportDateEnd)) paramsBuilder.addString("endDate", exportDateEnd);
            var exportDate = getArgValueOrElseNull("date", args);
            if (Objects.nonNull(exportDate)) paramsBuilder.addString("date", exportDate);
            var exportApiKey = getArgValueOrElseNull("apiKey", args);
            if (Objects.nonNull(exportApiKey)) paramsBuilder.addString("apiKey", exportApiKey);

            jobLauncher.run(job, paramsBuilder.toJobParameters());

            jobsProcessed.incrementAndGet();
            jobsSuccessful.incrementAndGet();
            log.info("✅ Successfully completed Short URL Export job");

        } catch (Exception jobEx) {
            jobsFailed.incrementAndGet();
            jobsProcessed.incrementAndGet();
            log.error("❌ Short URL Export job failed", jobEx);
        }

    }
    
    /**
     * Shuts down the Spring Boot application, which will terminate the dyno.
     * 
     * @param exitCode the exit code (0 for success, non-zero for failure)
     */
    private void shutdownApplication(int exitCode) {
        log.info("Initiating application shutdown with exit code: {}", exitCode);
        
        // Schedule shutdown in a separate thread to allow current operations to complete
        new Thread(() -> {
            try {
                Thread.sleep(2000); // Give 2 seconds for logging to complete
                log.info("🔌 Shutting down dyno now...");
                System.exit(exitCode);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("❌ Shutdown interrupted");
                System.exit(exitCode);
            }
        }, "shutdown-thread").start();
    }

    private String getArgValueOrElseNull(String name, ApplicationArguments args){
        if (args.containsOption(name)
                && Strings.isNotBlank(args.getOptionValues(name).get(0))) {
            return args
                    .getOptionValues(name)
                    .get(0);
        } else {
            return null;
        }
    }

}

