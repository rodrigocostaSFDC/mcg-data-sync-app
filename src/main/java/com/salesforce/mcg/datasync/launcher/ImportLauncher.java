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

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.datasync.data.FileDiscoveryResult;
import com.salesforce.mcg.datasync.service.SftpService;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Application Runner implementation that lists data files from a remote SFTP directory
 * and launches configured Spring Batch jobs to process these files.
 * Now supports individual files (CSV, TXT, GZ, DAT) instead of ZIP files.
 * <p>
 * Exception handling is performed to log and propagate critical errors.
 * This class is designed to run as a standalone application or Heroku dyno.
 */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "app.mode", havingValue = "import")
@Slf4j
public class ImportLauncher implements ApplicationRunner {

    @Resource(name = "dataSyncSftpService")
    private SftpService sftpService;

    @Resource
    private SftpPropertyContext propertyContext;

    @Resource
    private Map<Pattern, Job> jobResolverMap;

    @Value("${app.auto-shutdown:true}")
    private boolean autoShutdown;

    private final ApplicationContext applicationContext;

    private final JobLauncher jobLauncher;
    
    private final AtomicInteger jobsProcessed = new AtomicInteger(0);
    private final AtomicInteger jobsSuccessful = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);

    @Override
    public void run(ApplicationArguments args) {

        log.info("🚀 Starting Import Jobs...");

        var props = propertyContext.getPropertiesForActiveCompany();
        var subfolderPath = props.outputDir();

        log.info("🔍 Discovering files in subfolder: {}", subfolderPath);
        try {

            List<String> files = sftpService.listFiles(subfolderPath);
            if (files.isEmpty()){
                log.warn("⚠️ Folder is empty!");
                return;
            }

            for (String file : files) {
                var jobs = jobResolverMap.keySet().stream()
                        .filter(pattern -> pattern.matcher(file).matches())
                        .map(pattern -> jobResolverMap.get(pattern))
                        .toList();
                if (jobs.isEmpty()){
                    log.warn("⚠️ No processors found for file named '{}' and will not be processed.",
                            file);
                } else {
                    log.info("✅️ Processor '{}' found for file named '{}'. Adding to processing Queue,",
                            jobs.get(0).getName(),
                            file);
                }
            }
            //All files added to execution queue.
            //Starting the first
            //Register to be notified when finished

        } catch (JSchException | SftpException e) {
            log.error("❌ Unable to read files from remove server. Error: {}",
                    e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Launches a configured Spring Batch job for the specified file name.
     * Pulls the job bean from the application context and executes it with file-based parameters.
     *
     * @param fileName    the remote CSV file name to be processed
     * @param job         the Spring Batch Job to execute
     * @throws JobExecutionAlreadyRunningException if the batch job is already running
     * @throws JobRestartException                 if the batch job cannot be restarted
     * @throws JobInstanceAlreadyCompleteException if the batch job instance is already complete
     * @throws JobParametersInvalidException       if provided job parameters are invalid
     */
    public void launchBatchJob(String fileName, Job job)
            throws JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException,
            JobParametersInvalidException {
        
        JobParameters params = new JobParametersBuilder()
                .addLong("ts", System.currentTimeMillis(), true)
                //.addString(JobConfig.FILE_NAME, fileName, true)
                .addString("live", "datasync_sch.subscriber_portability")
                .addString("candidate", "datasync_sch.subscriber_portability_staging")
                .toJobParameters();

        log.info("Launching batch job for file: {}", fileName);
        jobLauncher.run(job, params);
    }

    /**
     * Launch a grouped job with multiple files passed as comma-separated parameter
     * 
     * @param fileNames the list of file names to process as a group
     * @param job the Spring Batch job to execute
     * @param jobType the type of job (for logging and parameters)
     * @throws JobExecutionAlreadyRunningException if the batch job is already running
     * @throws JobRestartException                 if the batch job cannot be restarted
     * @throws JobInstanceAlreadyCompleteException if the batch job instance is already complete
     * @throws JobParametersInvalidException       if provided job parameters are invalid
     */
    public void launchGroupJob(List<String> fileNames, Job job, String jobType)
            throws JobExecutionAlreadyRunningException,
            JobRestartException,
            JobInstanceAlreadyCompleteException,
            JobParametersInvalidException {
        
        String fileList = String.join(",", fileNames);
        
        JobParameters params = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                //.addString(JobConfig.FILE_NAME, fileList)
                .addString("fileCount", String.valueOf(fileNames.size()))
                .addString("jobType", jobType)
                .toJobParameters();

        log.info("Launching grouped {} job for {} files: {}", jobType, fileNames.size(), fileList);
        jobLauncher.run(job, params);
    }

    /**
     * Executes the sync job by retrieving data files from remote SFTP directories
     * and launching a batch job for each file. Uses priority-based file discovery:
     * 1. First checks subfolder (portados_series) for single files
     * 2. Falls back to root directory for multiple files if subfolder files aren't found
     *
     * @param requestedJob optional job name to execute. If null or not found, all jobs will run.
     *                     Valid values: "operator", "sheet", "series", "portability", "shorturlexport"
     * @throws RuntimeException if unable to retrieve SFTP files or launch jobs
     */
    public void execute(String requestedJob) {
        int totalFiles = 0;
        try {

            var props = propertyContext.getPropertiesForActiveCompany();
            var subfolderPath = props.outputDir();

            String portabilityFile = null;
            String seriesFile = null;
            String operatorFile = null;
            List<String> sheetFiles = new ArrayList<>();

            
            // Discover files from SFTP
            FileDiscoveryResult discoveryResult = discoverFiles();
//            totalFiles = discoveryResult.getTotalFileCount();
//
//            // Get files from discovery result
//            String operatorFile = discoveryResult.getOperatorFile();
//            List<String> sheetFiles = discoveryResult.getSheetFiles();
//            String seriesFile = discoveryResult.getSeriesFile();
//            String portabilityFile = discoveryResult.getPortabilityFile();
//
//            log.info("ℹ️ Files found - Operator: {}, Sheets: {}, Series: {}, Portability: {}",
//                    operatorFile != null ? "yes" : "no",
//                    sheetFiles.size(),
//                    seriesFile != null ? "yes" : "no",
//                    portabilityFile != null ? "yes" : "no");

            
            // 2. Process operator file
            if (shouldRunJob(requestedJob, "operator") && operatorFile != null) {
                try {
                    Job job = applicationContext.getBean("operatorJobBean", Job.class);
                    launchBatchJob(operatorFile, job);
                    jobsSuccessful.incrementAndGet();
                    log.info("🏁 Successfully completed operator job for file: {}", operatorFile);
                } catch (Exception jobEx) {
                    jobsFailed.incrementAndGet();
                    log.error("❌ Operator job failed for file: {}", operatorFile, jobEx);
                }
                jobsProcessed.incrementAndGet();
            } else if (requestedJob != null && "operator".equalsIgnoreCase(requestedJob)) {
                log.warn("❌ Operator job requested but no operator file found");
            }
            
            // 3. Process sheet files (from parent folder)
            if (shouldRunJob(requestedJob, "sheet") && !sheetFiles.isEmpty()) {
                try {
                    log.info("Processing {} sheet files: {}", sheetFiles.size(), sheetFiles);
                    Job job = applicationContext.getBean("sheetJobBean", Job.class);
                    launchGroupJob(sheetFiles, job, "sheet");
                    jobsSuccessful.incrementAndGet();
                    log.info("Successfully completed sheet job for {} files", sheetFiles.size());
                } catch (Exception jobEx) {
                    jobsFailed.incrementAndGet();
                    log.error("Sheet job failed for files: {}", sheetFiles, jobEx);
                }
                jobsProcessed.incrementAndGet();
            } else if (requestedJob != null && "sheet".equalsIgnoreCase(requestedJob)) {
                log.warn("Sheet job requested but no sheet files found");
            }
            
            // 4. Process series file
            if (shouldRunJob(requestedJob, "series") && seriesFile != null) {
                try {
                    Job job = applicationContext.getBean("subscriberSeriesJobBean", Job.class);
                    launchBatchJob(seriesFile, job);
                    jobsSuccessful.incrementAndGet();
                    log.info("Successfully completed series job for file: {}", seriesFile);
                } catch (Exception jobEx) {
                    jobsFailed.incrementAndGet();
                    log.error("Series job failed for file: {}", seriesFile, jobEx);
                }
                jobsProcessed.incrementAndGet();
            } else if (requestedJob != null && "series".equalsIgnoreCase(requestedJob)) {
                log.warn("Series job requested but no series file found");
            }
            
            // 5. Process portability file
            if (shouldRunJob(requestedJob, "portability") && portabilityFile != null) {
                try {
                    log.info("Using PostgreSQL COPY-based portability job for file: {}", portabilityFile);
                    Job job = applicationContext.getBean("subscriberPortabilityJobBean", Job.class);
                    launchBatchJob(portabilityFile, job);
                    jobsSuccessful.incrementAndGet();
                    log.info("Successfully completed portability job for file: {}", portabilityFile);
                } catch (Exception jobEx) {
                    jobsFailed.incrementAndGet();
                    log.error("Portability job failed for file: {}", portabilityFile, jobEx);
                }
                jobsProcessed.incrementAndGet();
            } else if (requestedJob != null && "portability".equalsIgnoreCase(requestedJob)) {
                log.warn("Portability job requested but no portability file found");
            }
            
        } catch (SftpException e) {
            log.error("Unable to list files from SFTP due to error: {}", e.getMessage());
            shutdownApplication(1);
            throw new RuntimeException(e);
        } catch (JSchException e) {
            log.error("❌ SFTP connection error: {}", e.getMessage());
            shutdownApplication(1);
            throw new RuntimeException(e);
        } finally {
            // Log final statistics
            log.info("🏁Job execution completed. Total files: {}, Processed: {}, Successful: {}, Failed: {}",
                    totalFiles, jobsProcessed.get(), jobsSuccessful.get(), jobsFailed.get());
            
            // Shutdown the application if auto-shutdown is enabled
            if (autoShutdown) {
                int exitCode = jobsFailed.get() > 0 ? 1 : 0;
                shutdownApplication(exitCode);
            }
        }
    }
    
    /**
     * Shuts down the Spring Boot application, which will terminate the dyno.
     * 
     * @param exitCode the exit code (0 for success, non-zero for failure)
     */
    private void shutdownApplication(int exitCode) {
        log.info("🚀 Initiating application shutdown with exit code: {}", exitCode);
        
        // Schedule shutdown in a separate thread to allow current operations to complete
        new Thread(() -> {
            try {
                Thread.sleep(2000); // Give 2 seconds for logging to complete
                log.info("🔌 Shutting down dyno now...");
                System.exit(exitCode);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("⚠️ Shutdown interrupted");
                System.exit(exitCode);
            }
        }, "shutdown-thread").start();
    }

    /**
     * Discovers and categorizes files from SFTP directories.
     * - Operator, Series, Portability files: from subfolder (portados_series)
     * - Sheet files: from parent folder (remoteDir), pattern *_Catalogo_Plantillas_SMS
     * 
     * @return FileDiscoveryResult containing categorized files
     * @throws SftpException if SFTP operations fail
     * @throws JSchException if SFTP connection fails
     */
    private FileDiscoveryResult discoverFiles() throws SftpException, JSchException {
        var props = propertyContext.getPropertiesForActiveCompany();
        var subfolderPath = props.outputDir();
        
        String portabilityFile = null;
        String seriesFile = null;
        String operatorFile = null;
        List<String> sheetFiles = new ArrayList<>();
        
        // Discover operator, series, portability from subfolder
        log.info("🔍 Discovering files in subfolder: {}", subfolderPath);
        try {
            List<String> files = sftpService.listFiles(subfolderPath);
            for (String fileName : files) {
                String filenameLower = fileName.toLowerCase();
                String fullPath = subfolderPath + "/" + fileName;
                
                if (filenameLower.contains("fin_portados")) {
                    portabilityFile = fullPath;
                    log.debug("🔍 Found portability file: {}", fileName);
                } else if (filenameLower.contains("fin_series")) {
                    seriesFile = fullPath;
                    log.debug("🔍Found series file: {}", fileName);
                } else if (filenameLower.contains("fin_operador")) {
                    operatorFile = fullPath;
                    log.debug("🔍 Found operator file: {}", fileName);
                } else {
                    log.debug("⚠️ Unrecognized file in subfolder: {}", fileName);
                }
            }
        } catch (SftpException e) {
            log.warn("⚠️ Subfolder {} not accessible: {}", subfolderPath, e.getMessage());
        }
        
        // Discover sheet files from parent folder
        log.info("🔍Discovering sheet files in parent folder: {}", props.inputDir());
        try {
            List<String> files = sftpService.listFiles(props.inputDir());
            for (String fileName : files) {
                String filenameLower = fileName.toLowerCase();
                if (filenameLower.contains("catalogo_plantillas_sms")) {
                    String fullPath = props.inputDir() + "/" + fileName;
                    sheetFiles.add(fullPath);
                    log.debug("Found sheet file: {}", fileName);
                }
            }
        } catch (SftpException e) {
            log.warn("Parent folder {} not accessible: {}", props.inputDir(), e.getMessage());
        }
        
        return FileDiscoveryResult.builder()
                .portabilityFile(portabilityFile)
                .seriesFile(seriesFile)
                .operatorFile(operatorFile)
                .sheetFiles(sheetFiles)
                .build();
    }

    /**
     * Determines if a specific job should be executed based on the requested job name.
     * 
     * @param requestedJob the job name requested via command line parameter (can be null)
     * @param jobType the type of job to check (operator, sheet, series, portability)
     * @return true if the job should run, false otherwise
     */
    private boolean shouldRunJob(String requestedJob, String jobType) {
        // If no specific job is requested, run all jobs
        if (requestedJob == null) {
            return true;
        }
        // If a specific job is requested, only run if it matches this job type
        return jobType.equalsIgnoreCase(requestedJob);
    }
    
    /**
     * Validates if the provided job name is a valid job type.
     * 
     * @param jobName the job name to validate
     * @return true if valid, false otherwise
     */
    private boolean isValidJobName(String jobName) {
        if (jobName == null) {
            return false;
        }
        String normalizedName = jobName.toLowerCase();
        return normalizedName.equals("operator") || 
               normalizedName.equals("sheet") || 
               normalizedName.equals("series") || 
               normalizedName.equals("portability");
    }


}

