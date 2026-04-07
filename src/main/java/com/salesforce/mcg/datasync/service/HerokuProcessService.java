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

package com.salesforce.mcg.datasync.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Service for managing Heroku processes using the Heroku CLI.
 * Provides functionality to check for running processes, stop processes, and launch new ones.
 */
@Service
@Slf4j
public class HerokuProcessService {

    @Value("${heroku.app.name:}")
    private String herokuAppName;

    @Value("${heroku.cli.timeout:30}")
    private int cliTimeoutSeconds;

    /**
     * Check if any 'run' process types are currently running on the Heroku app.
     *
     * @return true if any run processes are active, false otherwise
     */
    public boolean hasRunningJobs() {
        if (Strings.isBlank(herokuAppName)) {
            log.warn("Heroku app name not configured, assuming no running jobs");
            return false;
        }

        try {
            List<String> command = List.of("heroku", "ps", "--app", herokuAppName);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            
            Process process = pb.start();
            
            boolean finished = process.waitFor(cliTimeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.error("❌ Heroku ps command timed out after {} seconds", cliTimeoutSeconds);
                return false;
            }

            if (process.exitValue() != 0) {
                log.error("❌ Heroku ps command failed with exit code: {}", process.exitValue());
                return false;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Look for lines containing 'run.' which indicates a run process type
                    if (line.contains("run.") && (line.contains("up") || line.contains("starting"))) {
                        log.info("✅ Found running job process: {}", line.trim());
                        return true;
                    }
                }
            }

            log.info("⚠️ No running job processes found");
            return false;

        } catch (IOException | InterruptedException e) {
            log.error("❌ Error checking for running jobs: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Get details of currently running 'run' processes.
     *
     * @return list of running process descriptions
     */
    public List<String> getRunningJobDetails() {
        List<String> runningJobs = new ArrayList<>();
        
        if (Strings.isBlank(herokuAppName)) {
            return runningJobs;
        }

        try {
            List<String> command = List.of("heroku", "ps", "--app", herokuAppName);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            
            Process process = pb.start();
            
            boolean finished = process.waitFor(cliTimeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.error("❌ Heroku ps command timed out");
                return runningJobs;
            }

            if (process.exitValue() != 0) {
                log.error("❌ Heroku ps command failed");
                return runningJobs;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("run.") && (line.contains("up") || line.contains("starting"))) {
                        runningJobs.add(line.trim());
                    }
                }
            }

        } catch (IOException | InterruptedException e) {
            log.error("❌ Error getting running job details: {}", e.getMessage(), e);
        }

        return runningJobs;
    }

    /**
     * Stop all running 'run' process types on the Heroku app.
     *
     * @return true if the stop command was executed successfully, false otherwise
     */
    public boolean stopRunningJobs() {
        if (herokuAppName == null || herokuAppName.trim().isEmpty()) {
            log.error("❌ Heroku app name not configured, cannot stop jobs");
            return false;
        }

        try {
            List<String> command = List.of("heroku", "ps:stop", "--process-type", "run", "--app", herokuAppName);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            
            log.info("🔌 Stopping running jobs with command: {}", String.join(" ", command));
            
            Process process = pb.start();
            
            boolean finished = process.waitFor(cliTimeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.error("❌ Heroku ps:stop command timed out");
                return false;
            }

            int exitCode = process.exitValue();
            
            // Log the output for debugging
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info("ℹ️ Heroku ps:stop output: {}", line);
                }
            }

            if (exitCode == 0) {
                log.info("🏁 Successfully stopped running jobs");
                return true;
            } else {
                log.error("❌ Heroku ps:stop command failed with exit code: {}", exitCode);
                return false;
            }

        } catch (IOException | InterruptedException e) {
            log.error("❌ Error stopping running jobs: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Launch a new job using the Heroku run command.
     *
     * @param command the command to run (e.g., "java -jar app.jar")
     * @return true if the job was launched successfully, false otherwise
     */
    public boolean launchJob(String command) {
        if (herokuAppName == null || herokuAppName.trim().isEmpty()) {
            log.error("❌ Heroku app name not configured, cannot launch job");
            return false;
        }

        try {
            List<String> herokuCommand = List.of("heroku", "run", "--app", herokuAppName, command);
            ProcessBuilder pb = new ProcessBuilder(herokuCommand);
            
            log.info("🚀 Launching job with command: {}", String.join(" ", herokuCommand));
            
            // Start the process but don't wait for it to complete (it's a one-off dyno)
            Process process = pb.start();
            
            // Give it a moment to start and check if it failed immediately
            Thread.sleep(2000);
            
            if (process.isAlive()) {
                log.info("✅ Job launched successfully");
                return true;
            } else {
                int exitCode = process.exitValue();
                log.error("❌ Job failed to start, exit code: {}", exitCode);
                return false;
            }

        } catch (IOException | InterruptedException e) {
            log.error("❌ Error launching job: {}", e.getMessage(), e);
            return false;
        }
    }
}
