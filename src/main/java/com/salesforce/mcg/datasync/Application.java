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

package com.salesforce.mcg.datasync;

import com.salesforce.mcg.datasync.common.SalesforceBanner;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import picocli.CommandLine;

import java.util.*;

/**
 * Entry point for the Data Sync microservice application.
 * <p>
 * Bootstraps the Spring Boot application as a one-off dyno (no web server).
 * Jobs are triggered via command line parameters (--job=xxx) or scheduled externally via Heroku Scheduler.
 * </p>
 */
@SpringBootApplication
@Getter
@Slf4j
public class Application {

    public static final String IMPORT = "import";
    public static final String EXPORT = "export";
    public static final String JOB_PARAM = "app.job";
    public static final String MODE_PARAM = "app.mode";

    public static Map<String, String> VALID_IMPORT_JOBS = Map.of(
            "sheet", "sheetImportCSVJob",
            "operator", "operatorImportCSVJob",
            "series", "subscriberSeriesImportCSVJob",
            "portability", "subscriberPortabilityImportCSVJob");

    public static Map<String, String> VALID_EXPORT_JOBS = Map.of(
            "sheet", "sheetImportCSVJob",
            "operator", "operatorImportCSVJob",
            "series", "subscriberSeriesImportCSVJob",
            "portability", "subscriberPortabilityImportCSVJob");

    /**
     * Company identifier.
     */
    @NotBlank(message = "--company is required")
    @Pattern(
            regexp = "^(?i)(telmex|telnor)$",
            message = "--company must be telmex or telnor"
    )
    @CommandLine.Option(
            names = "--company",
            required = true,
            description = "Allowed values: telmex, telnor"
    )
    private String company;

    /**
     * Company identifier.
     */
    @NotBlank(message = "--mode is required")
    @Pattern(
            regexp = "^(?i)(import|export)$",
            message = "--mode must be import or export"
    )
    @CommandLine.Option(
            names = "--mode",
            required = true,
            description = "Allowed values: import, export"
    )
    private String mode;

    @CommandLine.Option(
            names = "--date",
            description = "Defines export date"
    )
    private Date date;

    @CommandLine.Option(
            names = "--startDate",
            description = "Defines export initial date"
    )
    private Date startDate;

    @CommandLine.Option(
            names = "--endDate",
            description = "Defines export final date"
    )
    private Date endDate;

    @CommandLine.Option(
            names = "--apiKey",
            description = "Defines campaign apiKey"
    )
    private String apiKey;

    private String job;

    public static void main(String[] args) {

        var appInstance = new Application();

        // 1️⃣ Parse CLI arguments (picocli)
        var cmd = new CommandLine(appInstance);
        try {
            cmd.parseArgs(args);
        } catch (Exception e){
            log.error("❌ {}", e.getMessage());
            System.exit(1);
        }

        // 2️⃣ Validate (Jakarta Validation)
        validate(appInstance);

        // 3️⃣ Normalize values
        appInstance.mode = appInstance.mode.toLowerCase();
        appInstance.job = Objects.nonNull(appInstance.job) ? appInstance.job.trim(): null;

        // 4️⃣ Start Spring only after validation
        var app = new SpringApplication(Application.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setBanner(new SalesforceBanner());
        app.setBannerMode(Banner.Mode.CONSOLE);

        Map<String, Object> defaultProperties = new HashMap<>();
        defaultProperties.put(MODE_PARAM, appInstance.mode);
        if (Objects.nonNull(appInstance.job)) {
            if (appInstance.mode.equals(IMPORT)){
                defaultProperties.put(JOB_PARAM, VALID_IMPORT_JOBS.get(appInstance.job));
            } else {
                defaultProperties.put(JOB_PARAM, VALID_EXPORT_JOBS.get(appInstance.job));
            }
        }
        app.setDefaultProperties(defaultProperties);
        System.exit(SpringApplication.exit(app.run(args)));
    }

    /**
     * Validates CLI parameters.
     *
     * @param app application instance
     */
    private static void validate(Application app) {
        validateMode(app);
        validateJob(app);
        validateDayCampaign(app);
    }

    /**
     * Validates the mode argument.
     *
     * @param app application instance
     */
    private static void validateMode(Application app) {
        if (app.mode == null || app.mode.isBlank()) {
            fail("--mode is required");
        }

        if (!List.of("import", "export").contains(app.mode)) {
            fail("--mode must be import or export");
        }
    }

    /**
     * Validates the DAY CAMPAIGN
     *
     * @param app application instance
     */
    private static void validateDayCampaign(Application app) {
        if (Strings.isNotBlank(app.apiKey)){
            if (app.date == null && app.startDate == null){
                fail("campaign reports required at least one of them [--date | --startDate]");
            }
            if (app.startDate != null && app.endDate == null){
                fail("reports required both [--startDate & --endDate] ");
            }
        }
    }

    /**
     * Validates the job argument only when it was explicitly provided.
     *
     * @param app application instance
     */
    private static void validateJob(Application app) {
        if (Objects.isNull(app.job)) {
            return;
        } else if (app.job.isBlank()) {
            fail("--job requires a job name");
        }

        List<String> validJobs = getValidJobsForMode(app.mode);

        if (!validJobs.contains(app.job)) {
            fail("--job must be one of " + String.join(", ", validJobs) + " when --mode=" + app.mode);
        }
    }

    /**
     * Returns the valid jobs for the given mode.
     *
     * @param mode execution mode
     * @return list of valid jobs
     */
    private static List<String> getValidJobsForMode(String mode) {
        if (IMPORT.equals(mode)) {
            return VALID_IMPORT_JOBS.keySet().stream().toList();
        }
        if (EXPORT.equals(mode)) {
            return VALID_EXPORT_JOBS.keySet().stream().toList();
        }
        return List.of();
    }

    /**
     * Logs the validation error and terminates the application.
     *
     * @param message validation error message
     */
    private static void fail(String message) {
        log.error("❌️ {}", message);
        System.exit(1);
    }


}
