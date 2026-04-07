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

package com.salesforce.mcg.datasync.model;

import lombok.*;

/**
 * Represents the application configuration model storing scheduler cron expressions
 * and cleanup retention time settings.
 *
 * @author Rodrigo Costa (rodrigo.costa@salesforce.com)
 * @since 2024-06-09
 */
@Data
@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DataSyncConfig {

    /**
     * Cron expression for the synchronization scheduler.
     */
    private String syncSchedulerCronExpression;

    /**
     * Cron expression for the cleanup scheduler.
     */
    private String cleanUpSchedulerCronExpression;

    /**
     * Retention time in days for cleanup operations.
     */
    private Integer cleanUpRetentionTimeInDays;

    private Boolean moveAfterCopy;

    private String movePath;

    private Integer keepLastFileQuantity;
}

