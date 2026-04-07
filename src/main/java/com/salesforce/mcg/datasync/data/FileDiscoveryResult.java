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

package com.salesforce.mcg.datasync.data;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Result object containing categorized files discovered from SFTP directories.
 * Each job type expects a single file (except sheets which may have multiple).
 * If a file is missing for a job type, that job is simply skipped.
 *
 * @author AI Generated
 * @since 2024-09-25
 */
@Getter
@Builder
public class FileDiscoveryResult {
    
    /** Single portability file (fin_portados) */
    private final String portabilityFile;
    
    /** Single series file (fin_series) */
    private final String seriesFile;
    
    /** Single operator file (fin_operador) */
    private final String operatorFile;
    
    /** Sheet files (catalogo_plantillas_sms) - may have multiple */
    @Builder.Default
    private final List<String> sheetFiles = new java.util.ArrayList<>();
    
    /**
     * Gets the total count of all discovered files.
     * 
     * @return total number of files found
     */
    public int getTotalFileCount() {
        int count = sheetFiles.size();
        if (portabilityFile != null) count++;
        if (seriesFile != null) count++;
        if (operatorFile != null) count++;
        return count;
    }
    
    /**
     * Checks if any files were discovered.
     * 
     * @return true if at least one file was found, false otherwise
     */
    public boolean hasFiles() {
        return getTotalFileCount() > 0;
    }
}

