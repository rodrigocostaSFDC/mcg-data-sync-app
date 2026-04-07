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
 *****************************************************************************/

package com.salesforce.mcg.datasync.data;

import lombok.*;

/**
 * Represents a range of subscriber phone numbers along with associated operator information.
 * Provides utility functionality to check if a phone number falls within this series range.
 */
public record SubscriberSeries (
        Long seriesStart,
        Long seriesEnd,
        String operator,
        String virtualOperator){

    public boolean matches(Long phoneNumber) {
        if (phoneNumber == null || seriesStart == null || seriesEnd == null) {
            return false;
        }
        return phoneNumber >= seriesStart && phoneNumber <= seriesEnd;
    }
}

