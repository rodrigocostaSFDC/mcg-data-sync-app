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

/**
 * Represents an Operator entity with various account and operator-related details.
 *
 * <p>This class acts as a model for holding operator information, such as IDs and account references.
 */
public record Operator(
    String operator,
    String name,
    String type,
    String rao,
    String masterAccount,
    String additionalMasterAccount,
    String virtualOperator,
    String dualOperator,
    String masterAccount11,
    String masterAccount16,
    String masterAccountTelnor,
    String idRvtaMayorista,
    String idx
){
    // Compatibility getters for existing repository code.
    public String getOperator() { return operator; }
    public String getName() { return name; }
    public String getType() { return type; }
    public String getRao() { return rao; }
    public String getMasterAccount() { return masterAccount; }
    public String getAdditionalMasterAccount() { return additionalMasterAccount; }
    public String getVirtualOperator() { return virtualOperator; }
    public String getDualOperator() { return dualOperator; }
    public String getMasterAccount11() { return masterAccount11; }
    public String getMasterAccount16() { return masterAccount16; }
    public String getMasterAccountTelnor() { return masterAccountTelnor; }
    public String getIdRvtaMayorista() { return idRvtaMayorista; }
    public String getIdx() { return idx; }
}

