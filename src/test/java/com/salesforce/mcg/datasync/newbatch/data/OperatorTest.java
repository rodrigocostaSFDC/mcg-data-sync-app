package com.salesforce.mcg.datasync.newbatch.data;

import com.salesforce.mcg.datasync.data.Operator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorTest {

    @Test
    void compatibilityGetters_shouldReturnRecordValues() {
        Operator operator = new Operator(
                "op",
                "name",
                "type",
                "rao",
                "m1",
                "m2",
                "v",
                "d",
                "m11",
                "m16",
                "mt",
                "rv",
                "idx"
        );

        assertThat(operator.getOperator()).isEqualTo("op");
        assertThat(operator.getName()).isEqualTo("name");
        assertThat(operator.getType()).isEqualTo("type");
        assertThat(operator.getRao()).isEqualTo("rao");
        assertThat(operator.getMasterAccount()).isEqualTo("m1");
        assertThat(operator.getAdditionalMasterAccount()).isEqualTo("m2");
        assertThat(operator.getVirtualOperator()).isEqualTo("v");
        assertThat(operator.getDualOperator()).isEqualTo("d");
        assertThat(operator.getMasterAccount11()).isEqualTo("m11");
        assertThat(operator.getMasterAccount16()).isEqualTo("m16");
        assertThat(operator.getMasterAccountTelnor()).isEqualTo("mt");
        assertThat(operator.getIdRvtaMayorista()).isEqualTo("rv");
        assertThat(operator.getIdx()).isEqualTo("idx");
    }
}
