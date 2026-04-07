package com.salesforce.mcg.datasync.helper;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class DateFormatterHelperTest {

    @Test
    void formatTimestamp_shouldHandleNullAndValue() {
        assertThat(DateFormatterHelper.format((Timestamp) null)).isEqualTo("");
        assertThat(DateFormatterHelper.format(Timestamp.valueOf("2024-12-05 10:15:30")))
                .isNotBlank()
                .contains("2024-12-05");
    }

    @Test
    void formatOffsetDateTime_shouldHandleNullAndValue() {
        assertThat(DateFormatterHelper.format((OffsetDateTime) null)).isEqualTo("");
        assertThat(DateFormatterHelper.format(OffsetDateTime.parse("2024-12-05T10:15:30Z")))
                .isNotBlank()
                .contains("2024-12-05");
    }

    @Test
    void formatLocalDateTime_shouldHandleNullAndValue() {
        assertThat(DateFormatterHelper.format((LocalDateTime) null)).isEqualTo("");
        assertThat(DateFormatterHelper.format(LocalDateTime.of(2024, 12, 5, 10, 15, 30)))
                .isNotBlank()
                .contains("2024-12-05");
    }

    @Test
    void formatNow_shouldReturnFormattedNow() {
        DateFormatterHelper helper = new DateFormatterHelper();
        assertThat(helper.formatNow()).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}");
    }
}
