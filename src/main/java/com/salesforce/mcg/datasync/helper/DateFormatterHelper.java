package com.salesforce.mcg.datasync.helper;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.salesforce.mcg.datasync.common.AppConstants.*;

public class DateFormatterHelper {


    private static final ZoneId MEXICO_ZONE = ZoneId.of(Timezone.AMERICA_MEXICO_CITY);

    private static final DateTimeFormatter MILLIS_FORMAT =
            DateTimeFormatter.ofPattern(Formats.MILLIS_FORMAT);

    public static String format(Timestamp timestamp) {
        return Optional.ofNullable(timestamp)
                .map(Timestamp::toInstant)
                .map(instant -> instant.atZone(MEXICO_ZONE))
                .map(MILLIS_FORMAT::format)
                .orElse("");
    }

    public static String format(OffsetDateTime offsetTime) {
        return Optional.ofNullable(offsetTime)
                .map(instant -> instant.atZoneSameInstant(MEXICO_ZONE))
                .map(MILLIS_FORMAT::format)
                .orElse("");
    }

    public static String format(LocalDateTime localDateTime) {
        return Optional.ofNullable(localDateTime)
                .map(instant -> instant.atZone(MEXICO_ZONE))
                .map(MILLIS_FORMAT::format)
                .orElse("");
    }

    public String formatNow() {
        return Instant.now()
                .atZone(MEXICO_ZONE)
                .format(MILLIS_FORMAT);
    }
}
