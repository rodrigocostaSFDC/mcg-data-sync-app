package com.salesforce.mcg.datasync.aspect;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Thread-local holder that provides the active timezone set by
 * {@link MexicoCityTimezoneAspect}.
 * <p>
 * Uses {@link InheritableThreadLocal} so child threads (e.g. writer threads)
 * inherit the timezone context automatically.
 */
public final class TimezoneContext {

    private static final InheritableThreadLocal<ZoneId> CURRENT_ZONE = new InheritableThreadLocal<>();

    private TimezoneContext() {}

    public static void set(ZoneId zone) {
        CURRENT_ZONE.set(zone);
    }

    public static void clear() {
        CURRENT_ZONE.remove();
    }

    public static ZoneId zone() {
        ZoneId z = CURRENT_ZONE.get();
        if (z == null) {
            throw new IllegalStateException(
                    "No timezone context — annotate the calling method with @MexicoCityTimezone");
        }
        return z;
    }

    public static LocalDate today() {
        return LocalDate.now(zone());
    }

    public static ZonedDateTime startOfDay(LocalDate date) {
        return date.atStartOfDay(zone());
    }

    public static ZonedDateTime startOfNextDay(LocalDate date) {
        return date.plusDays(1).atStartOfDay(zone());
    }
}
