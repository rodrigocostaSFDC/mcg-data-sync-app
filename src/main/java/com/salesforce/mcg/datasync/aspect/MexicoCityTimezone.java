package com.salesforce.mcg.datasync.aspect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks methods whose execution should run under America/Mexico_City timezone context.
 * <p>
 * The {@link MexicoCityTimezoneAspect} intercepts annotated methods, sets the timezone
 * in {@link TimezoneContext}, and cleans up after execution. Any code in the call chain
 * can then use {@code TimezoneContext.zone()} to obtain the active zone.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MexicoCityTimezone {
    String value() default "";
}
