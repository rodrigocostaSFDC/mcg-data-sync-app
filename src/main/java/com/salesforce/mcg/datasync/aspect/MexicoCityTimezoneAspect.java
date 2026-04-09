package com.salesforce.mcg.datasync.aspect;

import com.salesforce.mcg.datasync.common.AppConstants;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.time.ZoneId;

/**
 * Around advice that sets America/Mexico_City as the active timezone
 * for methods annotated with {@link MexicoCityTimezone}.
 * <p>
 * Follows the same pattern as {@link ElapsedTimeAspect} / {@link TrackElapsed}.
 */
@Aspect
@Component
@Slf4j
public class MexicoCityTimezoneAspect {

    private static final ZoneId MEXICO_ZONE = ZoneId.of(AppConstants.Timezone.AMERICA_MEXICO_CITY);

    @Around("@annotation(annotation)")
    public Object applyTimezone(ProceedingJoinPoint pjp, MexicoCityTimezone annotation) throws Throwable {
        String operation = annotation.value().isBlank()
                ? pjp.getSignature().toShortString()
                : annotation.value();
        log.info("🌎 [{}] Applying timezone: {}", operation, MEXICO_ZONE);
        TimezoneContext.set(MEXICO_ZONE);
        try {
            return pjp.proceed();
        } finally {
            TimezoneContext.clear();
            log.debug("🌎 [{}] Timezone context cleared", operation);
        }
    }
}
