package com.salesforce.mcg.datasync.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Around advice that logs elapsed time for methods annotated with {@link TrackElapsed}.
 */
@Aspect
@Component
@Slf4j
public class ElapsedTimeAspect {

    @Around("@annotation(trackElapsed)")
    public Object logElapsed(ProceedingJoinPoint pjp, TrackElapsed trackElapsed) throws Throwable {
        long startNanos = System.nanoTime();
        String operation = trackElapsed.value().isBlank()
                ? pjp.getSignature().toShortString()
                : trackElapsed.value();
        try {
            Object result = pjp.proceed();
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            log.debug("⏱️ {} completed in {}ms", operation, elapsedMs);
            return result;
        } catch (Throwable t) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            log.warn("⏱️ {} failed after {}ms: {}", operation, elapsedMs, t.getMessage());
            throw t;
        }
    }
}
