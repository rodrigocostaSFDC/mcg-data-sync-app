package com.salesforce.mcg.datasync.batch;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SkipListener<LineWithNumber, SubscriberPortability> implements org.springframework.batch.core.SkipListener<LineWithNumber, SubscriberPortability> {

    @Override
    public void onSkipInProcess(@NonNull LineWithNumber item, Throwable t) {
        log.warn("Skipped in PROCESS: {} -> {}", item, t.getMessage());
    }

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Skipped in READ: {}", t.getMessage());
    }

    @Override
    public void onSkipInWrite(@NonNull SubscriberPortability item, Throwable t) {
        log.warn("Skipped in WRITE: {} -> {}", item, t.getMessage());
    }
}