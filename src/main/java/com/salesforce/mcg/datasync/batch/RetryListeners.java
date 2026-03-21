package com.salesforce.mcg.datasync.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryListeners {

    @Bean
    public RetryListener retryLoggingListener() {
        return new RetryListener() {
            @Override
            public <T, E extends Throwable> void onError(
                    RetryContext context,
                    RetryCallback<T, E> callback,
                    Throwable throwable) {
                log.warn("Retry attempt {} failed: {}", context.getRetryCount(), throwable.toString());
            }
        };
    }
}