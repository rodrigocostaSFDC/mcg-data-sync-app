package com.salesforce.mcg.datasync.data;

import java.util.concurrent.atomic.AtomicLong;

public record ImportContext(
        AtomicLong totalRecord,
        AtomicLong processedRecord,
        AtomicLong skippedRecords,
        AtomicLong errorRecords
) {
}
