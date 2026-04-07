package com.salesforce.mcg.datasync.newbatch.data;

import java.util.concurrent.atomic.AtomicLong;

public record ImportContext(
        AtomicLong totalRecord,
        AtomicLong processedRecord,
        AtomicLong skippedRecords,
        AtomicLong errorRecords
) {
}
