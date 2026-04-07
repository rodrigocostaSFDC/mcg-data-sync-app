package com.salesforce.mcg.datasync.data;

public record ExportRequest(
            String exportDateStart,
            String exportDateEnd,
            String exportApiKey) {
    }