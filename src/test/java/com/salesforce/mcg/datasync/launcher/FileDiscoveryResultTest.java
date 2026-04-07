package com.salesforce.mcg.datasync.launcher;

import com.salesforce.mcg.datasync.data.FileDiscoveryResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FileDiscoveryResult}.
 */
class FileDiscoveryResultTest {

    @Test
    void getTotalFileCount_shouldCountAllFiles() {
        FileDiscoveryResult result = FileDiscoveryResult.builder()
                .portabilityFile("port.txt")
                .seriesFile("series.txt")
                .operatorFile("op.txt")
                .sheetFiles(List.of("sheet1.txt", "sheet2.txt"))
                .build();

        assertThat(result.getTotalFileCount()).isEqualTo(5);
    }

    @Test
    void getTotalFileCount_shouldHandleNullFiles() {
        FileDiscoveryResult result = FileDiscoveryResult.builder()
                .portabilityFile(null)
                .seriesFile(null)
                .operatorFile(null)
                .sheetFiles(List.of())
                .build();

        assertThat(result.getTotalFileCount()).isEqualTo(0);
    }

    @Test
    void hasFiles_shouldReturnFalseWhenEmpty() {
        FileDiscoveryResult result = FileDiscoveryResult.builder()
                .sheetFiles(List.of())
                .build();

        assertThat(result.hasFiles()).isFalse();
    }

    @Test
    void hasFiles_shouldReturnTrueWhenAnyFilePresent() {
        FileDiscoveryResult result = FileDiscoveryResult.builder()
                .portabilityFile("port.txt")
                .build();

        assertThat(result.hasFiles()).isTrue();
    }
}
