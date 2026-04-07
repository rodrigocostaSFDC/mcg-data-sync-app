package com.salesforce.mcg.datasync.repository.impl;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobExecutionHistoryJdbcRepositoryTest {

    @Test
    void findLastSuccessfulExecutionTime_shouldReturnTimestampWhenFound() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobExecutionHistoryJdbcRepository repository = new JobExecutionHistoryJdbcRepository(jdbcTemplate);

        LocalDateTime expected = LocalDateTime.of(2026, 4, 2, 11, 30, 0);
        when(jdbcTemplate.queryForObject(
                eq(JobExecutionHistoryJdbcRepository.LAST_SUCCESSFUL_EXECUTION_SQL),
                eq(Timestamp.class),
                eq("shortUrlExportJob")
        )).thenReturn(Timestamp.valueOf(expected));

        Optional<LocalDateTime> result = repository.findLastSuccessfulExecutionTime("shortUrlExportJob");

        assertThat(result).contains(expected);
        verify(jdbcTemplate).queryForObject(
                JobExecutionHistoryJdbcRepository.LAST_SUCCESSFUL_EXECUTION_SQL,
                Timestamp.class,
                "shortUrlExportJob"
        );
    }

    @Test
    void findLastSuccessfulExecutionTime_shouldReturnEmptyWhenNoSuccessfulExecution() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobExecutionHistoryJdbcRepository repository = new JobExecutionHistoryJdbcRepository(jdbcTemplate);

        when(jdbcTemplate.queryForObject(
                eq(JobExecutionHistoryJdbcRepository.LAST_SUCCESSFUL_EXECUTION_SQL),
                eq(Timestamp.class),
                eq("shortUrlExportJob")
        )).thenReturn(null);

        Optional<LocalDateTime> result = repository.findLastSuccessfulExecutionTime("shortUrlExportJob");

        assertThat(result).isEmpty();
    }

    @Test
    void findLastSuccessfulExecutionTime_shouldRejectBlankJobName() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        JobExecutionHistoryJdbcRepository repository = new JobExecutionHistoryJdbcRepository(jdbcTemplate);

        assertThatThrownBy(() -> repository.findLastSuccessfulExecutionTime(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("jobName");
    }
}
