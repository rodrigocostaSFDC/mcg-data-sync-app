package com.salesforce.mcg.datasync.tasklet;

import com.salesforce.mcg.datasync.properties.SftpServerProperties;
import com.salesforce.mcg.datasync.repository.impl.JobExecutionHistoryJdbcRepository;
import com.salesforce.mcg.datasync.service.SftpService;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;

import javax.sql.DataSource;
import java.io.PipedInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShortUrlExportCSVTaskletTest {

    @Test
    void execute_shouldExportAndRenameCampaignFile() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        SftpPropertyContext context = mock(SftpPropertyContext.class);
        SftpService sftpService = mock(SftpService.class);
        JobExecutionHistoryJdbcRepository historyRepository = mock(JobExecutionHistoryJdbcRepository.class);
        SftpServerProperties serverProperties = mock(SftpServerProperties.class);

        when(context.getPropertiesForActiveCompany()).thenReturn(serverProperties);
        when(historyRepository.findLastSuccessfulExecutionTime(anyString()))
                .thenReturn(Optional.of(LocalDateTime.of(2026, 4, 1, 0, 0)));

        ShortUrlExportCSVTasklet tasklet = new ShortUrlExportCSVTasklet(
                dataSource, context, sftpService, historyRepository);
        setField(tasklet, "exportDirectory", "/exports");
        setField(tasklet, "batchSize", 1000);

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rs);

        when(rs.next()).thenReturn(true, false);
        when(rs.getTimestamp("creation_date")).thenReturn(Timestamp.valueOf("2026-04-01 12:00:00"));
        when(rs.getString("phone_number")).thenReturn("5551234567");
        when(rs.getString("email")).thenReturn(null);
        when(rs.getString("subscriber_key")).thenReturn("5551234567");
        when(rs.getString("api_key")).thenReturn("campaign-A");
        when(rs.getString("short_url")).thenReturn("abc");
        when(rs.getString("original_url")).thenReturn("https://example.com");
        when(rs.getString("message_type")).thenReturn("sms");
        when(rs.getInt("redirect_count")).thenReturn(5);
        when(rs.getString("transaction_date")).thenReturn("12345678901234-xyz");
        when(rs.getString("transaction_id")).thenReturn("tx-001");
        when(rs.getString("tcode")).thenReturn("t1");
        when(rs.getString("company")).thenReturn("telmex");

        StepContribution contribution = mock(StepContribution.class);
        ChunkContext chunkContext = buildChunkContext("2026-04-01", null, null, "campaign-A");
        RepeatStatus result = tasklet.execute(contribution, chunkContext);

        assertThat(result).isEqualTo(RepeatStatus.FINISHED);
        verify(sftpService).uploadStreamToSftp(anyString(), any(PipedInputStream.class), eq(serverProperties));
        verify(sftpService).renameFileOnSftp(anyString(), eq("/exports/shorturl_campaign_campaign-A_20260401.csv"));
        verify(contribution).incrementWriteCount(1L);
    }

    @Test
    void execute_shouldDeleteTempFileWhenNoRows() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        SftpPropertyContext context = mock(SftpPropertyContext.class);
        SftpService sftpService = mock(SftpService.class);
        JobExecutionHistoryJdbcRepository historyRepository = mock(JobExecutionHistoryJdbcRepository.class);
        SftpServerProperties serverProperties = mock(SftpServerProperties.class);

        when(context.getPropertiesForActiveCompany()).thenReturn(serverProperties);
        when(historyRepository.findLastSuccessfulExecutionTime(anyString())).thenReturn(Optional.empty());

        ShortUrlExportCSVTasklet tasklet = new ShortUrlExportCSVTasklet(
                dataSource, context, sftpService, historyRepository);
        setField(tasklet, "exportDirectory", "/exports");
        setField(tasklet, "batchSize", 1000);

        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(false);

        StepContribution contribution = mock(StepContribution.class);
        ChunkContext chunkContext = buildChunkContext("2026-04-01", null, null, "campaign-A");
        RepeatStatus result = tasklet.execute(contribution, chunkContext);

        assertThat(result).isEqualTo(RepeatStatus.FINISHED);
        verify(sftpService).deleteFile(anyString());
        verify(sftpService, never()).renameFileOnSftp(anyString(), anyString());
        verify(contribution, never()).incrementWriteCount(anyLong());
    }

    @Test
    void privateHelpers_shouldProduceExpectedValues() throws Exception {
        ShortUrlExportCSVTasklet tasklet = new ShortUrlExportCSVTasklet(
                mock(DataSource.class),
                mock(SftpPropertyContext.class),
                mock(SftpService.class),
                mock(JobExecutionHistoryJdbcRepository.class));

        Method resolveEmailSmsValue = ShortUrlExportCSVTasklet.class.getDeclaredMethod(
                "resolveEmailSmsValue", String.class, String.class, String.class);
        resolveEmailSmsValue.setAccessible(true);
        assertThat(resolveEmailSmsValue.invoke(tasklet, "555", null, "555")).isEqualTo("555");
        assertThat(resolveEmailSmsValue.invoke(tasklet, null, "a@b.com", "a@b.com")).isEqualTo("a@b.com");

        Method extractTipoEnvio = ShortUrlExportCSVTasklet.class.getDeclaredMethod("extractTipoEnvio", String.class);
        extractTipoEnvio.setAccessible(true);
        assertThat(extractTipoEnvio.invoke(tasklet, "sms")).isEqualTo("S");
        assertThat(extractTipoEnvio.invoke(tasklet, "")).isEqualTo("");

        Method extractFirst14Digits = ShortUrlExportCSVTasklet.class.getDeclaredMethod("extractFirst14Digits", String.class);
        extractFirst14Digits.setAccessible(true);
        assertThat(extractFirst14Digits.invoke(tasklet, "12345678901234-abc")).isEqualTo("12345678901234");
        assertThat(extractFirst14Digits.invoke(tasklet, "1234")).isEqualTo("1234");
        assertThat(extractFirst14Digits.invoke(tasklet, (Object) null)).isEqualTo("");

        Method asText = ShortUrlExportCSVTasklet.class.getDeclaredMethod("asText", String.class);
        asText.setAccessible(true);
        assertThat(asText.invoke(tasklet, "a\"b")).isEqualTo("\"a\"\"b\"");
        assertThat(asText.invoke(tasklet, "")).isEqualTo("\"\"");

        Method generateDateRangeFilename = ShortUrlExportCSVTasklet.class.getDeclaredMethod(
                "generateDateRangeFilename", String.class, String.class);
        generateDateRangeFilename.setAccessible(true);
        assertThat(generateDateRangeFilename.invoke(tasklet, "2026-04-01", "2026-04-05"))
                .isEqualTo("shorturl_daterange_20260401_to_20260405.csv");

        Method generateCampaignDayFilename = ShortUrlExportCSVTasklet.class.getDeclaredMethod(
                "generateCampaignDayFilename", String.class, String.class);
        generateCampaignDayFilename.setAccessible(true);
        assertThat(generateCampaignDayFilename.invoke(tasklet, "camp", "2026-04-01"))
                .isEqualTo("shorturl_campaign_camp_20260401.csv");

        Method resolveFinalFileName = ShortUrlExportCSVTasklet.class.getDeclaredMethod(
                "resolveFinalFileName", String.class, String.class, String.class, String.class, String.class);
        resolveFinalFileName.setAccessible(true);
        assertThat(resolveFinalFileName.invoke(tasklet, "camp", "2026-04-01", null, null, "20000101_000000"))
                .isEqualTo("shorturl_campaign_camp_20260401.csv");
        assertThat(resolveFinalFileName.invoke(tasklet, null, null, "2026-04-01", "2026-04-05", "20000101_000000"))
                .isEqualTo("shorturl_daterange_20260401_to_20260405.csv");
        assertThat(resolveFinalFileName.invoke(tasklet, null, null, null, null, "20000101_000000"))
                .isEqualTo("shorturl_export_20000101_000000.csv");
    }

    private static ChunkContext buildChunkContext(String date, String startDate, String endDate, String apiKey) {
        JobParametersBuilder builder = new JobParametersBuilder();
        if (date != null) {
            builder.addString("date", date);
        }
        if (startDate != null) {
            builder.addString("startDate", startDate);
        }
        if (endDate != null) {
            builder.addString("endDate", endDate);
        }
        if (apiKey != null) {
            builder.addString("apiKey", apiKey);
        }
        JobParameters params = builder.toJobParameters();

        StepExecution stepExecution = mock(StepExecution.class);
        when(stepExecution.getJobParameters()).thenReturn(params);

        StepContext stepContext = mock(StepContext.class);
        when(stepContext.getStepExecution()).thenReturn(stepExecution);

        ChunkContext chunkContext = mock(ChunkContext.class);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
        return chunkContext;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
