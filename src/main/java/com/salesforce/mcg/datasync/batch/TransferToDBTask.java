package com.salesforce.mcg.datasync.batch;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.datasync.service.LargeObjectService;
import com.salesforce.mcg.datasync.repository.UploadedFileRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.lang.NonNull;

import java.io.InputStream;

@Slf4j
@StepScope
public class TransferToDBTask implements Tasklet {

    private final LargeObjectService loService;
    private final UploadedFileRepository repo;
    private final Session session;

    public TransferToDBTask(
            LargeObjectService loService,
            UploadedFileRepository repo, Session session) {
        this.loService = loService;
        this.repo = repo;
        this.session = session;
    }

    @Override
    public RepeatStatus execute(@NonNull StepContribution c, ChunkContext ctx) throws Exception {

        /* unchecked */
        String fileName = (String) ctx.getStepContext().getJobParameters().get("fileName");

        ChannelSftp channelSftp = null;

        try {

            try {
                channelSftp = (ChannelSftp) session.openChannel("sftp");
                channelSftp.connect();
                InputStream in = channelSftp.get(fileName);
                log.info("Loading file {} from SFTP to Postgres...", fileName);
                long oid = loService.store(in);
                repo.insert(fileName, oid);
                ctx.getStepContext()
                        .getStepExecution()
                        .getJobExecution()
                        .getExecutionContext().put("lobId", oid);
                log.info("File loaded to storage. File: {}", fileName);
            } catch (Exception e) {
                log.error("Failed to load file {} from SFTP", fileName, e);
            }

        } finally {
            try {
                if (channelSftp != null) {
                    channelSftp.disconnect();
                }
            } catch (Exception ignore) {
            }
        }
        return RepeatStatus.FINISHED;
    }
}