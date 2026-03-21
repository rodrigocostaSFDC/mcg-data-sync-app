package com.salesforce.mcg.datasync.sftp;

import com.jcraft.jsch.Session;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.core.io.AbstractResource;
import org.springframework.lang.NonNull;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

public class SftpReadableResource extends AbstractResource {

    private final Session session;

    public SftpReadableResource(Session session, String remotePath) {
        this.session = session;
    }

    @Override
    public boolean exists() {
        try {
            String remotePath = remotePath();
            com.jcraft.jsch.ChannelSftp sftp = (com.jcraft.jsch.ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            return !sftp.ls(remotePath).stream().toList().isEmpty();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    @Override
    public String getFilename() {
        String remotePath = remotePath();
        return StringUtils.getFilename(remotePath);
    }

    @Override
    @NonNull
    public String getDescription() {
        String remotePath = remotePath();
        return "SFTP resource [" + remotePath + "]";
    }

    @Override
    @NonNull
    public InputStream getInputStream() throws IOException {
        String remotePath = remotePath();
        try {
            com.jcraft.jsch.ChannelSftp sftp = (com.jcraft.jsch.ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            // Abre um novo stream a cada chamada (importante para restart do step)
            return sftp.get(remotePath);
        } catch (Exception e) {
            throw new IOException("Falha ao abrir stream SFTP: " + remotePath, e);
        }
    }

    @Override
    public long contentLength() throws IOException {
        String remotePath = remotePath();
        try {
            com.jcraft.jsch.ChannelSftp sftp = (com.jcraft.jsch.ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            return sftp.stat(remotePath).getSize();
        } catch (Exception e) {
            throw new IOException("Falha ao obter tamanho de " + remotePath, e);
        }
    }

    @Override
    public long lastModified() throws IOException {
        String remotePath = remotePath();
        try {
            com.jcraft.jsch.ChannelSftp sftp = (com.jcraft.jsch.ChannelSftp) session.openChannel("sftp");
            sftp.connect();
            return sftp.stat(remotePath).getMTime();
        } catch (Exception e) {
            throw new IOException("Falha ao obter lastModified de " + remotePath, e);
        }
    }

    private String remotePath(){
        try {
            StepContext context = StepSynchronizationManager.getContext();
            Map<String, Object> params = Objects.requireNonNull(context).getJobParameters();
            return (String) params.get("fileName");
        } catch (Exception e) {
            return  null;
        }
    }

}
