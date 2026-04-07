package com.salesforce.mcg.datasync.service;

import com.salesforce.mcg.datasync.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.RejectAllPublickeyAuthenticator;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Starts an embedded SFTP server for local development.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "sftp.mode", havingValue = "local")
public class LocalEmbeddedSftpServer implements DisposableBean {

    private final SshServer sshServer;

    public LocalEmbeddedSftpServer(
            SftpPropertyContext sftpPropertyContext,
            SftpPreprocessorProperties properties) throws IOException {
        var props = sftpPropertyContext.getPropertiesForActiveCompany();
        var localSftpPath = "./sftp/inbox_%s".formatted(props.company());
        var rootPath = Path
                .of(localSftpPath)
                .toAbsolutePath().normalize();
        Files.createDirectories(rootPath);
        sshServer = SshServer.setUpDefaultServer();
        sshServer.setHost(props.host());
        sshServer.setPort(props.port());
        sshServer.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(rootPath.resolve("hostkey.ser")));
        sshServer.setSubsystemFactories(List.of(new SftpSubsystemFactory.Builder().build()));
        sshServer.setFileSystemFactory(new VirtualFileSystemFactory(rootPath));
        sshServer.setPublickeyAuthenticator(RejectAllPublickeyAuthenticator.INSTANCE);

        sshServer.setPasswordAuthenticator((providedUsername, providedPassword, session) ->
            props.username().equals(providedUsername)
                    && props.password().equals(providedPassword));

        sshServer.start();

        log.info("▶️ Local embedded SFTP started on {}:{} with root [{}]",
                props.host(),
                props.port(),
                rootPath);
    }

    @Override
    public void destroy() throws Exception {
        if (sshServer != null && !sshServer.isClosed()) {
            sshServer.stop();
            log.info("⏹️ Local embedded SFTP stopped");
        }
    }
}