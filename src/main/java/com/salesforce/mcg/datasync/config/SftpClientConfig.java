/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services; loss of use, data, or profits; or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ****************************************************************************/

package com.salesforce.mcg.datasync.config;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.datasync.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.datasync.util.LocalEmbeddedSftpServer;
import com.salesforce.mcg.datasync.util.SftpPropertyContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;

import java.util.Properties;
import static com.salesforce.mcg.datasync.common.AppConstants.*;

@Configuration
@ConditionalOnClass(JSch.class)
@EnableConfigurationProperties(SftpPreprocessorProperties.class)
@Slf4j
public class SftpClientConfig {

    @Value("${sftp.mode}") private String sftpMode;
    @Value("${sftp.prod.out-dir}") String prodOutDir;
    @Value("${sftp.staging.out-dir}") String stagingOutDir;
    @Value("${sftp.default.out-dir}") String defaultOutDir;

    @ConditionalOnProperty(name = Sftp.Mode.MODE, havingValue = Sftp.Mode.LOCAL)
    @Bean JSch jSchLocal(
            SftpPropertyContext context,
            LocalEmbeddedSftpServer sftpServer) throws JSchException {

        var props = context.getPropertiesForActiveCompany();
        var jsch = new JSch();
        var session = jsch.getSession(props.username(), props.host(), props.port());
        session.setPassword(props.password());
        var config = new Properties();
        config.put(Sftp.StrictHost.HOST_KEY_CHECKING, Sftp.StrictHost.NO);
        session.setConfig(config);
        session.connect();
        return jsch;
    }

    @Bean
    @ConditionalOnProperty(name = Sftp.Mode.MODE, havingValue = Sftp.Mode.REMOTE, matchIfMissing = true)
    public JSch jschRemote(SftpPropertyContext context) throws JSchException {
        var props = context.getPropertiesForActiveCompany();
        var jsch = new JSch();
        if (Strings.isNotBlank(props.knownHosts())) {
            jsch.setKnownHosts(props.knownHosts());
        }
        if (Strings.isNotBlank(props.privateKey())) {
            if (props.passphrase() != null && !props.passphrase().isBlank()) {
                jsch.addIdentity(props.privateKey(), props.passphrase());
            } else {
                jsch.addIdentity(props.privateKey());
            }
        }
        return jsch;
    }

    @Bean(destroyMethod = "disconnect")
    public Session sftpSession(JSch jsch, SftpPropertyContext context) {
        try {
            return buildAndConnectSession(jsch, context);
        } catch (JSchException e) {
            throw new BeanCreationException("❌ Failed to create SFTP session", e);
        }
    }

    @Bean
    public DefaultSftpSessionFactory sftpSessionFactory(SftpPropertyContext context) throws JSchException {
        var props = context.getPropertiesForActiveCompany();
        var factory = new DefaultSftpSessionFactory(true);
        factory.setHost(props.host());
        factory.setPort(props.port());
        factory.setUser(props.username());
        factory.setPassword(props.password());
        factory.setAllowUnknownKeys(props.allowUnknownKeys());
        factory.setTimeout(props.setTimeout());
        return factory;
    }

    @Bean("outDir")
    public String getOutDir(@Value("${APP_NAME:}")String appName){
        return switch (appName) {
            case "mcg-data-sync-app" -> prodOutDir;
            case "mcg-data-sync-app-staging" -> stagingOutDir;
            default -> defaultOutDir;
        };
    }

    @Bean
    public SftpRemoteFileTemplate sftpRemoteFileTemplate(DefaultSftpSessionFactory factory) {
        return new SftpRemoteFileTemplate(factory);
    }

    private Session buildAndConnectSession(JSch jsch, SftpPropertyContext context) throws JSchException {
        var preferredAuthentications = Sftp.Mode.LOCAL.equals(sftpMode) ?
                Sftp.PreferredAuth.LOCAL : Sftp.PreferredAuth.REMOTE;
        var props = context.getPropertiesForActiveCompany();
        var session = jsch.getSession(props.username(), props.host(), props.port());
        session.setPassword(props.password());
        if ((props.privateKey() == null || props.privateKey().isBlank())
                && props.passphrase() != null) {
            session.setPassword(props.password());
        }
        var cfg = new Properties();
        var strictHostKey = !props.allowUnknownKeys();
        cfg.put(Sftp.StrictHost.HOST_KEY_CHECKING, strictHostKey ? Sftp.StrictHost.YES : Sftp.StrictHost.NO);
        cfg.put(Sftp.PreferredAuth.PREFERRED_AUTHENTICATIONS, preferredAuthentications);
        session.setConfig(cfg);
        session.setServerAliveInterval(props.serverAliveInterval());
        session.setServerAliveCountMax(props.setServerAliveCountMax());
        session.setTimeout(props.setTimeout());
        return session;
    }

}

