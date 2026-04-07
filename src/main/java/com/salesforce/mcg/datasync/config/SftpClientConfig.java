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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;

import java.util.Properties;

@Configuration
@ConditionalOnClass(JSch.class)
@EnableConfigurationProperties(SftpProperties.class)
@Slf4j
public class SftpConfig {

    @Bean
    public JSch jsch(SftpProperties props) throws JSchException {
        JSch jsch = new JSch();
        // known_hosts for StrictHostKeyChecking=yes
        if (props.getKnownHosts() != null && !props.getKnownHosts().isBlank()) {
            jsch.setKnownHosts(props.getKnownHosts());
        }
        // private key (optional)
        if (props.getPrivateKey() != null && !props.getPrivateKey().isBlank()) {
            if (props.getPassphrase() != null && !props.getPassphrase().isBlank()) {
                jsch.addIdentity(props.getPrivateKey(), props.getPassphrase());
            } else {
                jsch.addIdentity(props.getPrivateKey());
            }
        }
        return jsch;
    }

    /**
     * Connected session bean for non-local profiles. Will be disconnected automatically on shutdown.
     */
    @Bean(destroyMethod = "disconnect")
    @Profile("!local")
    public Session sftpSession(JSch jsch, SftpProperties props) {
        try {
            Session session = jsch.getSession(props.getUsername(), props.getHost(), props.getPort());

            // Password only if not using key
            if ((props.getPrivateKey() == null || props.getPrivateKey().isBlank())
                    && props.getPassword() != null) {
                session.setPassword(props.getPassword());
            }

            Properties cfg = new Properties();
            cfg.put("StrictHostKeyChecking", "no");
            cfg.put("PreferredAuthentications", "publickey,password,keyboard-interactive");
            session.setConfig(cfg);

            session.setServerAliveInterval(20_000);
            session.setServerAliveCountMax(3);
            session.setTimeout(30_000);     // read timeout
            session.connect(15_000);        // TCP connect timeout

            return session;
        } catch (JSchException e) {
            throw new BeanCreationException("Failed to create session", e);
        }
    }

    /**
     * For local profile, retry connection to allow SFTP server startup time.
     */
    @Bean(name = "sftpSession")
    @Profile("local")
    public Session sftpSessionLocal(JSch jsch, SftpProperties props) {
        // For local profile, retry connection to allow SFTP server startup time
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                Session session = jsch.getSession(props.getUsername(), props.getHost(), props.getPort());
                if ((props.getPrivateKey() == null || props.getPrivateKey().isBlank())
                        && props.getPassword() != null) {
                    session.setPassword(props.getPassword());
                }
                Properties cfg = new Properties();
                cfg.put("StrictHostKeyChecking", "no");
                cfg.put("PreferredAuthentications", "publickey,password,keyboard-interactive");
                session.setConfig(cfg);
                session.setServerAliveInterval(20_000);
                session.setServerAliveCountMax(3);
                session.setTimeout(30_000);
                session.connect(15_000);
                log.info("✅ SFTP session connected successfully on attempt {}", attempt);
                return session;
            } catch (JSchException e) {
                log.warn("SFTP connection attempt {} failed: {}", attempt, e.getMessage());
                if (attempt < 5) {
                    try {
                        Thread.sleep(2000); // Wait 2 seconds before retry
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new BeanCreationException("Failed to create session, interrupted while waiting for retry", e);
                    }
                } else {
                    throw new BeanCreationException("Failed to create session after " + attempt + " attempts", e);
                }
            }
        }
        throw new BeanCreationException("Failed to create session after multiple retries");
    }

    @Bean
    DefaultSftpSessionFactory sftpSessionFactory(SftpProperties props) {
        var f = new DefaultSftpSessionFactory(true);
        f.setHost(props.getHost());
        f.setPort(props.getPort());
        f.setUser(props.getUsername());
        f.setPassword(props.getPassword());
        f.setTimeout(30_000);
        f.setAllowUnknownKeys(true);
        return f;
    }

    @Bean
    SftpRemoteFileTemplate sftpRemoteFileTemplate(DefaultSftpSessionFactory sf) {
        return new SftpRemoteFileTemplate(sf);
    }
}

