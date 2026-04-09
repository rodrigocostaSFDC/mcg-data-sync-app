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

package com.salesforce.mcg.datasync.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.datasync.properties.SftpServerProperties;
import jakarta.annotation.Resource;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.util.List;
import java.util.Objects;
import java.util.Vector;

import static com.salesforce.mcg.datasync.common.AppConstants.*;
/**
 * Service responsible for managing SFTP operations, specifically listing CSV files
 * from a given remote directory using a JSch SFTP channel.
 */
@Component("dataSyncSftpService")
@Slf4j
public class SftpService {

    @Resource
    private Session session;

    /**
     * Lists the names of data files in the specified remote directory.
     * Now supports individual files (CSV, TXT, GZ) instead of ZIP files.
     *
     * @param remoteDir the path to the remote directory
     * @return a list of data file names found in the directory
     * @throws SftpException if an SFTP error occurs during listing
     */
    public List<String> listFiles(String remoteDir) throws SftpException, JSchException {
        ChannelSftp sftp = null;
        try {
            if (!session.isConnected()) {
                session.connect();
            }
            sftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
            sftp.connect();
            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> files = sftp.ls(remoteDir);
            return files.stream()
                    .map(ChannelSftp.LsEntry::getFilename)
                    .filter(f -> {
                        String filename = f.toLowerCase();
                        return filename.endsWith(File.Extensions.CSV) ||
                               filename.endsWith(File.Extensions.TXT) ||
                               filename.endsWith(File.Extensions.GZ) ||
                               filename.endsWith(File.Extensions.DAT);
                    })
                    .toList();
        } catch (SftpException | JSchException e) {
            log.error("❌ Failed to list files in {}: {}", remoteDir, e.getMessage());
            throw e;
        } finally {
            if (sftp != null && sftp.isConnected()) {
                sftp.disconnect();
            }
        }
    }

    /**
     * Delete a temporary file on SFTP
     */
    public void deleteFile(String filePath) {
        String path = normalizeRemotePath(filePath);
        var dir = extractDirectory(path);
        var fileName = extractFileName(path);
        ChannelSftp channelSftp = null;
        try {
            if (!session.isConnected()){
                session.connect();
            }
            channelSftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
            channelSftp.connect();
            channelSftp.cd(dir);
            channelSftp.rm(fileName);
            log.info("🗑️ Deleted file: {}", fileName);
        } catch (Exception e) {
            log.error("❌️ Could not delete file {}. Error: {}", fileName, e.getMessage());
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }

    /**
     * Rename a file on SFTP using full remote paths.
     * <p>
     * Prefer {@link SftpChannel#rename} when upload and rename should share one channel.
     * </p>
     */
    public void renameFileOnSftp(String oldPath, String newPath) throws JSchException, SftpException {
        ChannelSftp channelSftp = null;
        try {
            if (!session.isConnected()) {
                session.connect();
            }
            channelSftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
            channelSftp.connect();

            String from = normalizeRemotePath(oldPath);
            String to = normalizeRemotePath(newPath);
            channelSftp.rename(from, to);
            log.info("✅ Renamed file on SFTP: {} ➡️ {}", from, to);
        } catch (SftpException | JSchException e) {
            log.error("❌ Failed to rename {} → {}: {}", oldPath, newPath, e.getMessage());
            throw e;
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }

    /**
     * Ensure directory exists on SFTP
     */
    public void ensureDirectoryExists(@NonNull String path) {
        ChannelSftp channelSftp = null;
        try {
            if (!session.isConnected()){
                session.connect();
            }
            channelSftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
            if (!channelSftp.isConnected()) channelSftp.connect();
            channelSftp.cd(path);
        } catch (SftpException | JSchException e) {
            try {
                if (Objects.nonNull(channelSftp)){
                    channelSftp.mkdir(path);
                    log.info("✅ Directory created: {}", path);
                } else {
                    log.error("❌ " + e.getMessage());
                }
            } catch (SftpException ex) {
                log.warn("❌ Could not create directory {}: {}", path, ex.getMessage());
            }
        }
    }

    /**
     * Upload stream to SFTP
     */
    public void uploadStreamToSftp(String remotePath, PipedInputStream inputStream, SftpServerProperties props) throws JSchException, SftpException, IOException {
        ChannelSftp channelSftp = null;
        try {
            if (!session.isConnected()) {
                session.connect();
            }
            channelSftp = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
            channelSftp.connect();
            log.info("📡 Connected to SFTP server: {}", props.host());
            String normalizedPath = normalizeRemotePath(remotePath);
            var directory = extractDirectory(normalizedPath);
            ensureDirectoryExists(directory);
            log.info("📡 Starting streaming upload to: {}", normalizedPath);
            channelSftp.put(inputStream, normalizedPath);
            log.info("📡 File uploaded successfully to: {}", normalizedPath);
        } catch (SftpException | JSchException e) {
            log.error("❌ Failed to upload to {}: {}", remotePath, e.getMessage());
            throw e;
        } finally {
            if (channelSftp != null && channelSftp.isConnected()) {
                channelSftp.disconnect();
            }
        }
    }

    public String extractDirectory(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash > 0 ? path.substring(0, lastSlash) : Strings.EMPTY;
    }

    public String extractFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash > 0 ? path.substring(lastSlash + 1) : path;
    }

    /**
     * Collapses repeated slashes and trims trailing slashes (except root "/") so upload and rename
     * use the same path the server resolves after {@link ChannelSftp#put}.
     */
    static String normalizeRemotePath(String path) {
        if (path == null || path.isEmpty()) {
            return path;
        }
        String p = path.trim().replaceAll("/+", "/");
        while (p.length() > 1 && p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    /**
     * Opens a single SFTP channel that stays open for multiple operations.
     * Use with try-with-resources so the channel is closed automatically.
     */
    public SftpChannel openChannel() throws JSchException {
        if (!session.isConnected()) {
            session.connect();
        }
        ChannelSftp ch = (ChannelSftp) session.openChannel(Sftp.Channel.SFTP);
        ch.connect();
        return new SftpChannel(ch);
    }

    /**
     * A single SFTP channel that can perform upload, rename, and delete
     * without opening a new connection for each operation.
     */
    public static class SftpChannel implements AutoCloseable {

        private final ChannelSftp channel;

        SftpChannel(ChannelSftp channel) {
            this.channel = channel;
        }

        public void upload(String remotePath, InputStream inputStream) throws SftpException {
            String normalized = normalizeRemotePath(remotePath);
            String directory = normalized.substring(0, normalized.lastIndexOf('/'));
            ensureDirectory(directory);
            log.info("📡 Starting streaming upload to: {}", normalized);
            channel.put(inputStream, normalized);
            log.info("📡 File uploaded successfully to: {}", normalized);
        }

        public void rename(String oldPath, String newPath) throws SftpException {
            String from = normalizeRemotePath(oldPath);
            String to = normalizeRemotePath(newPath);
            channel.rename(from, to);
            log.info("✅ Renamed file on SFTP: {} ➡️ {}", from, to);
        }

        public void delete(String filePath) {
            try {
                String normalized = normalizeRemotePath(filePath);
                channel.rm(normalized);
                log.info("🗑️ Deleted file: {}", normalized);
            } catch (SftpException e) {
                log.error("❌️ Could not delete file {}. Error: {}", filePath, e.getMessage());
            }
        }

        private void ensureDirectory(String path) {
            try {
                channel.cd(path);
            } catch (SftpException e) {
                try {
                    channel.mkdir(path);
                    log.info("✅ Directory created: {}", path);
                } catch (SftpException ex) {
                    log.warn("❌ Could not create directory {}: {}", path, ex.getMessage());
                }
            }
        }

        @Override
        public void close() {
            if (channel.isConnected()) {
                channel.disconnect();
            }
        }
    }

}
