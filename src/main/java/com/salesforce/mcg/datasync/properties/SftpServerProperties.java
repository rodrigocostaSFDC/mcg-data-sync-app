package com.salesforce.mcg.datasync.properties;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Sftp Server properties
 * Supports multiple sftp configurations
 *
 * @param company company name
 * @param host sftp host name
 * @param port sftp port
 * @param username connection username for the sftp server
 * @param password connection username for the sftp server
 * @param privateKey privateKey for the sftp server
 * @param passphrase passphrase for the sftp server
 * @param knownHosts allow to connect to unknown hosts
 * @param allowUnknownKeys allow to connect with unknown keys
 * @param inputDir sftp input dir
 * @param outputDir sftp output dir
 * @param filePattern patterns files must match to be processed
 */
public record SftpServerProperties(
        @NotBlank String company,
        @NotBlank String host,
        @DefaultValue("25") int port,
        @NotBlank String username,
        @NotBlank String password,
        @NotBlank String privateKey,
        @NotBlank String passphrase,
        @NotBlank String knownHosts,
        @DefaultValue("false") boolean allowUnknownKeys,
        @DefaultValue("/") String inputDir,
        @DefaultValue("/") String outputDir,
        @DefaultValue("_S_") String filePattern,
        @DefaultValue("20000") int serverAliveInterval,
        @DefaultValue("3") int setServerAliveCountMax,
        @DefaultValue("30000") int setTimeout
){}