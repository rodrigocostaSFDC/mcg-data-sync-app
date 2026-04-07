package com.salesforce.mcg.preprocessor.properties;

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
            String company,
            String host,
            int port,
            String username,
            String password,
            String privateKey,
            String passphrase,
            String knownHosts,
            boolean allowUnknownKeys,
            String inputDir,
            String outputDir,
            String filePattern){}
