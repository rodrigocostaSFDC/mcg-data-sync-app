package com.salesforce.mcg.datasync.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * SFTP preprocessor configuration grouped by company.
 */
@ConfigurationProperties(prefix = "sftp.preprocessor")
public record SftpPreprocessorProperties(
        SftpServerProperties telmex,
        SftpServerProperties telnor
) {

}