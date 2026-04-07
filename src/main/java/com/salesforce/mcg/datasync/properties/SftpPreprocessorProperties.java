package com.salesforce.mcg.preprocessor.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * SFTP preprocessor configuration grouped by company.
 *
 * @author Rodrigo Costa (rodrigo.costa@salesforce.com)
 * @since 2026-03-30
 */
@ConfigurationProperties(prefix = "sftp.preprocessor")
public record SftpPreprocessorProperties(
        SftpServerProperties telmex,
        SftpServerProperties telnor
) {

}