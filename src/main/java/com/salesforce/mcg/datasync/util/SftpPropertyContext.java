package com.salesforce.mcg.datasync.util;

import com.salesforce.mcg.datasync.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.datasync.properties.SftpServerProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static com.salesforce.mcg.datasync.common.AppConstants.Company;

@Service
public class SftpPropertyContext {

    private final SftpPreprocessorProperties properties;
    private final String company;

    public SftpPropertyContext(
            SftpPreprocessorProperties properties,
            @Value("#{environment['company'] ?: 'telmex'}") String company){
        this.properties = properties;
        this.company = company;
    }

    public SftpServerProperties getPropertiesForActiveCompany(){
        return Company.TELMEX.equals(company) ?
                    properties.telmex(): properties.telnor();
    }
}
