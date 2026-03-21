package com.salesforce.mcg.datasync.batch.portability;// StagingItemWriter.java

import com.salesforce.mcg.datasync.service.LargeObjectService;
import com.salesforce.mcg.datasync.service.PostgresCopyService;
import com.salesforce.mcg.datasync.model.UploadedFile;
import com.salesforce.mcg.datasync.repository.UploadedFileRepository;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import java.io.InputStream;

//@Component
public class StagingItemWriter implements ItemWriter<UploadedFile> {

    //  @Resource
    private LargeObjectService largeObjectService;

    //  @Resource
    private PostgresCopyService copyService;

    //  @Resource
    private UploadedFileRepository repo;

    private String stagingTable;
    private String columnsCsv;

    @Override
    public void write(Chunk<? extends UploadedFile> items) throws Exception {
        for (UploadedFile f : items) {
            InputStream in = largeObjectService.open(f.getOid());
            copyService.copyCsv(in, stagingTable, columnsCsv);
        }
    }
}