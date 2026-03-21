package com.salesforce.mcg.datasync.batch;

import com.salesforce.mcg.datasync.service.LargeObjectService;
import jakarta.annotation.Resource;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.core.io.AbstractResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Component
@StepScope
public class LargeObjectResource extends AbstractResource {

  @Resource
  private LargeObjectService largeObjectService;

  @Override public String getDescription() {
    return "DB BLOB resource id=" + lobId(); }

  @Override public InputStream getInputStream() throws IOException {
    try {
      return largeObjectService.open(lobId());
    } catch (Exception e) {
      throw new IOException("Open BLOB failed", e);
    }
  }

  @Override public boolean exists() {
    try {
      InputStream is = largeObjectService.open(lobId());
      is.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private Long lobId(){
    StepContext context = StepSynchronizationManager.getContext();
    return (Long) Objects.requireNonNull(context).getJobExecutionContext().get("lobId");
  }
}