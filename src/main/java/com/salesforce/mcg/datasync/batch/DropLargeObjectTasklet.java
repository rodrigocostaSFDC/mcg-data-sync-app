package com.salesforce.mcg.datasync.batch;

import com.salesforce.mcg.datasync.service.LargeObjectService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DropLargeObjectTasklet implements Tasklet {

  @Resource
  private LargeObjectService largeObjectService;


  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
    try {
      long lobId = (long) chunkContext.getStepContext().getJobExecutionContext().get("lobId");
      largeObjectService.delete(lobId);
    } catch (Exception e) {
      log.error("Error deleting lob", e);
    }
    return RepeatStatus.FINISHED;
  }
}