package io.kmaker.batch.sbia.ch02.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

@Slf4j
public class ImportProductsJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Called when job starts");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("Called when job ends successfully");
        } else if (jobExecution.getStatus() == BatchStatus.FAILED) {
            log.info("Called when job ends in failure");
        }
    }
}
