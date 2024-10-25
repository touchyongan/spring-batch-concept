package io.kmaker.batch.sbia.ch02.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;

@Slf4j
public class ImportProductsExecutionListener {

    @BeforeStep
    public void handlingBeforeStep(StepExecution stepExecution) {
        log.info("Called before step starts");
    }

    @AfterStep
    public ExitStatus handlingAfterStep(StepExecution stepExecution) {
        log.info("Called after step ends");
        return ExitStatus.COMPLETED;
    }
}
