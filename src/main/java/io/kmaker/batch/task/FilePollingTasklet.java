package io.kmaker.batch.task;

import io.kmaker.batch.service.FileService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FilePollingTasklet implements Tasklet {
    private final FileService fileService;
    private boolean isFileReady;

    public FilePollingTasklet(FileService fileService) {
        this.fileService = fileService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution,
                                ChunkContext chunkContext) throws Exception {
        log.info("Checking if file is ready....");
        isFileReady = fileService.isFileReady();
        if (isFileReady) {
            log.info("File is ready...");
            return RepeatStatus.FINISHED;
        }
        log.info("File is not ready. polling again...");
        return RepeatStatus.CONTINUABLE;
    }
}
