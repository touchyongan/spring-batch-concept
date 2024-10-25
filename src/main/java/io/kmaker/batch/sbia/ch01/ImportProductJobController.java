package io.kmaker.batch.sbia.ch01;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.ApplicationContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ImportProductJobController {
    private final JobLauncher jobLauncher;
    private final ApplicationContext context;

    @GetMapping("/trigger-import-product")
    public ResponseEntity<?> triggerImportProduct() {

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("inputResource", "classpath:/input/products.zip")
                .addString("targetDirectory", "./build/importproductsbatch/")
                .addString("targetFile", "products.txt")
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();
        Job job = context.getBean("importProducts", Job.class);
        try {
            jobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException |
                 JobRestartException |
                 JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            throw new RuntimeException(e);
        }
        return ResponseEntity.ok("");
    }
}
