package io.kmaker.batch.dynamic;

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
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class DynamicJobController {
    private final ApplicationContext context;
    private final JobLauncher jobLauncher;

    @PostMapping("/trigger-dynamic-import")
    public ResponseEntity<?> triggerJob(@RequestBody Map<String, Object> body) {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .addString("path", (String) body.get("path"))
                .addString("columns", (String) body.get("columns"))
                .addString("table", (String) body.get("table"))
                .toJobParameters();
        Job job = context.getBean("dynamicImportJob", Job.class);
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
