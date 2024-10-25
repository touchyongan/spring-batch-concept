package io.kmaker.batch.config;

import io.kmaker.batch.task.DataCleanupTasklet;
import io.kmaker.batch.task.FilePollingTasklet;
import io.kmaker.batch.task.NotificationTasklet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@Slf4j
public class BatchConfig {

    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("step1", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("Run step1");
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean
    public Job myJob(JobRepository jobRepository,
                     Step step1) {
        return new JobBuilder("myJob", jobRepository)
                .start(step1)
                .build();
    }

    @Bean
    public Step cleanupStep(JobRepository jobRepository,
                            PlatformTransactionManager platformTransactionManager,
                            DataCleanupTasklet tasklet) {
        return new StepBuilder("cleanupStep", jobRepository)
                .tasklet(tasklet, platformTransactionManager)
                .build();
    }

    @Bean
    public Job dataCleanupJob(JobRepository jobRepository, Step cleanupStep) {
        return new JobBuilder("dataCleanupJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(cleanupStep)
                .build();
    }

    // Sending email after job finish
    @Bean
    public Step notificationStep(JobRepository jobRepository,
                                 PlatformTransactionManager platformTransactionManager,
                                 NotificationTasklet tasklet) {
        return new StepBuilder("notificationStep", jobRepository)
                .tasklet(tasklet, platformTransactionManager)
                .build();
    }

    @Bean
    public Step processStep(JobRepository jobRepository,
                            PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("processStep", jobRepository)
                .tasklet(((contribution, chunkContext) -> {
                    log.info("Simulate processing step to do something....");
                    return RepeatStatus.FINISHED;
                }), platformTransactionManager)
                .build();
    }

    @Bean
    public Job dataProcessingJob(JobRepository jobRepository,
                                 Step processStep,
                                 Step notificationStep) {
        return new JobBuilder("dataProcessingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(processStep)
                .next(notificationStep)
                .build();
    }

    // Tasklet Continuable
    @Bean
    public Job filePollingJob(JobRepository jobRepository,
                              Step pollingStep) {
        return new JobBuilder("filePollingJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(pollingStep)  // The polling step is the first step in the job
                .build();
    }

    @Bean
    public Step pollingStep(JobRepository jobRepository,
                            PlatformTransactionManager platformTransactionManager,
                            FilePollingTasklet filePollingTasklet) {
        return new StepBuilder("pollingStep", jobRepository)
                .tasklet(filePollingTasklet, platformTransactionManager)
                .build();
    }
}
