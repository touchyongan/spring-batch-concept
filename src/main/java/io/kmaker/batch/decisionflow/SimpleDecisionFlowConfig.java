package io.kmaker.batch.decisionflow;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class SimpleDecisionFlowConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Step s1() {
        return new StepBuilder("s1", jobRepository)
                .tasklet(((contribution, chunkContext) -> {
                    System.out.println("Step 1....");
                    throw new IllegalArgumentException("test flow");
                    //return RepeatStatus.FINISHED;
                }), platformTransactionManager)
                .build();
    }

    @Bean
    public Step s2() {
        return new StepBuilder("s2", jobRepository)
                .tasklet(((contribution, chunkContext) -> {
                    System.out.println("Step 2....");
                    return RepeatStatus.FINISHED;
                }), platformTransactionManager)
                .build();
    }

    @Bean
    public Step s3() {
        return new StepBuilder("s3", jobRepository)
                .tasklet(((contribution, chunkContext) -> {
                    System.out.println("Step 3....");
                    return RepeatStatus.FINISHED;
                }), platformTransactionManager)
                .build();
    }

    @Bean
    public Job jobFlow(Step s1,
                       Step s2,
                       Step s3) {
        return new JobBuilder("jobFlow", jobRepository)
                .start(s1)
                .on("FAILED").to(s2)
                .from(s1).on("*").to(s3)
                .end()
                .build();
    }

    @Bean
    public Job jobFlow1(Step s2,
                       Step s3) {
        return new JobBuilder("jobFlow1", jobRepository)
                .start(s2)
                .next(new CustomDecider()).on(FlowExecutionStatus.FAILED.getName())
                .to(s3)
                .end()
                .build();
    }

    @Bean
    public ApplicationRunner jobFlowRunner(JobLauncher jobLauncher,
                                           ApplicationContext context) {
        return args -> {
            Job job = context.getBean("jobFlow1", Job.class);
            jobLauncher.run(job, new JobParameters());
        };
    }

    static class CustomDecider implements JobExecutionDecider {

        @Override
        public FlowExecutionStatus decide(JobExecution jobExecution,
                                          StepExecution stepExecution) {
            return FlowExecutionStatus.FAILED;
        }
    }
}
