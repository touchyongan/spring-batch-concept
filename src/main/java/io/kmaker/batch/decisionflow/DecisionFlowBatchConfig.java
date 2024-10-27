package io.kmaker.batch.decisionflow;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
public class DecisionFlowBatchConfig {

    @Bean
    @StepScope
    public ListItemReader<Map<String, Object>> listItemReader() {
        Map<String, Object> item1 = new LinkedHashMap<>();
        item1.put("key1", "string");
        item1.put("key2", 10);
        Map<String, Object> item2 = new LinkedHashMap<>();
        item2.put("key1", "string");
        item2.put("key2", "should number");
        return new ListItemReader<>(Arrays.asList(item1, item2));
    }

    @Bean
    public ListItemWriter<Map<String, Object>> listItemWriter() {
        return new ListItemWriter<>() {
            @Override
            public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
                super.write(chunk);
                System.out.println("Written items: " + chunk);
            }
        };
    }

    @Bean
    public ListItemWriter<ValidationException> listItemWriter1() {
        return new ListItemWriter<>();
    }

    @Bean
    public ValidatingItemProcessor<Map<String, Object>> simpleValidator() {
        ValidatingItemProcessor<Map<String, Object>> processor = new ValidatingItemProcessor<>();
        processor.setValidator(new SimpleValidator());
        processor.setFilter(false);
        return processor;
    }

    @Bean
    public Step itemStep(JobRepository jobRepository,
                         PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("itemStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(2, platformTransactionManager)
                .reader(listItemReader())
                .writer(listItemWriter())
                .build();
    }

    @Bean
    public Step validationStep(JobRepository jobRepository,
                               PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("validationStep", jobRepository)
                .<Map<String, Object>, ValidationException>chunk(2, platformTransactionManager)
                .reader(listItemReader())
                .processor(new ValidationProcessor())
                .writer(listItemWriter1())
                .listener(new ValidationStepExecution())
                .build();
    }

    @Bean
    public Job itemJob(JobRepository jobRepository,
                       Step itemStep,
                       Step validationStep) {
        return new JobBuilder("itemJob", jobRepository)
                .start(validationStep)
                .next(new MyDecider())
                .on("FAILED").stop()
                .from(validationStep)
                    .on("COMPLETED")
                    .to(itemStep)
                    .end()
                .build();
    }

    //@Bean
    public ApplicationRunner applicationRunner(JobLauncher jobLauncher,
                                               ApplicationContext context,
                                               ListItemWriter<Map<String, Object>> listItemWriter,
                                               ListItemWriter<ValidationException> listItemWriter1) {
        return args -> {
            Job job = context.getBean("itemJob", Job.class);
            jobLauncher.run(job, new JobParameters());
            System.out.println(listItemWriter.getWrittenItems().size());
            System.out.println(listItemWriter1.getWrittenItems().size());
        };
    }

    static class SimpleValidator implements Validator<Map<String, Object>> {

        @Override
        public void validate(Map<String, Object> value) throws ValidationException {
            if (!(value.get("key2") instanceof Number)) {
                throw new ValidationException("key2 should be a number");
            }
        }
    }

    static class ValidationProcessor implements ItemProcessor<Map<String, Object>, ValidationException> {

        @Override
        public ValidationException process(Map<String, Object> item) throws Exception {
            if (!(item.get("key2") instanceof Number)) {
                return new ValidationException("key2 should be a number");
            }
            return null;
        }
    }

    static class ValidationStepExecution implements StepExecutionListener {

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            if (stepExecution.getWriteCount() > 0) {
                System.out.println("Commit count: " + stepExecution.getCommitCount());
                return new ExitStatus("COMPLETED WITH INVALID DATA");
            }
            return StepExecutionListener.super.afterStep(stepExecution);
        }
    }

    static class MyDecider implements JobExecutionDecider {

        @Override
        public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
            String status;
            if (new ExitStatus("COMPLETED WITH INVALID DATA").equals(stepExecution.getExitStatus())) {
                status = "FAILED";
            }
            else {
                status = "COMPLETED";
            }
            return new FlowExecutionStatus(status);
        }
    }
}
