package io.kmaker.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringBatchConceptApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchConceptApplication.class, args);
	}

	//@Bean
	ApplicationRunner applicationRunner(JobLauncher jobLauncher,
										ApplicationContext context) {
		return args -> {
			final var job = context.getBean("filePollingJob", Job.class);
			jobLauncher.run(job, new JobParameters());
		};
	}
}
