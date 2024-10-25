package io.kmaker.batch.dynamic;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class DynamicImportBatchConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;
    private final ResourceLoader resourceLoader;

    @StepScope
    @Bean
    public FlatFileItemReader<Map<String, Object>> dynamicReader(@Value("#{jobParameters[path]}") String path,
                                                                 @Value("#{jobParameters[columns]}") String column) {
        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);

        reader.setResource(resourceLoader.getResource(path));

        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(new DynamicFieldMapSet(column));

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        String[] columns = Arrays.stream(column.split(","))
                .map(String::strip)
                .toList()
                .toArray(String[]::new);
        lineTokenizer.setNames(columns);

        lineMapper.setLineTokenizer(lineTokenizer);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @StepScope
    @Bean
    public JdbcBatchItemWriter<Map<String, Object>> dynamicWriter(@Value("#{jobParameters[columns]}") String column,
                                                                  @Value("#{jobParameters[table]}") String table,
                                                                  DataSource dataSource) {
        List<String> columns = Arrays.stream(column.split(","))
                .map(String::strip)
                .toList();
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ")
                .append(table)
                .append(" (%s) ".formatted(String.join(",", columns)))
                .append("values (%s)".formatted(String.join(",", columns.stream()
                        .map(s -> "?")
                        .toList()))
                );
        JdbcBatchItemWriter<Map<String, Object>> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setItemPreparedStatementSetter(new DynamicPreparedStmtSetter(columns));
        writer.setSql(sb.toString());
        return writer;
    }

    @Bean
    public Job dynamicImportJob(Step dynamicReadWriteStep) {
        return new JobBuilder("dynamicImportJob", jobRepository)
                .start(dynamicReadWriteStep)
                .build();
    }

    @Bean
    public Step dynamicReadWriteStep(FlatFileItemReader<Map<String, Object>> dynamicReader,
                                    JdbcBatchItemWriter<Map<String, Object>> dynamicWriter) {
        return new StepBuilder("dynamicReadWriteStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(10, platformTransactionManager)
                .reader(dynamicReader)
                .writer(dynamicWriter)
                .build();
    }
}
