package io.kmaker.batch.sbia.ch02;

import io.kmaker.batch.sbia.ch02.listener.ImportProductsExecutionListener;
import io.kmaker.batch.sbia.ch02.listener.ImportProductsJobListener;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class ImportProductCh02Config {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job importProductWithListener(Step readWriteWithListener) {
        return new JobBuilder("importProductWithListener", jobRepository)
                .listener(new ImportProductsJobListener())
                .start(readWriteWithListener)
                .build();
    }

    @Bean
    public Step readWriteWithListener(ItemReader<Product> readerCh02,
                                      JdbcBatchItemWriter<Product> productWriter) {
        return new StepBuilder("readWriteWithListener", jobRepository)
                .listener(new ImportProductsExecutionListener())
                .<Product, Product>chunk(100, platformTransactionManager)
                .reader(readerCh02)
                .writer(productWriter)
                .build();
    }

    @Bean
    public FlatFileItemReader<Product> readerCh02(@Value("classpath:ch02/products-delimited.txt") Resource resource) {
        FlatFileItemReader<Product> reader = new FlatFileItemReader<>();
        reader.setResource(resource);
        reader.setLineMapper(productLineMapper());
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public LineMapper<Product> productLineMapper() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setNames("id", "name", "description", "price");

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(new ProductFieldSetMapper());
        lineMapper.setLineTokenizer(tokenizer);
        return lineMapper;
    }

    @Bean
    public JdbcBatchItemWriter<Product> productWriter(DataSource dataSource) {
        JdbcBatchItemWriter<Product> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("insert into product (id,name,description,price) values(?,?,?,?)");
        writer.setItemPreparedStatementSetter(new ProductPreparedStatementSetter());
        return writer;
    }
}
