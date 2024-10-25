package io.kmaker.batch.sbia.ch01;

import io.kmaker.batch.sbia.ch01.batch.DecompressTasklet;
import io.kmaker.batch.sbia.ch01.batch.ProductFieldSetMapper;
import io.kmaker.batch.sbia.ch01.batch.ProductJdbcItemWriter;
import io.kmaker.batch.sbia.ch01.domain.Product;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class ImportProductBatchConfig {
    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job importProducts(Step decompress,
                              Step readWriteProducts) {
        return new JobBuilder("importProducts", jobRepository)
                .start(decompress)
                .next(readWriteProducts)
                .build();
    }

    @Bean
    public Step decompress(DecompressTasklet decompressTasklet) {
        return new StepBuilder("decompress", jobRepository)
                .tasklet(decompressTasklet, platformTransactionManager)
                .build();
    }

    @Bean
    public Step readWriteProducts(FlatFileItemReader<Product> reader,
                                  ProductJdbcItemWriter writer) {
        return new StepBuilder("readWriteProducts", jobRepository)
                .<Product, Product>chunk(3, platformTransactionManager)
                .reader(reader)
                .writer(writer)
                .faultTolerant()
                .skipLimit(5)
                .skip(FlatFileParseException.class)
                .build();
    }

    @Bean
    public DecompressTasklet decompressTasklet(ResourceLoader resourceLoader) {
        DecompressTasklet decompressTasklet = new DecompressTasklet();
        decompressTasklet.setResourceLoader(resourceLoader);
        return decompressTasklet;
    }

    @Bean
    public ProductJdbcItemWriter writer() {
        return new ProductJdbcItemWriter(dataSource);
    }

    // late binding
    @StepScope
    @Bean
    public FlatFileItemReader<Product> reader(@Value("file:#{jobParameters[targetDirectory]+jobParameters['targetFile']}") Resource resource) {
        FlatFileItemReader<Product> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setLinesToSkip(1);

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setNames("PRODUCT_ID", "NAME", "DESCRIPTION", "PRICE");

        DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(new ProductFieldSetMapper());

        flatFileItemReader.setLineMapper(lineMapper);

        return flatFileItemReader;
    }
}
