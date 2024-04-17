package com.java.config;

import com.java.entity.Customer;
import com.java.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@AllArgsConstructor
public class SpringBatchConfig {

    private CustomerRepository customerRepository;

    private JobRepository jobRepository;

    private PlatformTransactionManager transactionManager;

    // a. ItemReader (consists of: LineMapper + DelimitedTokenizer + BeanMapperFieldSetMapper)
    @Bean
    public FlatFileItemReader<Customer> flatFileItemReader() {
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csv-reader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    // for a particular line
    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> defaultLineMapper = new DefaultLineMapper<>();
        // declare separator
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
        // bean mapper field set mapper to target the database entity class
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);
        // set above 2 properties to line mapper
        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);
        return defaultLineMapper;
    }

    // b. ItemProcessor
    @Bean
    public CustomerProcessor customerProcessor() {
        return new CustomerProcessor();
    }

    // c. ItemWriter
    @Bean
    public RepositoryItemWriter<Customer> itemWritertemWriter() {
        RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
        // add JPA customer repository interface
        writer.setRepository(customerRepository);
        writer.setMethodName("save");
        return writer;
    }

    // 1. Step
    @Bean
    public Step step1() {
        return new StepBuilder("csv-step", jobRepository)
                .<Customer, Customer>chunk(100, transactionManager)
                .reader(flatFileItemReader())
                .processor(customerProcessor())
                .writer(itemWritertemWriter())
                .build();
    }

    // 2. Job
    @Bean
    public Job runJob() {
        return new JobBuilder("import customer info", jobRepository)
                .start(step1())
                .build();
    }

}
