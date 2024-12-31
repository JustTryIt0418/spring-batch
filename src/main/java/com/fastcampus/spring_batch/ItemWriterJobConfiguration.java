package com.fastcampus.spring_batch;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.PathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class ItemWriterJobConfiguration {

    @Bean
    public Job itemWriterJob(
            JobRepository jobRepository,
            @Qualifier("itemWriterStep") Step step
    ) {
        return new JobBuilder("itemWriterJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean(name = "itemWriterStep")
    public Step itemReaderStep(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("flatFileItemReader2") ItemReader<User> itemReader,
            @Qualifier("jdbcBatchItemWriter") ItemWriter<User> itemWriter
    ) {
        return new StepBuilder("step", jobRepository)
                .<User, User>chunk(2, platformTransactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .build();
    }

    @Bean(name = "flatFileItemReader2")
    public FlatFileItemReader<User> flatFileItemReader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("flatFileItemReader")
                .resource(new ClassPathResource("users.txt"))
                .linesToSkip(2)
                .delimited().delimiter(",")
                .names("name", "age", "region", "telephone")
                .targetType(User.class)
                .strict(false)  // 해당 파일이 없어도 정상 처리
                .build();
    }

    @Bean(name = "flatFileItemWriter")
    public ItemWriter<User> flatFileItemWriter() {
        return new FlatFileItemWriterBuilder<User>()
                .name("flatFileItemWriter")
                .resource(new PathResource("src/main/resources/new_users.txt"))
                .delimited().delimiter("__")
                .names("name", "age", "region", "telephone")
                .build();
    }

    @Bean(name = "formattedFileItemWriter")
    public ItemWriter<User> formattedFileItemWriter() {
        return new FlatFileItemWriterBuilder<User>()
                .name("formattedFileItemWriter")
                .resource(new PathResource("src/main/resources/new_formatted_users.txt"))
                .formatted()
                .format("%s의 나이는 %s입니다. 사는곳은 %s, 전화번호는 %s 입니다.")
                .names("name", "age", "region", "telephone")
//                .shouldDeleteIfExists(false)
//                .shouldDeleteIfEmpty(true)
//                .append(true)
                .build();
    }

    @Bean(name = "jsonFileItemWriter")
    public ItemWriter<User> jsonFileItemWriter() {
        return new JsonFileItemWriterBuilder<User>()
                .name("jsonFileItemWriter")
                .resource(new PathResource("src/main/resources/new_users.json"))
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .build();
    }

    @Bean(name = "jpaItemWriter")
    public ItemWriter<User> jpaItemWriter(
            EntityManagerFactory entityManagerFactory
    ) {
        return new JpaItemWriterBuilder<User>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean(name = "jdbcBatchItemWriter")
    public ItemWriter<User> jdbcBatchItemWriter(
            DataSource dataSource
    ) {
        return new JdbcBatchItemWriterBuilder<User>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO
                            user(name, age, region, telephone)
                        VALUES
                            (:name, :age, :region, :telephone)
                        """)
                .beanMapped()
                .build();
    }

}
