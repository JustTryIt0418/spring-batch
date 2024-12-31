package com.fastcampus.spring_batch;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.JsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class ItemReaderJobConfiguration {

    @Bean
    public Job itemReaderJob(
            JobRepository jobRepository,
            @Qualifier("itemReaderStep") Step step
    ) {
        return new JobBuilder("itemReaderJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean(name = "itemReaderStep")
    public Step itemReaderStep(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("jpaCursorItemReader") ItemReader<User> flatFileItemReader
    ) {
        return new StepBuilder("step", jobRepository)
                .<User, User>chunk(2, platformTransactionManager)
                .reader(flatFileItemReader)
                .writer(System.out::println)
                .build();
    }

    @Bean(name = "flatFileItemReader")
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

    @Bean(name = "fixedLengthFlatFileItemReader")
    public FlatFileItemReader<User> fixedLengthFlatFileItemReader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("fixedLengthFlatFileItemReader")
                .resource(new ClassPathResource("usersFixedLength.txt"))
                .linesToSkip(2)
                .fixedLength()
                .columns(
                    new Range[]{
                        new Range(2,3), new Range(4,5), new Range(6,7), new Range(8,20)
                    }
                )
                .names("name", "age", "region", "telephone")
                .targetType(User.class)
                .strict(false)  // 해당 파일이 없어도 정상 처리
                .build();
    }

    @Bean(name = "jsonItemReader")
    public JsonItemReader<User> jsonItemReader() {
        return new JsonItemReaderBuilder<User>()
                .name("jsonItemReader")
                .resource(new ClassPathResource("users.json"))
                .jsonObjectReader(new JacksonJsonObjectReader<>(User.class))
                .build();
    }

    @Bean(name = "jpaPagingItemReader")
    public ItemReader<User> jpaPagingItemReader (
            EntityManagerFactory entityManagerFactory
    ) {
        return new JpaPagingItemReaderBuilder<User>()
                .name("jpaPagingItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(3)
                .queryString("SELECT u FROM USER u ORDER BY u.id")
                .build();
    }

    @Bean(name = "jpaCursorItemReader")
    public ItemReader<User> jpaCursorItemReader(
            EntityManagerFactory entityManagerFactory
    ) {
        return new JpaCursorItemReaderBuilder<User>()
                .name("jpaCursorItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT u FROM USER u ORDER BY u.id")
                .build();
    }

}
