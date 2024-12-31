package com.fastcampus.spring_batch;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class MultiThreadedJobConfiguration {

    @Bean
    public Job multiThreadJob(
            JobRepository jobRepository,
            @Qualifier("multiThreadStep") Step step
            ) {
        return new JobBuilder("multiThreadJob", jobRepository)
                .start(step)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean(name = "multiThreadStep")
    public Step step(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("multiThreadJpaPagingItemReader2")ItemReader<User> itemReader
    ) {
        return new StepBuilder("step", jobRepository)
                .<User, User>chunk(5, platformTransactionManager)
                .reader(itemReader)
                .writer(result -> log.info(result.toString()))
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean(name = "multiThreadJpaPagingItemReader2")
    public JpaPagingItemReader<User> multiThreadJpaPagingItemReader2(
            EntityManagerFactory entityManagerFactory
    ) {
        return new JpaPagingItemReaderBuilder<User>()
                .name("multiThreadJpaPagingItemReader2")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(5)
                .saveState(false)   // 멀티 스레드 방식에서는 실패한 시점이 명확하지 않아서 꺼둠
                .queryString("SELECT u FROM USER u ORDER BY u.id")
                .build();
    }
}
