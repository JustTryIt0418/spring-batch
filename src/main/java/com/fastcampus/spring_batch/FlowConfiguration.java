package com.fastcampus.spring_batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class FlowConfiguration {

    @Bean
    public Job flowJob(
        JobRepository jobRepository,
        @Qualifier("step1") Step step1,
        @Qualifier("step2") Step step2,
        @Qualifier("step3") Step step3
    ) {
        return new JobBuilder("flowJob", jobRepository)
                .start(step1)
                    .on("COMPLETED").stopAndRestart(step2)
//                    .on("*").to(step2)
//                .from(step1)
//                    .on("FAILED").to(step3)
//                    .on("FAILED").end()     // 실패하더라도 완료 처리
//                    .on("FAILED").fail()
                .end()
                .build();
    }

    @Bean(name = "step1")
    public Step step1(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("step1", jobRepository)
                .tasklet((a, b) -> {
                    log.info("step1 실행");
//                    if (1==1) throw new IllegalStateException("예외 발생");
                    return null;
                }, platformTransactionManager)
                .build();
    }

    @Bean(name = "step2")
    public Step step2(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("step2", jobRepository)
                .tasklet((a, b) -> {
                    log.info("step2 실행");
                    return null;
                }, platformTransactionManager)
                .build();
    }

    @Bean(name = "step3")
    public Step step3(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("step3", jobRepository)
                .tasklet((a, b) -> {
                    log.info("step3 실행");
                    return null;
                }, platformTransactionManager)
                .build();
    }
}
