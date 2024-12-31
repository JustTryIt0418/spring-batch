package com.fastcampus.spring_batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class ParallelStepJobConfiguration {

    @Bean(name = "parallelStepJob")
    public Job parallelStepJob(
            JobRepository jobRepository,
            @Qualifier("parallelStep4") Step step4,
            @Qualifier("splitFlow") Flow splitFlow
    ) {
        return new JobBuilder("parallelStepJob", jobRepository)
                .start(splitFlow)
                .next(step4)
                .build().build();
    }

    @Bean(name = "splitFlow")
    public Flow splitFlow(
            @Qualifier("flow1") Flow flow1,
            @Qualifier("flow2") Flow flow2
    ) {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor())
                .add(flow1, flow2)
                .build();
    }

    @Bean(name = "flow1")
    public Flow flow1(
            @Qualifier("parallelStep1") Step step1,
            @Qualifier("parallelStep2")Step step2
    ) {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(step1)
                .next(step2)
                .build();
    }

    @Bean(name = "flow2")
    public Flow flow2(
            @Qualifier("parallelStep3") Step step3
    ) {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(step3)
                .build();
    }

    @Bean(name = "parallelStep1")
    public Step parallelStep1(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager
    ) {
        return new StepBuilder("parallelStep1", jobRepository)
                .tasklet((a, b) -> {
                    Thread.sleep(1000);
                    log.info("parallelStep1");
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean(name = "parallelStep2")
    public Step parallelStep2(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager
    ) {
        return new StepBuilder("parallelStep2", jobRepository)
                .tasklet((a, b) -> {
                    Thread.sleep(1000);
                    log.info("parallelStep2");
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean(name = "parallelStep3")
    public Step parallelStep3(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager
    ) {
        return new StepBuilder("parallelStep3", jobRepository)
                .tasklet((a, b) -> {
                    Thread.sleep(2500);
                    log.info("parallelStep3");
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }

    @Bean(name = "parallelStep4")
    public Step parallelStep4(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager
    ) {
        return new StepBuilder("parallelStep4", jobRepository)
                .tasklet((a, b) -> {
                    Thread.sleep(1000);
                    log.info("parallelStep4");
                    return RepeatStatus.FINISHED;
                }, platformTransactionManager)
                .build();
    }
}
