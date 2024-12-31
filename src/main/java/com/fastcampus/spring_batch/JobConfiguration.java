package com.fastcampus.spring_batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class JobConfiguration {

    @Bean
    public Job job(JobRepository jobRepository,
                   @Qualifier("step") Step step) {
        return new JobBuilder("job", jobRepository)
                .start(step)
                .build();
    }

    @Bean(name = "step")
    @JobScope
    public Step step(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Value("#{jobParameters['name']}") String name
    ) {
        log.info("name : {}", name);
        return new StepBuilder("step", jobRepository)
                .tasklet((a, b) -> RepeatStatus.FINISHED, platformTransactionManager)
                .build();
    }

//    @Bean
//    public Step step(
//            JobRepository jobRepository,
//            PlatformTransactionManager platformTransactionManager
//    ) {
//        ItemReader<Integer> itemReader = new ItemReader<>() {
//
//            private int count = 0;
//
//            @Override
//            public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
//                count++;
//
//                log.info("Read {}", count);
//
//                if (count == 20) return null;
//
//                /*if (count >= 15) {
//                    throw new IllegalStateException("예외 발생");
//                }*/
//
//                return count;
//            }
//        };
//
//        ItemProcessor<Integer, Integer> itemProcessor = new ItemProcessor<>() {
//            @Override
//            public Integer process(Integer item) throws Exception {
//
//                if (item == 15) {
//                    throw new IllegalStateException("예외 발생");
//                }
//
//                return item;
//            }
//        };
//
//        return new StepBuilder("job-chunk", jobRepository)
//                .<Integer, Integer>chunk(10, platformTransactionManager)
//                .reader(itemReader)
//                .processor(itemProcessor)
//                .writer(read -> {})
//                .allowStartIfComplete(true)
//                .faultTolerant()
////                .skipPolicy((t, skipCount) -> t instanceof IllegalStateException && skipCount < 5)
//                .retry(IllegalStateException.class)
//                .retryLimit(5)
//                .build();
//
//        /*return new StepBuilder("step", jobRepository)
//                .tasklet((stepContribution, chunkContext) -> {
//                    log.info("step 실행");
//                    return RepeatStatus.FINISHED;
//                }, platformTransactionManager)
//                .allowStartIfComplete(true)
//                .build();*/
//    }

//    @Bean
//    public Step step(
//            JobRepository jobRepository,
//            PlatformTransactionManager platformTransactionManager
//    ) {
//        /*Tasklet tasklet = new Tasklet() {
//
//            private int count = 0;
//
//            @Override
//            public RepeatStatus execute(StepContribution a, ChunkContext b) throws Exception {
//                count++;
//
//                if (count == 15) {
//                    log.info("Tasklet FINISHED");
//                    return RepeatStatus.FINISHED;
//                }
//
//                log.info("Tasklet CONTINUABLE {}", count);
//                return RepeatStatus.CONTINUABLE;
//            }
//        };*/
//
//        ItemReader<Integer> itemReader = new ItemReader<>() {
//
//            private int count = 0;
//
//            @Override
//            public Integer read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
//                count++;
//
//                log.info("Read {}", count);
//
//                if (count == 15) {
//                    return null;
//                }
//
//                return count;
//            }
//        };
//
//        return new StepBuilder("job-chunk", jobRepository)
////                .tasklet(tasklet, platformTransactionManager)
//                .chunk(10, platformTransactionManager)
//                .reader(itemReader)
//                //.processor()
//                .writer(read -> {})
//                .build();
//    }
}
