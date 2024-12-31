package com.fastcampus.spring_batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;
import java.util.List;

@Configuration
public class ItemProcessorJobConfiguration {

    @Bean
    public Job itemProcessorJob(
            JobRepository jobRepository,
            @Qualifier("itemProcessorStep") Step step
    ) {
        return new JobBuilder("itemProcessorJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean(name = "itemProcessorStep")
    public Step itemReaderStep(
            JobRepository jobRepository,
            PlatformTransactionManager platformTransactionManager,
            @Qualifier("flatFileItemReader3") ItemReader<User> flatFileItemReader
    ) {
        List<ItemProcessor<User, User>> processorList = Arrays.asList(customProcessor2(), customProcessor3(), customProcessor4());

        return new StepBuilder("step", jobRepository)
                .<User, String>chunk(2, platformTransactionManager)
                .reader(flatFileItemReader)
                .processor(new CompositeItemProcessor<>(processorList))
//                .processor(customProcessor1())
                .writer(System.out::println)
                .build();
    }

    @Bean(name = "flatFileItemReader3")
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

    private ItemProcessor<User, String> customProcessor1() {
        return user -> {
            if (user.getName().equals("이민수")) return null;

            return "%s의 나이는 %s입니다. 지역은 %s, 전화번호는 %s입니다."
                    .formatted(
                            user.getName(),
                            user.getAge(),
                            user.getRegion(),
                            user.getTelephone()
                    );
        };
    }

    private ItemProcessor<User, User> customProcessor2() {
        return user -> {
            user.setName(user.getName()+user.getName());
            return user;
        };
    }

    private ItemProcessor<User, User> customProcessor3() {
        return user -> {
            user.setAge(user.getAge()+user.getAge());
            return user;
        };
    }

    private ItemProcessor<User, User> customProcessor4() {
        return user -> {
            user.setRegion(user.getRegion()+user.getRegion());
            return user;
        };
    }
}
