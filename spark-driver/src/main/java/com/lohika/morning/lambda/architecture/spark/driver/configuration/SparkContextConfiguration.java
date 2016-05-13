package com.lohika.morning.lambda.architecture.spark.driver.configuration;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@PropertySource({"classpath:spark.properties", "classpath:twitter4j.properties"})
@ComponentScan("com.lohika.morning.lambda.architecture.spark.driver.*")
public class SparkContextConfiguration {

    @Bean
    public SparkContext sparkContext() {
        return new SparkContext(sparkConfiguration());
    }

    @Bean
    public SparkConf sparkConfiguration() {
        return sparkConfigurationBuilder().buildSparkConfiguration();
    }

    @Value("${spark.master}")
    private String master;

    @Value("${spark.application-name}")
    private String applicationName;

    @Value("${spark.distributed-libraries}")
    private String[] distributedLibraries;

    @Value("${spark.cores.max}")
    private String coresMax;

    @Value("${spark.executor.memory}")
    private String executorMemory;

    @Value("${spark.serializer}")
    private String serializer;

    @Value("${spark.sql.shuffle.partitions}")
    private String sqlShufflePartitions;

    @Value("${spark.default.parallelism}")
    private String defaultParallelism;

    @Bean
    public SparkConfigurationBuilder sparkConfigurationBuilder() {
        return new SparkConfigurationBuilder(master, applicationName, distributedLibraries, sparkProperties());
    }

    private Map<String, String> sparkProperties() {
        Map<String, String> sparkProperties = new HashMap<>();
        sparkProperties.put("spark.cores.max", coresMax);
        sparkProperties.put("spark.executor.memory", executorMemory);
        sparkProperties.put("spark.serializer", serializer);
        sparkProperties.put("spark.sql.shuffle.partitions", sqlShufflePartitions);
        sparkProperties.put("spark.default.parallelism", defaultParallelism);

        return sparkProperties;
    }

    @Bean
    // Static is extremely important here.
    // It should be created before @Configuration as it is also component.
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}



