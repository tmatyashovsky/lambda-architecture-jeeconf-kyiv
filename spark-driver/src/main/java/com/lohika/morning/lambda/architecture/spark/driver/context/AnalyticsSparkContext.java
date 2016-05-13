package com.lohika.morning.lambda.architecture.spark.driver.context;

import com.lohika.morning.lambda.architecture.spark.driver.service.speed.StreamingService;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

@Component
public class AnalyticsSparkContext implements InitializingBean {

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private JavaStreamingContext javaStreamingContext;

    @Autowired
    private SparkContext sparkContext;

    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public JavaStreamingContext getJavaStreamingContext() {
        return javaStreamingContext;
    }

    @Value("${spark.streaming.batch.duration.seconds}")
    private Integer batchDurationInSeconds;

    @Value("${spark.streaming.remember.duration.seconds}")
    private Integer rememberDurationInSeconds;

    @Value("${spark.streaming.context.enable}")
    private Boolean enableStreamingContext;

    @Value("${spark.streaming.checkpoint.directory}")
    private String checkpointDirectory;

    @Value("${twitter.filter.text}")
    private String filterText;

    @Value("${oauth.consumerKey}")
    private String oauthConsumerKey;

    @Value("${oauth.consumerSecret}")
    private String oauthConsumerSecret;

    @Value("${oauth.accessToken}")
    private String oauthAccessToken;

    @Value("${oauth.accessTokenSecret}")
    private String oauthAccessTokenSecret;

    @Autowired
    private StreamingService streamingService;

    @Override
    public void afterPropertiesSet() throws Exception {
        javaSparkContext = new JavaSparkContext(sparkContext);
        sqlContext = new SQLContext(sparkContext);

        if (enableStreamingContext) {
            JavaStreamingContextFactory javaStreamingContextFactory = () -> {
                JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
                        Durations.seconds(batchDurationInSeconds));

                JavaReceiverInputDStream<Status> twitterStatuses = TwitterUtils.createStream(javaStreamingContext,
                    createTwitterAuthorization(), new String[]{filterText});

                // Pipelines.
                streamingService.incrementRealTimeView(twitterStatuses);

                javaStreamingContext.remember(Durations.seconds(rememberDurationInSeconds));
                javaStreamingContext.checkpoint(checkpointDirectory);

                return javaStreamingContext;
            };

            javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointDirectory, javaStreamingContextFactory);
            javaStreamingContext.start();
        } else {
            javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(batchDurationInSeconds));
        }
    }

    private Authorization createTwitterAuthorization() {
        Configuration configuration = new ConfigurationBuilder()
                .setOAuthConsumerKey(oauthConsumerKey)
                .setOAuthConsumerSecret(oauthConsumerSecret)
                .setOAuthAccessToken(oauthAccessToken)
                .setOAuthAccessTokenSecret(oauthAccessTokenSecret)
                .build();

        return new OAuthAuthorization(configuration);
    }

}
