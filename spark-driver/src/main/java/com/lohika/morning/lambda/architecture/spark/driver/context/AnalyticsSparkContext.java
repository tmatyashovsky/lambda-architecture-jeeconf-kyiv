package com.lohika.morning.lambda.architecture.spark.driver.context;

import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map.TweetParser;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.View;
import com.lohika.morning.lambda.architecture.spark.driver.service.speed.StreamingService;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
    private SparkSession sparkSession;
    private JavaStreamingContext javaStreamingContext;

    @Autowired
    private SparkContext sparkContext;

    public JavaSparkContext getJavaSparkContext() {
        return javaSparkContext;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
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

    @Value("${spark.streaming.structured.enable}")
    private Boolean enableStructuredStreaming;

    @Value("${spark.streaming.checkpoint.directory}")
    private String checkpointDirectory;

    @Value("${twitter.filter.text}")
    private String twitterFilterText;

    @Value("${oauth.consumerKey}")
    private String oauthConsumerKey;

    @Value("${oauth.consumerSecret}")
    private String oauthConsumerSecret;

    @Value("${oauth.accessToken}")
    private String oauthAccessToken;

    @Value("${oauth.accessTokenSecret}")
    private String oauthAccessTokenSecret;

    @Value("${spark.streaming.directory}")
    private String streamingDirectory;

    @Value("${spark.streaming.file.stream.enable}")
    private Boolean enableFileStreamAsBackup;

    @Autowired
    private StreamingService streamingService;

    @Override
    public void afterPropertiesSet() throws Exception {
        javaSparkContext = new JavaSparkContext(sparkContext);
        sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        if (enableStreamingContext) {
            if (enableStructuredStreaming) {
                startStreamingQuery();
            } else {
                javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointDirectory, this::createJavaStreamingContext);
                javaStreamingContext.start();
            }
        } else {
            javaStreamingContext = new JavaStreamingContext(javaSparkContext, Durations.seconds(batchDurationInSeconds));
        }
    }

    private JavaStreamingContext createJavaStreamingContext() {
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(batchDurationInSeconds));

        JavaDStream<Status> twitterStatuses = null;

        if (enableFileStreamAsBackup) {
            // Fake tweets from file system used as a backup plan.
            JavaDStream<String> rawDataStream = javaStreamingContext.textFileStream(streamingDirectory);
            twitterStatuses = rawDataStream.map(new TweetParser());
        } else {
            // Real tweets from Twitter.
            twitterStatuses = TwitterUtils.createStream(javaStreamingContext,
                    createTwitterAuthorization(), new String[]{twitterFilterText});
        }

        // Pipeline is agnostic about source of the tweets.
        streamingService.incrementRealTimeView(twitterStatuses);

        javaStreamingContext.remember(Durations.seconds(rememberDurationInSeconds));
        javaStreamingContext.checkpoint(checkpointDirectory);

        return javaStreamingContext;
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

    private void startStreamingQuery() {
        Dataset<Row> artificialTweets = sparkSession.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<Row> filteredTweets = artificialTweets.filter(artificialTweets.col("value").contains(twitterFilterText));
        Dataset<Row> words = filteredTweets.withColumn("words", split(column("value"), "\\s+"));
        words = words.select(explode(column("words")).as("word"));

        Dataset<Row> hashTags = words.filter(words.col("word").startsWith("#"));
        hashTags = hashTags.withColumn("hashTag", regexp_replace(hashTags.col("word"), "#", ""));

        hashTags = hashTags.groupBy(hashTags.col("hashTag").as(Column.HASH_TAG.getValue())).count().as(Column.COUNT.getValue());

        hashTags.writeStream()
                .outputMode("complete")
                .format("memory")
                .queryName(View.REAL_TIME_VIEW.getValue())
                .start();
    }

}
