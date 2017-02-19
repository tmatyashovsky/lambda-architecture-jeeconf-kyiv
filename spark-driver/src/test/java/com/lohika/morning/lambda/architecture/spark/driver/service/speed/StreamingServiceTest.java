package com.lohika.morning.lambda.architecture.spark.driver.service.speed;

import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map.TweetParser;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column.HASH_TAG;
import com.lohika.morning.lambda.architecture.spark.driver.type.HashTagCount;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import twitter4j.Status;

public class StreamingServiceTest extends BaseStreamingTest {

    public static final int TERMINATION_TIMEOUT = 5000;
    public static final int TIMES = 3;

    @Autowired
    private StreamingService streamingService;

    @Test
    public void shouldAggregateHashTagCurrentCounts() throws IOException, InterruptedException {
        JavaDStream<String> rawDataStream = getAnalyticsSparkContext().getJavaStreamingContext()
            .textFileStream(getPathForNewDataStream());
        JavaDStream<Status> tweetsStream = rawDataStream.map(new TweetParser());

        streamingService.incrementRealTimeView(tweetsStream);

        startStreamingContext();

        triggerStreaming(TIMES);

        getAnalyticsSparkContext().getJavaStreamingContext().awaitTerminationOrTimeout(TERMINATION_TIMEOUT);

        assertResults();
    }

    private void startStreamingContext() {
        getAnalyticsSparkContext().getJavaStreamingContext().remember(Durations.milliseconds(TERMINATION_TIMEOUT * 2));
        getAnalyticsSparkContext().getJavaStreamingContext().checkpoint("/tmp/tests");
        getAnalyticsSparkContext().getJavaStreamingContext().start();
    }

    private void triggerStreaming(int times) throws InterruptedException, IOException {
        IntStream.range(0, times).forEach(i -> triggerStreaming());
    }

    private void triggerStreaming() {
        try {
            Thread.sleep(1000);

            Files.copy(Paths.get(getPathForNewDataStream() + "/tweets.txt"),
                       Paths.get(getPathForNewDataStream() + "/trigger-streaming at " + System.currentTimeMillis() + ".txt"));
        } catch (Exception exception) {
            throw new RuntimeException(exception.getMessage());
        }
    }

    private void assertResults() {
        Dataset<Row> resultAsDataFrame = streamingService.getRealTimeView().orderBy(HASH_TAG.getValue());
        List<Row> resultAsRows = resultAsDataFrame.collectAsList();
        List<HashTagCount> actualResults = resultAsRows.stream()
                                                        .map(row -> new HashTagCount(row.getString(0), row.getLong(1)))
                                                        .collect(Collectors.toList());

        List<HashTagCount> expectedResults = new ArrayList<>();
        expectedResults.add(new HashTagCount("apache", 3L * TIMES));
        expectedResults.add(new HashTagCount("architecture", 1L * TIMES));
        expectedResults.add(new HashTagCount("aws", 1L * TIMES));
        expectedResults.add(new HashTagCount("jeeconf", 3L * TIMES));
        expectedResults.add(new HashTagCount("lambda", 1L * TIMES));
        expectedResults.add(new HashTagCount("morningatlohika", 3L * TIMES));
        expectedResults.add(new HashTagCount("simpleworkflow", 1L * TIMES));
        expectedResults.add(new HashTagCount("spark", 3L * TIMES));
        expectedResults.add(new HashTagCount("twitter", 1L * TIMES));

        assertThat(actualResults, is(expectedResults));
    }
}
