package com.lohika.morning.lambda.architecture.spark.driver.service.speed;

import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.filter.FilterTweetsByDate;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map.FlatMapHashTagsToPair;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.reduce.ReduceBySumFunction;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.save.SaveHashTagsCount;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.state.UpdateStateByHashTag;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.SchemaUtils;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.View.REAL_TIME_VIEW;
import com.lohika.morning.lambda.architecture.spark.driver.context.AnalyticsSparkContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class StreamingService {

    @Autowired
    private AnalyticsSparkContext analyticsSparkContext;

    @Value("#{new java.text.SimpleDateFormat(\"yyyyMMdd\").parse(\"${twitter.filter.date}\")}")
    private Date date;

    public void incrementRealTimeView(final JavaDStream<Status> twitterStatuses) {
        JavaDStream<Status> filteredTweets = twitterStatuses
                .filter(new FilterTweetsByDate(date));

        JavaPairDStream<String, Long> hashTagPairs = filteredTweets
                .flatMapToPair(new FlatMapHashTagsToPair());

        JavaPairDStream<String, Long> reducedHashTagPairs = hashTagPairs
                .reduceByKey(new ReduceBySumFunction());

        JavaPairDStream<String, Long> realTimeIncrement = reducedHashTagPairs
                .updateStateByKey(new UpdateStateByHashTag());

        realTimeIncrement.foreachRDD(new SaveHashTagsCount());
    }

    public DataFrame getRealTimeView() {
        String[] tableNames = analyticsSparkContext.getSqlContext().tableNames();
        boolean isRealTimeViewCreated = Arrays.stream(tableNames)
                .anyMatch(tableName -> tableName.equals(REAL_TIME_VIEW.getValue()));

        if (isRealTimeViewCreated) {
            return analyticsSparkContext.getSqlContext().table(REAL_TIME_VIEW.getValue());
        } else {
            return createEmptyRealTimeView();
        }
    }

    private DataFrame createEmptyRealTimeView() {
        DataFrame emptyRealTimeView = analyticsSparkContext.getSqlContext().createDataFrame(new ArrayList<>(),
            SchemaUtils.generateSchemaStructure());

        emptyRealTimeView.cache();
        emptyRealTimeView.count();
        emptyRealTimeView.registerTempTable(REAL_TIME_VIEW.getValue());

        return emptyRealTimeView;
    }

}
