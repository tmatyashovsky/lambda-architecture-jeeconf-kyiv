package com.lohika.morning.lambda.architecture.spark.driver.service.speed;

import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.filter.FilterTweetsByDate;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map.FlatMapHashTagsToPair;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.reduce.ReduceBySumFunction;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.save.SaveHashTagsCount;
import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.state.UpdateStateByHashTag;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.SchemaUtils;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.View;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.View.REAL_TIME_VIEW;
import com.lohika.morning.lambda.architecture.spark.driver.context.AnalyticsSparkContext;
import java.util.ArrayList;
import java.util.Date;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalog.Table;
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

    public Dataset<Row> getRealTimeView() {
        Dataset<Table> tables = analyticsSparkContext.getSparkSession().catalog().listTables();
        Dataset<Table> realTimeViewTable = tables.filter(tables.col("name").equalTo(View.REAL_TIME_VIEW.getValue()));
        boolean isRealTimeViewCreated = realTimeViewTable.count() == 1;

        if (isRealTimeViewCreated) {
            return analyticsSparkContext.getSparkSession().table(REAL_TIME_VIEW.getValue());
        } else {
            return createEmptyRealTimeView();
        }
    }

    private Dataset<Row> createEmptyRealTimeView() {
        Dataset<Row> emptyRealTimeView = analyticsSparkContext.getSparkSession().createDataFrame(new ArrayList<>(),
            SchemaUtils.generateSchemaStructure());

        emptyRealTimeView.cache();
        emptyRealTimeView.count();
        emptyRealTimeView.createOrReplaceTempView(REAL_TIME_VIEW.getValue());

        return emptyRealTimeView;
    }

}
