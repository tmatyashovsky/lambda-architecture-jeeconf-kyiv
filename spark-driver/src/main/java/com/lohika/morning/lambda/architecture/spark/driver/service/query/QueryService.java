package com.lohika.morning.lambda.architecture.spark.driver.service.query;

import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column.COUNT;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.Column.HASH_TAG;
import com.lohika.morning.lambda.architecture.spark.driver.service.serving.ServingService;
import com.lohika.morning.lambda.architecture.spark.driver.service.speed.StreamingService;
import com.lohika.morning.lambda.architecture.spark.driver.type.HashTagCount;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QueryService {

    @Autowired
    private ServingService servingService;

    @Autowired
    private StreamingService streamingService;

    public List<HashTagCount> mergeHashTagsCount() {
        Dataset<Row> realTimeView = streamingService.getRealTimeView();
        Dataset<Row> batchView = servingService.getBatchView();
        Dataset<Row> mergedView = realTimeView.union(batchView)
                                           .groupBy(realTimeView.col(HASH_TAG.getValue()))
                                           .sum(COUNT.getValue())
                                           .orderBy(HASH_TAG.getValue());

        List<Row> merged = mergedView.collectAsList();

        return merged.stream()
                      .map(row -> new HashTagCount(row.getString(0), row.getLong(1)))
                      .collect(Collectors.toList());
    }

}
