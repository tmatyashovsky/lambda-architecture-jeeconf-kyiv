package com.lohika.morning.lambda.architecture.spark.driver.service.query;

import com.lohika.morning.lambda.architecture.spark.distributed.library.type.SchemaUtils;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.View;
import com.lohika.morning.lambda.architecture.spark.driver.service.serving.ServingService;
import com.lohika.morning.lambda.architecture.spark.driver.type.HashTagCount;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class QueryServiceTest extends BaseQueryTest {

    @Autowired
    private QueryService queryService;

    @Autowired
    private ServingService servingService;

    @Test
    public void shouldMergeHashTagsCount() throws IOException, InterruptedException {
        prepareRealTimeView();
        servingService.setBatchViewFilePath(getPathToBatchView());

        List<HashTagCount> expectedResults = new ArrayList<>();
        expectedResults.add(new HashTagCount("apache", 7L));
        expectedResults.add(new HashTagCount("architecture", 12L));
        expectedResults.add(new HashTagCount("aws", 4L));
        expectedResults.add(new HashTagCount("java", 4L));
        expectedResults.add(new HashTagCount("jeeconf", 8L));
        expectedResults.add(new HashTagCount("lambda", 6L));
        expectedResults.add(new HashTagCount("morningatlohika", 16L));
        expectedResults.add(new HashTagCount("simpleworkflow", 15L));
        expectedResults.add(new HashTagCount("spark", 6L));

        List<HashTagCount> actualResults = queryService.mergeHashTagsCount();
        assertThat(actualResults, is(expectedResults));
    }

    private void prepareRealTimeView() {
        List<Row> hashTagsCount = new ArrayList<>();
        hashTagsCount.add(RowFactory.create("morningatlohika", 1L));
        hashTagsCount.add(RowFactory.create("apache", 1L));
        hashTagsCount.add(RowFactory.create("spark", 1L));
        hashTagsCount.add(RowFactory.create("jeeconf", 1L));
        hashTagsCount.add(RowFactory.create("aws", 1L));
        hashTagsCount.add(RowFactory.create("simpleworkflow", 1L));

        DataFrame realTimeView = getAnalyticsSparkContext().getSqlContext().createDataFrame(hashTagsCount,
            SchemaUtils.generateSchemaStructure());

        realTimeView.registerTempTable(View.REAL_TIME_VIEW.getValue());
    }

}
