package com.lohika.morning.lambda.architecture.spark.driver.service.batch;

import com.lohika.morning.lambda.architecture.spark.distributed.library.type.SchemaUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BatchTest extends BaseBatchTest {

    @Test
    public void shouldCreateBatchView() {
        List<Row> hashTagsCount = new ArrayList<>();
        hashTagsCount.add(RowFactory.create("apache", 6L));
        hashTagsCount.add(RowFactory.create("architecture", 12L));
        hashTagsCount.add(RowFactory.create("itvechornyci", 12L));
        hashTagsCount.add(RowFactory.create("java", 4L));
        hashTagsCount.add(RowFactory.create("lambda", 6L));
        hashTagsCount.add(RowFactory.create("spark", 5L));

        Dataset batchView = getAnalyticsSparkContext().getSparkSession().createDataFrame(hashTagsCount,
            SchemaUtils.generateSchemaStructure());

        String parquetOutputFile = this.getClass().getResource("/").getPath()
            + "batch-view-" + System.currentTimeMillis() + ".parquet";

        batchView.write().parquet(parquetOutputFile);
        System.out.println("Created batch view at: " + parquetOutputFile);

        // Assert that the file was written.
        Dataset actualBatchView = getAnalyticsSparkContext().getSparkSession().read().parquet(parquetOutputFile);
        actualBatchView = actualBatchView.orderBy(actualBatchView.col("hashTag"));
        assertEquals(batchView.collectAsList(), actualBatchView.collectAsList());
    }

}
