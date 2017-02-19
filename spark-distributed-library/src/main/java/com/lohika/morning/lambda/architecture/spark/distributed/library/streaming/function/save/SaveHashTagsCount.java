package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.save;

import com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map.MapHashTagCountsToRow;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.SchemaUtils;
import static com.lohika.morning.lambda.architecture.spark.distributed.library.type.View.REAL_TIME_VIEW;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveHashTagsCount implements VoidFunction2<JavaPairRDD<String, Long>, Time> {

    private static Logger LOG = LoggerFactory.getLogger(SaveHashTagsCount.class);

    @Override
    public void call(JavaPairRDD<String, Long> currentData, Time time) throws Exception {
        if (!currentData.isEmpty()) {
            LOG.info("Matched tweets received at time: " + time.toString());

            JavaRDD<Row> hashTagsCountAsRows = currentData.map(new MapHashTagCountsToRow());;

            saveToTempTable(hashTagsCountAsRows, REAL_TIME_VIEW.getValue(), currentData.context());
        } else {
            LOG.info("No matched tweets received in this time interval");
        }
    }

    private void saveToTempTable(JavaRDD<Row> hashTagCounts, String tableName, SparkContext sparkContext) {
        // Get the singleton instance of SQLContext.
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();

        Dataset<Row> dataset = sparkSession.createDataFrame(hashTagCounts, SchemaUtils.generateSchemaStructure());
        dataset.cache();
        dataset.count();
        dataset.registerTempTable(tableName);

        LOG.info(String.format("%s table successfully created", tableName));
    }

}
