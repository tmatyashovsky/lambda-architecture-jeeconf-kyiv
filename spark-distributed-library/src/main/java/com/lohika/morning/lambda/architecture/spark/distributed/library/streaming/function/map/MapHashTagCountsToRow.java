package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Tuple2;

public class MapHashTagCountsToRow implements Function<Tuple2<String, Long>, Row> {

    @Override
    public Row call(Tuple2<String, Long> hashTagCounts) throws Exception {
        return RowFactory.create(hashTagCounts._1(), hashTagCounts._2());
    }

}
