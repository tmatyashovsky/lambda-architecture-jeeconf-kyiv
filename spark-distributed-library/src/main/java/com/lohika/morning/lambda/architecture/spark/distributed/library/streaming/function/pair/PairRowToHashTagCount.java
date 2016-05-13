package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.pair;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class PairRowToHashTagCount implements PairFunction<Row, String, Long> {

    @Override
    public Tuple2<String, Long> call(Row row) throws Exception {
        return new Tuple2<>(row.getString(0), row.getLong(1));
    }

}
