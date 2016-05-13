package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import twitter4j.Status;

public class FlatMapHashTagsToPair implements PairFlatMapFunction<Status, String, Long> {

    @Override
    public Iterable<Tuple2<String, Long>> call(Status status) throws Exception {
        return Arrays.stream(status.getHashtagEntities())
                .map(hashTag -> new Tuple2<>(hashTag.getText(), 1L))
                .collect(Collectors.toList());
    }

}
