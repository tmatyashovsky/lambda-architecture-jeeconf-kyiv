package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.state;

import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;

public class UpdateStateByHashTag implements Function2<List<Long>, Optional<Long>, Optional<Long>> {

    public Optional<Long> call(List<Long> counts, Optional<Long> result) throws Exception {
        long sum = result.or(0L);
        for (long count : counts) {
            sum += count;
        }

        return Optional.of(sum);
    }

}
