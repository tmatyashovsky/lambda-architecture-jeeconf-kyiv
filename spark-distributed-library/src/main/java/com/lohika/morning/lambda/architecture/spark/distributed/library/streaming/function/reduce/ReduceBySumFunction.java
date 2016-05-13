package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.reduce;

import org.apache.spark.api.java.function.Function2;

public class ReduceBySumFunction implements Function2<Long, Long, Long> {

    @Override
    public Long call(Long count1, Long count2) throws Exception {
        return count1 + count2;
    }

}
