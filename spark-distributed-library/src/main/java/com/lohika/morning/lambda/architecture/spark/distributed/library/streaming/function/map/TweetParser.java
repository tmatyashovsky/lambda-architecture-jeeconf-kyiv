package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lohika.morning.lambda.architecture.spark.distributed.library.type.dummy.DummyTwitterStatus;
import org.apache.spark.api.java.function.Function;
import twitter4j.Status;

public class TweetParser implements Function<String, Status> {

    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
    }

    @Override
    public Status call(String json) throws Exception {
        return objectMapper.readValue(json, DummyTwitterStatus.class);
    }

}
