package com.lohika.morning.lambda.architecture.spark.driver.service.speed.type;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import twitter4j.HashtagEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DummyHashtagEntity implements HashtagEntity {

    private String text;

    @Override
    public String getText() {
        return this.text;
    }

    @Override
    public int getStart() {
        return 0;
    }

    @Override
    public int getEnd() {
        return 0;
    }
}
