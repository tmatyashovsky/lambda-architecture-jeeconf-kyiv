package com.lohika.morning.lambda.architecture.spark.distributed.library.streaming.function.filter;

import java.util.Date;
import org.apache.spark.api.java.function.Function;
import twitter4j.Status;

public class FilterTweetsByDate implements Function<Status, Boolean> {

    private final Date date;

    public FilterTweetsByDate(final Date date) {
        this.date = date;
    }

    @Override
    public Boolean call(Status twitterStatus) throws Exception {
        return twitterStatus.getCreatedAt().compareTo(date) > 0;
    }

}
