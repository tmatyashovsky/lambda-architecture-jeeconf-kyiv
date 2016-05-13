package com.lohika.morning.lambda.architecture.analytics.api.service;

import com.lohika.morning.lambda.architecture.spark.driver.service.query.QueryService;
import com.lohika.morning.lambda.architecture.spark.driver.type.HashTagCount;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsService {

    @Autowired
    private QueryService queryService;

    public List<HashTagCount> getTwitterHashTagsCount() {
        return queryService.mergeHashTagsCount();
    }

}
