package com.lohika.morning.lambda.architecture.analytics.api.controller;

import com.lohika.morning.lambda.architecture.analytics.api.service.AnalyticsService;
import com.lohika.morning.lambda.architecture.spark.driver.type.HashTagCount;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnalyticsController {

    @Autowired
    private AnalyticsService analyticsService;

    @RequestMapping(value = "/analytics", method = RequestMethod.GET)
    ResponseEntity<List<HashTagCount>> getAnalytics() {
        List<HashTagCount> twitterHashTagsCount =
                analyticsService.getTwitterHashTagsCount();

        if (!twitterHashTagsCount.isEmpty()) {
            return new ResponseEntity<>(twitterHashTagsCount, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
