package com.lohika.morning.lambda.architecture.analytics.api.controller;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StreamingController {

    @Value("${spark.streaming.directory}")
    private String streamingDirectory;

    @RequestMapping(value = "/streaming/{times}", method = RequestMethod.GET)
    ResponseEntity<String> triggerStreaming(@PathVariable Integer times) {
        IntStream.range(0, times).forEach(i -> triggerStreaming());

        return new ResponseEntity<>(String.format("Triggered streaming from file system %s time(s)", times), HttpStatus.OK);
    }

    private void triggerStreaming() {
        try {
            Thread.sleep(100);

            Files.copy(Paths.get(streamingDirectory + "/tweets.txt"),
                    Paths.get(streamingDirectory + "/trigger-streaming at " + System.currentTimeMillis() + ".txt"));
        } catch (Exception exception) {
            throw new RuntimeException(exception.getMessage());
        }
    }

}
