package com.lohika.morning.lambda.architecture.spark.distributed.library.type;

public enum View {

    REAL_TIME_VIEW("real_time_view"),

    BATCH_VIEW("batch_view");

    private final String value;

    View(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
