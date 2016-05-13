package com.lohika.morning.lambda.architecture.spark.distributed.library.type;

public enum Column {

    HASH_TAG("hashTag"),

    COUNT("count");

    private final String value;

    Column(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
