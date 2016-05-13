package com.lohika.morning.lambda.architecture.spark.driver.type;

import java.util.Objects;

public class HashTagCount {

    private final String hashTag;
    private final Long count;

    public HashTagCount(final String hashTag, final Long count) {
        this.hashTag = hashTag;
        this.count = count;
    }

    public String getHashTag() {
        return hashTag;
    }

    public Long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashTagCount that = (HashTagCount) o;
        return Objects.equals(hashTag, that.hashTag) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashTag, count);
    }

    @Override
    public String toString() {
        return "HashTagCount{" +
                "hashTag='" + hashTag + '\'' +
                ", count=" + count +
                '}';
    }
}
