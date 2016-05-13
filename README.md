# Media/Data real-time analytics

### Build
Standard build:
```
./gradlew clean build shadowJar
```
Quick build without tests:
```
./gradlew clean build shadowJar -x test
```
Build and test:
```
./gradlew clean build -Dspring.profiles.active=kafkatest
```
"kafkatest" is a Spring profile used for JUnit tests.
```

## Configuration
| Name | Type | Default value | Description |
| ---- | ---- | ------------- | ----------- |
| server.port | Integer | 9090 | The port to listen for incoming HTTP requests |
| spark.master | String | spark://127.0.0.1:7077 | The URL of the Spark master. For development purposes, you can use `local[n]` that will run Spark on n threads on the local machine without connecting to a cluster. For example, `local[2]`. |
**TODO**: describe all other settings

### Sample configuration for a local development environment
```
Create spark.properties and spark-test.properties in your user home directory and use the following properties for your local environment.

spark.properties
```
spark.distributed-libraries=<path_to_your_repo>/kaizen/real-time-reporting/spark-distributed-library/build/libs/spark-distributed-library-1.0-SNAPSHOT-all.jar
spark.streaming.checkpoint.directory=/tmp/spark/checkpoint
historical.data.file.path=<path_to_your_repo>/kaizen/real-time-reporting/spark-driver/src/test/resources/test-historical-data/historical-data.parquet
spark.streaming.input.kafka.brokers=localhost:9092
spark.master=spark://127.0.0.1:7077
```
spark-test.properties:
```
log.path.bidder=<path_to_your_files>/temp_bid.log
log.path.impressions=<path_to_your_files>/temp_impression.log
sample.parquet.adserver.file=<path_to_your_files>/adserver.parquet
sample.parquet.bid.statistics.file=<path_to_your_files>/bid_statistics.parquet
```
You may need to ask your colleagues about the sample logs. They are really large and not supposed to be committed in a Git repo.


## Preparation of a development environment
* [Build](#Build) the project
**Note**: for the instructions on how to set up a local Kafka cluster, look [here](kafka-tapper/README.md)
