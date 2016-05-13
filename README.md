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
## Configuration
| Name | Type | Default value | Description |
| ---- | ---- | ------------- | ----------- |
| server.port | Integer | 9090 | The port to listen for incoming HTTP requests |
| spark.master | String | spark://127.0.0.1:7077 | The URL of the Spark master. For development purposes, you can use `local[n]` that will run Spark on n threads on the local machine without connecting to a cluster. For example, `local[2]`. |

### Sample configuration for a local development environment
```
Create spark.properties and spark-test.properties in your user home directory and use the following properties for your local environment.

spark.properties
```
spark.distributed-libraries=<path_to_your_repo>/spark-distributed-library/build/libs/spark-distributed-library-1.0-SNAPSHOT-all.jar
spark.streaming.checkpoint.directory=/tmp/checkpoint
historical.data.file.path=<path_to_your_repo>/spark-driver/src/test/resources/test-historical-data/historical-data.parquet
spark.master=spark://127.0.0.1:7077
