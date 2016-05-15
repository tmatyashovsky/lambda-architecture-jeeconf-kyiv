# Sample Application for "Lambda Architecture with Apache Spark" Presentation

## Presentation
Link to the presentation: http://www.slideshare.net/tmatyashovsky/lambda-architecture-with-apache-spark

### Build
Standard build:
```
./gradlew clean build shadowJar
```
Quick build without tests:
```
./gradlew clean build shadowJar -x test
```
### Configuration
| Name | Type | Default value | Description |
| ---- | ---- | ------------- | ----------- |
| server.port | Integer | 9090 | The port to listen for incoming HTTP requests |
| spark.master | String | spark://127.0.0.1:7077 | The URL of the Spark master. For development purposes, you can use `local[n]` that will run Spark on n threads on the local machine without connecting to a cluster. For example, `local[2]`. |

#### Sample configuration for a local development environment

Create application.properties in your user home directory and use the following properties for your local environment:
```
spark.master=spark://127.0.0.1:7077

spark.distributed-libraries=<path_to_your_repo>/spark-distributed-library/build/libs/spark-distributed-library-1.0-SNAPSHOT-all.jar

batch.view.file.path=<path_to_your_repo>/spark-driver/src/test/resources/batch-views/batch-view.parquet

spark.streaming.batch.duration.seconds=10
```
Create twitter4j.properties in your user home directory and use the following properties for your local environment:
```
oauth.consumerKey=<replace with your own>
oauth.consumerSecret=<replace with your own>
oauth.accessToken=<replace with your own>
oauth.accessTokenSecret=<replace with your own>
```
Make sure that you have entered correct values that correspond to your twitter account. 

### Run
In order to run the application please add spring.config.location parameter that corresponds to directory that contains your custom application.properties and twitter4j.properties. Or just enumerate them explicitly, for instance on my machine:

```
spring.config.location=/Users/<your user>/application.properties,/Users/<your user>/twitter4j.properties
```

