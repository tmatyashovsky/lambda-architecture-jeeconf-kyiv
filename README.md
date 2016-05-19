# Sample Application for "Lambda Architecture with Apache Spark" Presentation

## Presentation
Link to the presentation: http://www.slideshare.net/tmatyashovsky/lambda-architecture-with-apache-spark

## Idea
Provide hashtags statistics used in a tweets filtered by particular hashtag, e.g. *#morningatlohika*, *#jeeconf*, etc.
Statistics should be provided from all time till today + **right now**.

### Batch View
Batch view creation is simplified, i.e. it is assumed that there is a batch layer that processed all historical tweets and produced some output. BatchViewTest can be used to generated batch view in *.parquet* format with data listed below:
* apache – 6
* architecture – 12
* aws – 3
* java – 4
* jeeconf – 7
* lambda – 6
* morningatlohika – 15 
* simpleworkflow – 14
* spark – 5

### Real-time View
Assuming application had received new tweet like 
*"Cool presentation by @tmatyashovsky about #lambda #architecture using #apache #spark at #jeeconf"* 
real-time view will be as following:
* apache – 1
* architecture – 1
* jeeconf – 1
* lambda – 1
* spark – 1

### Query, i.e. Batch View + Real-time View
Upon receiving a request query will merge batch view and real-time view to get the following result:
* apache – 7
* architecture – 7
* aws – 3
* java – 4
* jeeconf – 8
* lambda – 7
* morningatlohika – 15 
* simpleworkflow – 14
* spark – 6

### Simplified Steps
* Create batch view (.parquet) via Apache Spark
* Cache batch view in Apache Spark
* Start streaming application connected to Twitter
* Focus on real-time #jeeconf tweets*
* Build incremental real-time views
* Query, i.e. merge batch and real-time views on a fly

Stream from file system (used for testing) can be used as a backup

## Build, Configure and Run

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
All available configuration properties are spread out via 3 files:
* application.properties - contains business logic specific stuff
* spark.properties - contains Spark specific stuff
* twitter4j.properties - contains credentials needed to make authorization call to Twitter

All properties are self explanatory, but few the most important ones are listed explicitly below. 

#### Application Properties
| Name | Type | Default value | Description |
| ---- | ---- | ------------- | ----------- |
| server.port | Integer | 9090 | The port to listen for incoming HTTP requests |

#### Spark Properties
| Name | Type | Default value | Description |
| ---- | ---- | ------------- | ----------- |
| spark.master | String | spark://127.0.0.1:7077 | The URL of the Spark master. For development purposes, you can use `local[n]` that will run Spark on n threads on the local machine without connecting to a cluster. For example, `local[2]`. |

#### Sample configuration for a local development environment
Create application.properties (for instance, in your user home directory) and override any of the described properties. For instance, minimum set of values that should be specified for your local environment is listed below:
```
spark.distributed-libraries=<path_to_your_repo>/spark-distributed-library/build/libs/spark-distributed-library-1.0-SNAPSHOT-all.jar

batch.view.file.path=<path_to_your_repo>/spark-driver/src/test/resources/batch-views/batch-view.parquet
```
Create twitter4j.properties (for instance, in your user home directory) and use the following properties for your local environment:
```
oauth.consumerKey=<replace with your own>
oauth.consumerSecret=<replace with your own>
oauth.accessToken=<replace with your own>
oauth.accessTokenSecret=<replace with your own>
```
Make sure that you have entered correct values that correspond to your twitter account. 

### Run
In order to run the application please add spring.config.location parameter that corresponds to directory that contains your custom application.properties and twitter4j.properties (in our example your user home directory). Or just enumerate them explicitly, for instance:
```
spring.config.location=/Users/<your user>/application.properties,/Users/<your user>/twitter4j.properties
```

