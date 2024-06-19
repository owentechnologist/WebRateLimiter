# Multi-modal Redis-based Application with many features:

### WebRateLimiter and Redis Search application

### The sliding window rate limiter pattern used in this app is borrowed from this python example available here:
https://github.com/maguec/RateLimitingExample/tree/sliding_window

This is a Java code example of limiting number of requests to a webserver using Redis SortedSets API

This example embeds a Java webserver ( https://sparkjava.com/documentation ) 

#### This web-app is the front end of a micro-services demo involving a deduper and a search lookup.


![UI](multimodalRedis.png)

![services](multimodalServices.png)

The main flow of the application is driven by the user who submits possible city names for verification / correction:
![commonflow](commonflow.png)

Behind the scenes: by two separate microservices (in an asynchronous fashion) -the requests are deduped and then processed through a redis search effort:

![asyncflow](asynchronousflow.png)

The premise of the overall demo is - spell check / cleanse submitted city names.

Reference [good] City address data is loaded from a csv file populated with data from a free data set provided by: https://simplemaps.com/


The various services are connected asynchronously through redis streams.

They all emit a heartbeat to redis TimeSeries every 10 seconds to show they are healthy.
Try this query:  

```
TS.MRANGE - + AGGREGATION count 60000 FILTER measure=heartbeat GROUPBY servicename reduce avg 
```

One service loads the redis database with Hashes containing Canadian city names and a nod to NY.
It also creates the search index so that others can search for citynames.

Another dedups the entries made by users so the spellchecking/lookup/search effort is done only one time for each unique entry. 

The last service does the search lookup using phonetic and fuzzy matching to grab the closest match and writes the best match to a stream.

To run the example:
* build the project in an environment supporting Maven (getting the jars manually is a pain)
* edit the jedisconnectionfactory.properties file to match your Redis Server details
  (make sure your redis instance supports the TimeSeries, Search, and Blooom modules)

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false 
```

If you want to have each of the 4 services started with a lengthy pause between them you can pass an argument to the Main.main method like this:

``` 
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="goslow"
```
(you will have to wait 4 + minutes for the full launch in this case)

## General information for the user:
From a browser use http://[host]:4567?accountKey=[yourKey]

Example ...  http://127.0.0.1:4567?accountKey=007

The response will show how many requests have been made in the last minute and last hour.

Each accountKey coming from a particular IP address is allowed 3 requests/minute and 25 requests/hour

These limits are defined in WebRateLimitService.java:

    int ratePerMinuteAllowed = 5;
    int ratePerHourAllowed = 25;

Hit the webserver a few times with requests with the same accountKey to see the response change from a friendly welcome to a friendly -- too many requests.

Due to the use of zremrangeByScore and the score being equal to the time the request occurred, the application does a good job 
of providing a sliding window of allowed requests.

Each request made through the web-UI results in an entry being added to a redis stream that will be processed by various services. 


To see what is happening in Redis you can use RedisInsight   https://redislabs.com/redis-enterprise/redis-insight/
(look at the streams section to see entries being added to the streams)
(look at the SortedSets to see what rate-limiting data is being processed there)
or redis-cli:

127.0.0.1:6379> keys z:*
1) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute"
2) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour"

127.0.0.1:6379> zrange z:rateLimiting:127.0.0.1:GET:accountKey=008:minute 0 -1 withscores
 1) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute:17-17-41:759"
 2) "1611011861759"
 3) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute:17-17-42:555"
 4) "1611011862555"
 5) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute:17-17-43:471"
 6) "1611011863471"
 7) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute:17-17-44:012"
 8) "1611011864012"
 9) "z:rateLimiting:127.0.0.1:GET:accountKey=008:minute:17-17-45:347"
10) "1611011865347"

127.0.0.1:6379> zrange z:rateLimiting:127.0.0.1:GET:accountKey=008:hour 0 -1 withscores
 1) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-15-59:959"
 2) "1611011759959"
 3) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-16-05:143"
 4) "1611011765143"
 5) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-16-06:754"
 6) "1611011766754"
 7) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-16-07:412"
 8) "1611011767412"
 9) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-16-09:370"
10) "1611011769370"
11) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-17-41:759"
12) "1611011861759"
13) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-17-42:555"
14) "1611011862555"
15) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-17-43:471"
16) "1611011863471"
17) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-17-44:012"
18) "1611011864012"
19) "z:rateLimiting:127.0.0.1:GET:accountKey=008:hour:17-17-45:347"
20) "1611011865347"

