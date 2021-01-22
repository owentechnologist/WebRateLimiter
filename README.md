# WebRateLimiter. -- based off of the example provided by Chris Mague here: 
https://github.com/Redislabs-Solution-Architects/RateLimitingExample/tree/sliding_window
# chris also makes his example available here:
https://github.com/maguec/RateLimitingExample/tree/sliding_window

A simple Java code example of limiting number of requests to a webserver using Redis SortedSets API

This example embeds a Java webserver ( https://sparkjava.com/documentation ) 

to use: 

* build the project in an environment supporting Maven (getting the jars manually is a pain)
* edit the jedisconnectionfactory.properties file to match your Redis Server details
* run the Application providing the path to the jedisconnectionfactory.properties file as an argument to Main
Example: com.redislabs.sa.ot.Main /Users/owentaylor/IdeaProjects/WebRateLimiter/src

From a browser use http://[host]:4567?accountKey=[yourKey]

Example ...  http://127.0.0.1:4567?accountKey=007

The response will show how many requests have been made in the last minute and last hour.

Each accountKey coming from a particular IP address is allowed 4 requests/minute and 25 requests/hour

These limits are defined in WebRateLimitService.java:

    int ratePerMinuteAllowed = 4;
    int ratePerHourAllowed = 25;

Hit the webserver a few times with your request to see the reponse change from a friendly welcome to a friendly -- too many requests.

Due to the use of zremrangeByScore and the score being equal to the time the request occurred, the application does a good job 
of providing a sliding window of allowed requests.

To see what is happening in Redis you can use RedisInsight   https://redislabs.com/redis-enterprise/redis-insight/

or redis-cli:

127.0.0.1:6379> keys *
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

