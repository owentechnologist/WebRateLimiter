package com.redislabs.sa.ot.demoservices;

import com.redislabs.sa.ot.city.dedup.DedupMain;
import com.redislabs.sa.ot.city.loader.DataSearchBootstrapMain;
import com.redislabs.sa.ot.city.search.BestMatchMain;
import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.webRateLimiter.WebRateLimitService;
import redis.clients.jedis.JedisPool;

/**
 *
 * This application uses several features of Redis and Redis Modules:
 * Redis Search, Redis TimeSeries, Redis Bloom, The Hash type, the String type, SortedSets, and Streams
 * To run the default configuration of this application make certain your jedisconnectionfactory.properties are correct then execute:
 *
 * mvn compile exec:java
 * That starts the following processes:
 *
 * 1) A Search Bootstrapper that loads data into Redis in the form of Hashes -
 * -  these hashes then get indexed by RedisSearch
 *
 * 2) A webserver that demonstrates limiting access to an API using a sliding window rate-limiting solution
 * (This rate limiter is based on SortedSets)
 * The Web Application allows users to hit a URL and submit a misspelled City Name
 * The submitted cityName is added to a redis stream allowing other services to pick them up and process them asynchronously
 *
 * 3) A Deduping service that picks up the stream entry from the web app, checks to see if that same city name has been
 * searched for already, and if it hasn't adds it to a second stream for processing by a different service
 *
 * 4) A best-match city name lookup service that picks yp any messages that the deduper submits and then uses redisSearch
 * and its phonetic and fuzzy matching capabilities to find the closest match which is then written to third Stream
 * so that interested parties can find the 'cleansed' version of the submitted city names.
 *
 * All of these services also use TimeSeries to submit a heartbeat every ten seconds
 * that can be tracked over time to show evidence of when they are 'up'
 *
 * You can interact with the solution through a web browser:
 * Example ...>  http://127.0.0.1:4567?accountKey=007
 * Example 2 ...> http://127.0.0.1:4567/cleaned-submissions?accountKey=007
 * (if you are not running the browser on the same machine you must provide the proper host)
 * Example: http://192.168.1.59:4567?accountKey=007
 *
 *
 */
public class Main {
    public static JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();

    public static void main(String[] args) {
        DedupMain.main(null);
        DataSearchBootstrapMain.main(null);
        BestMatchMain.main(null);
        WebRateLimitService service = WebRateLimitService.getInstance();
        System.out.println("Kicked Off Webserver listening on port 4567");
        System.out.println("To test your rate limiting ... use http://[host]:4567?accountKey=[yourKey]");
        System.out.println("Example ...  http://127.0.0.1:4567?accountKey=007");
        System.out.println("Example2 ...  http://192.168.1.59:4567?accountKey=007");
        System.out.println(("Example 3... http://127.0.0.1:4567/cleaned-submissions?accountKey=007"));
    }


}