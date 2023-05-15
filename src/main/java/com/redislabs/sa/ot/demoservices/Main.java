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
 * Adding any argument to the startup of this application deliberately causes a multi-minute delay
 * in the startup of all the 4 services - simply to create a more interesting Time Series data set
 * mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="goslow"
 */
public class Main {
    public static JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();

    public static void main(String[] args){
        if(args.length<1){
            Main.goFast();
        }else{
            Main.goSlow();
        }
        System.out.println("Kicked Off Webserver listening on port 4567");
        System.out.println("To test your rate limiting ... use http://[host]:4567?accountKey=[yourKey]");
        System.out.println("Example ...  http://127.0.0.1:4567?accountKey=007");
        System.out.println("Example2 ...  http://192.168.1.59:4567?accountKey=007");
        System.out.println(("Example 3... http://127.0.0.1:4567/cleaned-submissions?accountKey=007"));
    }

    static void goFast(){
        DedupMain.main(null);
        DataSearchBootstrapMain.main(null);
        BestMatchMain.main(null);
        WebRateLimitService service = WebRateLimitService.getInstance();
    }

    static void goSlow(){
        try {
            System.out.println("\n\nStarting 4 services with \n\tBIG \n\tpauses \n\tbetween \n\tthem \nfor TimeSeries fun...\n");
            System.out.println("Expect to wait 4 + minutes before the app is fully ready...\n\n");
            DedupMain.main(null);
            Thread.sleep(60000);
            DataSearchBootstrapMain.main(null);
            Thread.sleep(90000);
            BestMatchMain.main(null);
            Thread.sleep(120000);
            WebRateLimitService service = WebRateLimitService.getInstance();
        }catch(Throwable t){}//Thread sleep is considered risky...
    }

}