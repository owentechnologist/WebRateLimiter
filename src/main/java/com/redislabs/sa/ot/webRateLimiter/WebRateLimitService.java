package com.redislabs.sa.ot.webRateLimiter;
import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;
import static spark.Spark.*;

import com.redislabs.sa.ot.util.JedisConnectionFactory;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import spark.Request;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//NB: this class uses SparkJava -a simple embedded web server
//Documentation for SparkJava can be found here:
// https://sparkjava.com/documentation
/**
The purpose of this web-application is to accept badly spelled city names
and submit them as data into the 'system' (a redis database).
Two things make this web-application less likely to suffer abuse:
1) it uses a sortedSet -based rate-limiting algorithm to limit the # of requests by:
 requests/minute/account max==5
 requests/hour/account max==25
2) it uses redis to generate a unique requestID for each request
 (this id expires in 30 seconds whether used or not)
 */

public class WebRateLimitService {

    int ratePerMinuteAllowed = 5;
    int ratePerHourAllowed = 25;

    private static WebRateLimitService instance = new WebRateLimitService();
    public static WebRateLimitService getInstance(){ return instance; }

    private WebRateLimitService() {
        jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
        //The following requires the presence of an active Redis TimeSeries module:
        /*
        Use commands like these to see what is being posted to the timeseries:
        TS.MRANGE - + AGGREGATION avg 2 FILTER measure=heartbeat
        TS.RANGE TS:com.redislabs.sa.ot.webRateLimiter.WebRateLimitService - + AGGREGATION avg 2
         */
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool,WebRateLimitService.class.getCanonicalName());
    }

    private static void restartWebRateLimitService(){
        stop();
        init();
    }


    static{
        init(); //starts the webserver listening on port 4567

        before((request, response) -> {
            //checks request for both an 'accountKey' (which helps us recognize and track our accounts) and
            //uses Redis Sorted Sets to count the # of requests made by that account in the past minute and hour
            boolean isIdentified = false;
            String requestMethod = request.requestMethod();
            String requestIP = request.ip();
            String queryString = request.queryString();
            if(!queryString.contains("uniqueRequestKey")) {
                RateLimitRecord record = instance.buildRateLimitRecord(requestMethod, requestIP, queryString);
                System.out.println("Before() called... Query String: " + queryString);
                if (queryString.contains("accountKey")) {
                    isIdentified = true;
                    storeQueryStringInRedis(queryString);
                    if (instance.isRateOfAccessTooHighForContract(record)) {
                        System.out.println("Too Many requests!");
                        halt(429, "" + (record.getMessage()+getLinks()));
                    }
                }
                if (!isIdentified) {
                    System.out.println("you submitted: " + queryString);
                    halt(401, "You must use an accountKey example:    http://127.0.0.1:4567?accountKey=007");
                }
            }
        });

        get("/", (req, res) -> WebRateLimitService.getResponseForRootPath(req));

        get("/correct-city-spelling", (req, res) -> (getResponseForCorrectCitySpellPath(req))+getLinks());

        get("/cleaned-submissions", (req, res) -> (getResponseForCleanedSubmissions(req))+getLinks());

        get("/delete-cuckoo-and-stream-data", (req, res) -> (getResponseForDeleteKeys(req))+getLinks());

    }

    static void storeQueryStringInRedis(String queryString) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(Runtime.getRuntime().toString()+"queryString",queryString);
        }
    }

    static String getQueryStringFromRedis() {
        String queryString ="";
        try (Jedis jedis = jedisPool.getResource()) {
            queryString=jedis.get(Runtime.getRuntime().toString()+"queryString");
        }
        return queryString;
    }

    // needs to be invoked like this: /delete-cuckoo-and-stream-data?accountKey=11151977
    static String getResponseForDeleteKeys(Request req) {
        String val = "<h1>Cuckoo Filters and Stream Data used by this application have been deleted.</h1>";
        if(req.queryString().contains("11151977")) {
            try (Jedis jedis = jedisPool.getResource()) {
                String[] deleteMe = new String[]{"X:GBG:CITY", "X:BEST_MATCHED_CITY_NAMES_BY_SEARCH",
                        "X:DEDUPED_CITY_NAME_SPELLCHECK_REQUESTS", "CF_BAD_SPELLING_SUBMISSIONS", "CF_BEST_MATCH_SUBMISSIONS",
                        "CF_CITIES_LIST"
                };
                jedis.del(deleteMe);
            } catch (Throwable t) {
                val += "<p><b><h2>" + t.getMessage() + "</h2></b></p>";
            }
        }else{
            val = "<p><b><h2>Hold on a minute!<br>Only Account Holder 11151977 is authorized to use this link!</br></h2></b></p>";
        }
        return val;
    }

    static String getResponseForCorrectCitySpellPath(Request req) {
        String val = "</h1>Thank you for your submission</h1>" +
                "" + instance.submitCity(req.queryParams("uniqueRequestKey"), req.queryParams("city")) + "<p />";
        return val;
    }

    static String getLinks(){
        String queryString =getQueryStringFromRedis();
        return
                "<p /><a href=\"http://localhost:4567?"+queryString+"\">RequestAnother?</a>"+
                "<p /><a href=\"http://localhost:4567/cleaned-submissions?"+queryString+"\">See all Submissions?</a>"+
                "<p /><a href=\"http://localhost:4567/delete-cuckoo-and-stream-data?"+queryString+"\"><em>Reset All Records Of Past Queries?</em></a>"
                ;
    }

    static String getResponseForCleanedSubmissions(Request req){
        String response = "<p /><h3>Here are up to 100 CityNames that have been submitted and cleaned up by this service: <p /> <ol>";
        try (Jedis jedis = jedisPool.getResource()) {
            try {
                StreamEntryID start = new StreamEntryID("0-0");
                StreamEntryID end = new StreamEntryID(Long.MAX_VALUE,0l);
                List<StreamEntry> x = jedis.xrange(BEST_MATCHED_CITY_NAMES_STREAM_NAME, start, end, 100);
                if (null != x) {
                    if(x.size()<1){
                        x = jedis.xrange(GARBAGE_CITY_STREAM_NAME, start, end, 100);
                        response += "<h2><p style=\"color:blue\">No entries have been processed yet</h2></p>"+
                                x.size()+" Entries have been submitted for processing:<p />";
                    }
                    Iterator i = x.iterator();
                    while (i.hasNext()) {
                        response += "<li />";
                        response += ((StreamEntry) i.next()).toString();
                    }
                }
            } catch (Throwable t) {
                response += t.getMessage() + "<li />";
                response += t.getCause().getMessage() + "<li />";
                t.printStackTrace();
            }
        }
        response+="</ol></h3>";
        return response;
    }

        static String getResponseForRootPath(Request req){
        String val ="Ahhh - of course it's you: </p>" +
                "<h3>"+req.ip()+":"+req.requestMethod()+":"+req.queryString()+ "</h3> " +
                "</br> <em>During the past minute you have issued <b>"+
                instance.countRequestsThisMinute(instance.buildRateLimitRecord(req.requestMethod(),req.ip(),req.queryString()))+
                "</b> requests.</em></br> <em>During the past hour you have issued <b>"+
                instance.countRequestsThisHour(instance.buildRateLimitRecord(req.requestMethod(),req.ip(),req.queryString()))+
                "</b> requests.</em></br> <h1>It is good to serve you again!</h1>" +
                "<p />Use this code to submit your request:<br />"+
                "<p />You have 30 seconds before your code expires.<p />" +
                "<form action=\"/correct-city-spelling\" method=\"get\">"+
                "<ul>" +
                "<li><label for=\"accountKey\">DO NOT CHANGE:</label>"+
                "<input type=\"text\" id=\"accountKey\" name=\"accountKey\"" +
                "value=\""+req.queryString()+"\"></li>" +
                "<li><label for=\"uniqueRequestKey\">UniqueRequestKey:</label>"+
                "<input type=\"text\" id=\"uniqueRequestKey\" name=\"uniqueRequestKey\"" +
                "value=\""+instance.getBusinessRequestKey()+"\"></li>" +
                "<li><label for=\"cityName\">CityNameToSpellCheck:</label>"+
                "<input type=\"text\" id=\"city\" name=\"city\"></li>" +
                "<li class=\"button\"><button type=\"submit\">Submit</button></li>"+
                "</ul></form>";
        return val;
    }

    private String dummyLog(String val){
        System.out.println("dummy log called with "+val);
        return ".";
    }

    private RateLimitRecord buildRateLimitRecord(String requestMethod,String requestIP, String accountID){
        String rateLimitKey= "z:rateLimiting:"+requestIP+":"+requestMethod+":"+accountID;
        return new RateLimitRecord(rateLimitKey);
    }

    private boolean isRateOfAccessTooHighForContract(RateLimitRecord record){
        removeOldKeysByTime(record);
        storeCurrentRequest(record);
        updateExpiryTimes(record);
        int rpm = countRequestsThisMinute(record);
        int rph = countRequestsThisHour(record);
        dummyLog("There have been "+rpm+" requests from "+record.getRateLimitKeyBase()+" in the last minute");
        dummyLog("There have been "+rph+" requests from "+record.getRateLimitKeyBase()+" in the last hour");
        boolean isAboveAllowedRateLimit = false;
        if(rpm > this.ratePerMinuteAllowed){
            record.setMessage("<b>You have used all your available requests for this minute!</b></p> You issued this many requests this minute: "+rpm+"</br></p><em>Sing the ABC song and try again</em>");
        }
        if(rph > this.ratePerHourAllowed){
            record.setMessage("<b>You have used all your available requests for this hour!</b></p> You issued this many requests this hour: "+rph+"</br></p><em>This could take a while...</em>" +
                    "</p><a href=\"/upgrade\">UPGRADE YOUR PLAN NOW!</a>");
        }
        if(rpm > this.ratePerMinuteAllowed || rph > this.ratePerHourAllowed){
            isAboveAllowedRateLimit = true;
        }
        return isAboveAllowedRateLimit;
    }

    private String submitCity(String requestKey,String city){
        dummyLog("submitCity() called... args: "+requestKey+" , "+city);

        String response ="<p />Your submission of "+city+" is processing!";
        try (Jedis connection = jedisPool.getResource()) {
            if(connection.exists(requestKey)) {
                connection.del(requestKey); // only one request / key allowed!
                connection.set("gbg:"+city,city);
                Map<String,String> citySubmission= new HashMap<String,String>();
                citySubmission.put("spellCheckMe",city);
                citySubmission.put("requestID",requestKey);
                connection.xadd(GARBAGE_CITY_STREAM_NAME,null,citySubmission);
            }else{
                response = "<p />.<p /><h1>WAIT A MINUTE... <br />" +
                        "YOU HAVE NO VALID REQUEST KEY!<p />" +
                        "<em>You must have been a bit too slow.</em></h1>";
            }
        }
        return response;
    }

    private String getBusinessRequestKey(){
        String uniqueKeyName = "";
        try (Jedis connection = jedisPool.getResource()) {
            instance.dummyLog("getRequestKey() connection ==  "+connection);
            uniqueKeyName= "PM_UID"+System.nanoTime();
            try {
                uniqueKeyName = "PM_UID"+connection.aclGenPass();
            }catch(Throwable t){t.getMessage();}
            connection.set(uniqueKeyName,"APPROVED",new SetParams().ex(30));
            instance.dummyLog("getRequestKey()  "+uniqueKeyName);
        }
        return uniqueKeyName;
    }

    private void updateExpiryTimes(RateLimitRecord val){
        try (Jedis connection = jedisPool.getResource()) {
            connection.expire(val.getMinuteLimitKey(),60);
        }
        try (Jedis connection = jedisPool.getResource()) {
            connection.expire(val.getHourLimitKey(),3600);
        }
    }

    private int countRequestsThisMinute(RateLimitRecord val){
        try (Jedis connection = jedisPool.getResource()) {
            return connection.zcard(val.getMinuteLimitKey()).intValue();
        }
    }

    private int countRequestsThisHour(RateLimitRecord val){
        try (Jedis connection = jedisPool.getResource()) {
            return connection.zcard(val.getHourLimitKey()).intValue();
        }
    }

    private void storeCurrentRequest(RateLimitRecord val){
        //There will always be one new request
        try (Jedis connection = jedisPool.getResource()) {
            connection.zadd(val.getMinuteLimitKey(), val.getCurrentTimeArg(), val.getMinuteLimitKey() + ":" + val.getCurrentTimeFormatted());
        }
        try (Jedis connection = jedisPool.getResource()) {
            connection.zadd(val.getHourLimitKey(), val.getCurrentTimeArg(), val.getHourLimitKey() + ":" + val.getCurrentTimeFormatted());
        }
    }

    private void removeOldKeysByTime(RateLimitRecord val){
        //score == time  time grows constantly so older records have smaller values
        //we want to remove the smallest values up to the time factor ago everytime we are called
        //this creates a rolling window where only those scores with times greater than 1 time factor ago will
        //remain in Redis

        try (Jedis connection = jedisPool.getResource()) {
            long countDeletedInMinuteKey = connection.zremrangeByScore(val.getMinuteLimitKey(), 0, val.getLastMinute());
            dummyLog("\tDeleted " + countDeletedInMinuteKey + " scores from " + val.getMinuteLimitKey());
        }
        try (Jedis connection = jedisPool.getResource()) {
            long countDeletedInHourKey = connection.zremrangeByScore(val.getHourLimitKey(), 0, val.getLastHour());
            dummyLog("\tDeleted " + countDeletedInHourKey + " scores from " + val.getHourLimitKey());
        }
    }

}
