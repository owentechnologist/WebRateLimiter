package com.redislabs.sa.ot.webRateLimiter;
import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;
import static spark.Spark.*;

import com.redislabs.sa.ot.demoservices.Main;
import com.redislabs.sa.ot.demoservices.SharedConstants;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import com.redislabs.sa.ot.util.TopkHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.StreamEntry;
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
    TopkHelper topkHelper = new TopkHelper().
            setJedis(jedisPool).
            setTopKSize(10).
            setTopKKeyNameForMyLog("TOPK:WRL:TOP_TEN_SUBMITTED_CITY_NAMES");

    private static WebRateLimitService instance = new WebRateLimitService();
    public static WebRateLimitService getInstance(){ return instance; }

    private WebRateLimitService() {
        //The following requires the presence of an active Redis TimeSeries module:
        /*
        Use commands like these to see what is being posted to the timeseries:
        TS.MRANGE - + AGGREGATION avg 2 FILTER measure=heartbeat
        TS.RANGE TS:com.redislabs.sa.ot.webRateLimiter.WebRateLimitService - + AGGREGATION avg 2
         */
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool,WebRateLimitService.class.getCanonicalName());
        try{
            topkHelper.initTopK();
        }catch(redis.clients.jedis.exceptions.JedisDataException jde){
            jde.getMessage();
        }
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
                    storeQueryStringInRedis(queryString);//Imagine this as a long-term session opportunity
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

        get("/top10-submissions", (req, res) -> (getResponseForTop10Submissions(req))+getLinks());

        get("/delete-cuckoo-and-stream-data", (req, res) -> (getResponseForDeleteKeys(req))+getLinks());

    }

    static void storeQueryStringInRedis(String queryString) {
        //Keeping a record of this runtime acting as vehicle for this acctKey for 1 hour:
        //This is just a placeholder for future anti-fraud or similar gatekeeping activities
        jedisPool.set(Runtime.getRuntime().toString()+"queryString",queryString,new SetParams().ex(3600));
    }

    static String getQueryStringFromRedis() {
        String queryString ="";
        queryString=jedisPool.get(Runtime.getRuntime().toString()+"queryString");
        return queryString;
    }

    //responds with the top10 submitted city names this service has observed
    //Uses the TopK data structure and TopkHelper class
    static String getResponseForTop10Submissions(Request req){
        String val = "<h1>Here is a list of the top 10 most requested City Names:</h1>";
        val += "<table border=\"1\"><tr><th>CITY NAME REQUESTED</th><th>TIMES REQUESTED</th></tr>";
        Map<String, Long> rMap = instance.topkHelper.getTopKlistWithCount(true);//we print topk to screen for debug
        Iterator i = rMap.keySet().iterator();
        while (i.hasNext()) {
            String iKey = i.next().toString();
            val += "<tr><td>";
            val += iKey;
            val += "</td><td>&nbsp;&nbsp;&nbsp;&nbsp;";
            val += rMap.get(iKey);
            val += "</td></tr>";
        }
        val += "</table>";
        return val;
    }

    // needs to be invoked like this: /delete-cuckoo-and-stream-data?accountKey=11151977
    // only that accountkey has the rights to delete the keys :)
    static String getResponseForDeleteKeys(Request req) {
        String val = "<h1>Cuckoo Filters and Stream Data used by this application have been deleted.</h1>";

        if(req.queryString().contains("11151977")) {
            String[] deleteMe = new String[]{"X:GBG:CITY", "X:BEST_MATCHED_CITY_NAMES_BY_SEARCH",
                    "X:DEDUPED_CITY_NAME_SPELLCHECK_REQUESTS", "CF_BAD_SPELLING_SUBMISSIONS", "CF_BEST_MATCH_SUBMISSIONS",
                    "CF_CITIES_LIST"
            };
            try{
                jedisPool.del(deleteMe);
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
                "<p /><a href=\"http://localhost:4567/top10-submissions?"+queryString+"\">Retrieve the top 10 Submissions?</a>"+
                "<p /><a href=\"http://localhost:4567/delete-cuckoo-and-stream-data?"+queryString+"\"><em>Reset All Records Of Past Queries?</em></a>"
                ;
    }

    static String getResponseForCleanedSubmissions(Request req){
        String response = "<p /><h3>Here are up to 100 CityNames that have been submitted and cleaned up by this service: <p /> <ol>";
        try {
            StreamEntryID start = new StreamEntryID("0-0");
            StreamEntryID end = new StreamEntryID(Long.MAX_VALUE,0l);
            List<StreamEntry> x = jedisPool.xrange(BEST_MATCHED_CITY_NAMES_STREAM_NAME, start, end, 100);
            if (null != x) {
                if(x.size()<1){
                    x = jedisPool.xrange(GARBAGE_CITY_STREAM_NAME, start, end, 100);
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
        topkHelper.addEntryToMyTopKKey(city);
        if(jedisPool.exists(requestKey)) {
            jedisPool.del(requestKey); // only one request / key allowed!
            jedisPool.set("gbg:"+city,city);
            Map<String,String> citySubmission= new HashMap<String,String>();
            citySubmission.put("spellCheckMe",city);
            citySubmission.put("requestID",requestKey);
            jedisPool.xadd(GARBAGE_CITY_STREAM_NAME,StreamEntryID.NEW_ENTRY,citySubmission);
        }else{
            response = "<p />.<p /><h1>WAIT A MINUTE... <br />" +
                    "YOU HAVE NO VALID REQUEST KEY!<p />" +
                    "<em>You must have been a bit too slow.</em></h1>";
        }
        return response;
    }

    private String getBusinessRequestKey(){
        String uniqueKeyName = "";
        instance.dummyLog("getRequestKey() connection ==  "+jedisPool);
        uniqueKeyName= "PM_UID"+System.nanoTime()%1000000;
        jedisPool.set(uniqueKeyName,"APPROVED",new SetParams().ex(30));
        instance.dummyLog("getRequestKey()  "+uniqueKeyName);
        return uniqueKeyName;
    }

    private void updateExpiryTimes(RateLimitRecord val){
        jedisPool.expire(val.getMinuteLimitKey(),60);
        jedisPool.expire(val.getHourLimitKey(),3600);
    }

    private int countRequestsThisMinute(RateLimitRecord val){
        return (int)jedisPool.zcard(val.getMinuteLimitKey());
    }

    private int countRequestsThisHour(RateLimitRecord val){
        return (int)jedisPool.zcard(val.getHourLimitKey());
    }

    private void storeCurrentRequest(RateLimitRecord val){
        //There will always be one new request
        jedisPool.zadd(val.getMinuteLimitKey(), val.getCurrentTimeArg(), val.getMinuteLimitKey() + ":" + val.getCurrentTimeFormatted());
        jedisPool.zadd(val.getHourLimitKey(), val.getCurrentTimeArg(), val.getHourLimitKey() + ":" + val.getCurrentTimeFormatted());
    }

    private void removeOldKeysByTime(RateLimitRecord val){
        //score == time  time grows constantly so older records have smaller values
        //we want to remove the smallest values up to the time factor ago everytime we are called
        //this creates a rolling window where only those scores with times greater than 1 time factor ago will
        //remain in Redis

        long countDeletedInMinuteKey = jedisPool.zremrangeByScore(val.getMinuteLimitKey(), 0, val.getLastMinute());
        dummyLog("\tDeleted " + countDeletedInMinuteKey + " scores from " + val.getMinuteLimitKey());
        long countDeletedInHourKey = jedisPool.zremrangeByScore(val.getHourLimitKey(), 0, val.getLastHour());
        dummyLog("\tDeleted " + countDeletedInHourKey + " scores from " + val.getHourLimitKey());
    }

}
