package com.redislabs.sa.ot.webRateLimiter;
import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;
import static spark.Spark.*;

import com.redislabs.sa.ot.demoservices.Main;
import com.redislabs.sa.ot.demoservices.SharedConstants;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import com.redislabs.sa.ot.util.TopkHelper;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.StreamEntry;
import spark.Request;

import java.util.*;

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

    public int weblistenerPort=4567;
    int ratePerMinuteAllowed = 5;
    int ratePerHourAllowed = 25;
    String specialAccount="11151977";
    TopkHelper topkCityNameHelper = new TopkHelper().
            setJedis(jedisPool).
            setTopKSize(10).
            setTopKKeyNameForMyLog("TOPK:WRL:TOP_TEN_SUBMITTED_CITY_NAMES");
    TopkHelper topkAcctIDHelper = new TopkHelper().
            setJedis(jedisPool).
            setTopKSize(10).
            setTopKKeyNameForMyLog("TOPK:WRL:TOP_TEN_BUSIEST_ACCOUNTS");

    private static WebRateLimitService instance =  new WebRateLimitService();
    public static WebRateLimitService getInstance(){ return instance; }

    private WebRateLimitService() {
        //The following requires the presence of an active Redis TimeSeries module:
        /*
        Use commands like these to see what is being posted to the timeseries:
        TS.MRANGE - + AGGREGATION avg 60 FILTER sharedlabel=heartbeat
        TS.MRANGE - + AGGREGATION count 60000 FILTER sharedlabel=heartbeat GROUPBY customlabel reduce avg
        TS.RANGE TS:com.redislabs.sa.ot.webRateLimiter.WebRateLimitService - + AGGREGATION avg 2
         */
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool,WebRateLimitService.class.getCanonicalName());
        try{
            topkCityNameHelper.initTopK();
            topkAcctIDHelper.initTopK();
        }catch(redis.clients.jedis.exceptions.JedisDataException jde){
            jde.getMessage();
        }
    }

    private static void assignPort(){
        ArrayList<String> arrayListArgs = new ArrayList<String>(Arrays.asList(Main.startupArgs));
        if(arrayListArgs.contains(SharedConstants.WEB_LISTENER_PORT)){
            String listenPort =
              arrayListArgs.get(1+(arrayListArgs.indexOf(SharedConstants.WEB_LISTENER_PORT)));
            getInstance().weblistenerPort=Integer.parseInt(listenPort);
            port(getInstance().weblistenerPort);
        }else{
            port(getInstance().weblistenerPort);//should be default of 4567
        }
    }

    private static void restartWebRateLimitService(){
        stop();
        assignPort();
        init();
    }


    static{
        assignPort(); //default is listening on port 4567
        init(); //starts the webserver
        before((request, response) -> {
            //checks request for both an 'accountKey' (which helps us recognize and track our accounts) and
            //uses Redis Sorted Sets to count the # of requests made by that account in the past minute and hour
            boolean isIdentified = false;
            String requestMethod = request.requestMethod();
            String requestIP = request.ip();
            String queryString = request.queryString();
            RateLimitRecord record = instance.buildRateLimitRecord(requestMethod, requestIP, queryString);
            instance.topkAcctIDHelper.addEntryToMyTopKKey(record.getAccountIDString());
            if(!queryString.contains("uniqueRequestKey")) {
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
                    halt(401, "You must use an accountKey example:    http://127.0.0.1:"+getInstance().weblistenerPort+"?accountKey=007");
                }
            }
        });

        get("/", (req, res) -> WebRateLimitService.getResponseForRootPath(req));

        get("/correct-city-spelling", (req, res) -> (getResponseForCorrectCitySpellPath(req))+getLinks());

        get("/cleaned-submissions", (req, res) -> (getResponseForCleanedSubmissions(req))+getLinks());

        get("/upgrade", (req, res) -> (getResponseForUpgrade(req))+getLinks());

        get("/top10-accounts", (req, res) -> (getResponseForTop10Accounts(req))+getLinks());

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
        String val = "<h1>Here is a list of the top 10 City Names submitted for cleansing:</h1>";
        val += "<table border=\"1\"><tr><th>CITY NAME REQUESTED</th><th>TIMES REQUESTED</th></tr>";
        Map<String, Long> rMap = instance.topkCityNameHelper.getTopKlistWithCount(false);//we don't print topk to terminal for debug
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

    //responds with the top10 busiest AcctIDs this service has observed
    //Uses the TopK data structure and TopkHelper class
    static String getResponseForTop10Accounts(Request req){
        String val = "<h1>Here is a list of the top 10 Busiest Accounts:</h1>";
        val += "<table border=\"1\"><tr><th>ACCOUNT ID USED</th><th>TIMES UTILIZED</th></tr>";
        Map<String, Long> rMap = instance.topkAcctIDHelper.getTopKlistWithCount(false);//we don't print topk to terminal for debug
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

    //invoke like this: http://localhost:4567?upgrade?accountKey=<yourkey>
    static String getResponseForUpgrade(Request req){
        String val = "<h3>Your plan has been upgraded:  <p/><em>From now on - Please use Your NEW AccountID: <h1>11151977</h1></em><p />Your account has been billed for</em>><p/><h1>$100 BILLION DOLLARS</h1></h3>";
        return val;
    }

    // needs to be invoked like this: /delete-cuckoo-and-stream-data?accountKey=11151977
    // only that accountkey has the rights to delete the keys :)
    static String getResponseForDeleteKeys(Request req) {
        String val = "<h1>Cuckoo Filters and Stream Data used by this application have been deleted.</h1>";

        if(req.queryString().contains(instance.specialAccount)) {
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
            val = "<p><b><h2>Hold on a minute!<br>Only Account Holder "+instance.specialAccount+" is authorized to use this link!</br></h2></b></p>";
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
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"?"+queryString+"\">RequestAnother?</a>"+
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"/cleaned-submissions?"+queryString+"\">See all Submissions?</a>"+
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"/top10-accounts?"+queryString+"\">Retrieve the top 10 Busiest Accounts?</a>"+
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"/top10-submissions?"+queryString+"\">Retrieve the top 10 Submissions?</a>"+
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"/delete-cuckoo-and-stream-data?"+queryString+"\">delete-cuckoo-and-stream-data?</a>"+
                "<p /><a href=\"http://localhost:"+getInstance().weblistenerPort+"/upgrade?"+queryString+"\">Upgrade your plan?</a>"
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
        RateLimitRecord rateLimitRecord = instance.buildRateLimitRecord(req.requestMethod(),req.ip(),req.queryString());
        String val ="";
        if(rateLimitRecord.getAccountIDString().equalsIgnoreCase(instance.specialAccount)){
            val+="<h1><b>You are a diamond account and have unlimited requests!</b></p></h1>";
        }
        val+=
                "<h3>"+req.ip()+":"+req.requestMethod()+":"+req.queryString()+ "</h3> " +
                "</br> <em>During the past minute you have issued <b>"+
                instance.countRequestsThisMinute(rateLimitRecord)+
                "</b> requests.</em></br> <em>During the past hour you have issued <b>"+
                instance.countRequestsThisHour(rateLimitRecord)+
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
        String acctID = record.getAccountIDString();
        dummyLog("\trecord.getAccountIDString(): "+record.getAccountIDString());
        if(record.getAccountIDString().equalsIgnoreCase(instance.specialAccount)){
            rpm=0;rph=0;
        }
        if(rpm > this.ratePerMinuteAllowed){
            record.setMessage("<b>You have used all your available requests for this minute!</b></p> You issued this many requests this minute: "+rpm+"</br></p><em>Sing the ABC song and try again</em>");
        }
        if(rph > this.ratePerHourAllowed){
            record.setMessage("<b>You have used all your available requests for this hour!</b></p> You issued this many requests this hour: "+rph+"</br></p><em>This could take a while...</em>" +
                    "</p>UPGRADE YOUR PLAN NOW USING THE LINK BELOW!</a>");
        }
        if(rpm > this.ratePerMinuteAllowed || rph > this.ratePerHourAllowed){
            isAboveAllowedRateLimit = true;
        }
        return isAboveAllowedRateLimit;
    }

    private String submitCity(String requestKey,String city){
        dummyLog("submitCity() called... args: "+requestKey+" , "+city);
        String response ="<p />Your submission of "+city+" is processing!";
        topkCityNameHelper.addEntryToMyTopKKey(city);
        if(jedisPool.exists(requestKey)) {
            jedisPool.del(requestKey); // only one request / key allowed!
            jedisPool.set("gbg:submissions:slidingyear:"+city,city,new SetParams().ex(31536000));
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
        uniqueKeyName= "PM_UID"+System.nanoTime()%25; // deliberately making it easy to occasionally match this
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
