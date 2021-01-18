package com.redislabs.sa.ot;
import static spark.Spark.*;

import redis.clients.jedis.*;

import java.util.Set;

//NB: this class uses SparkJava -a simple embedded web server
//Documentation for SparkJava can be found here:
// https://sparkjava.com/documentation

public class WebRateLimitService {
    int ratePerMinuteAllowed = 4;
    int ratePerHourAllowed = 25;
    Jedis connection = null;

    private static WebRateLimitService instance = new WebRateLimitService();

    public static WebRateLimitService getInstance(){
        return instance;
    }

    private static void restartWebRateLimitService(){
        stop();
        init();
    }


    static{
        init(); //starts the webserver listening on port 4567

        before((request, response) -> {
            //checks for both an 'accountKey' (which helps us recognize and track our accounts) and
            // counts the # of requests made by that account in the past minute and hour
            boolean isIdentified = false;
            String requestMethod = request.requestMethod();
            String requestIP = request.ip();
            String queryString = request.queryString();
            RateLimitRecord record = instance.buildRateLimitRecord(requestMethod,requestIP,queryString);
            if(queryString.contains("accountKey")){
                isIdentified=true;
                if(instance.isRateOfAccessTooHighForContract(record)) {
                    System.out.println("Too Many requests!");
                    halt(429, ""+record.getMessage());
                }
            }
            if (!isIdentified) {
                halt(401, "You must use an accountKey example:    http://127.0.0.1:4567?accountKey=007");
            }
        });

        get("/", (req, res) -> "Ahhh - of course it's you: </p>" +
                "<h3>"+req.ip()+":"+req.requestMethod()+":"+req.queryString()+ "</h3> " +
                "</br> <em>During the past minute you have issued <b>"+
                instance.countRequestsThisMinute(instance.buildRateLimitRecord(req.requestMethod(),req.ip(),req.queryString()))+
                "</b> requests.</em></br> <em>During the past hour you have issued <b>"+
                instance.countRequestsThisHour(instance.buildRateLimitRecord(req.requestMethod(),req.ip(),req.queryString()))+
                "</b> requests.</em></br> <h1>It is good to serve you again!</h1>");
    }

    private RateLimitRecord buildRateLimitRecord(String requestMethod,String requestIP, String secretKey){
        String rateLimitKey= "z:rateLimiting:"+requestIP+":"+requestMethod+":"+secretKey;
        return new RateLimitRecord(rateLimitKey);
    }

    private boolean isRateOfAccessTooHighForContract(RateLimitRecord record){
        removeOldKeysByTime(record);
        storeCurrentRequest(record);
        updateExpiryTimes(record);
        int rpm = countRequestsThisMinute(record);
        int rph = countRequestsThisHour(record);
        System.out.println("There have been "+rpm+" requests from "+record.getRateLimitKeyBase()+" in the last minute");
        System.out.println("There have been "+rph+" requests from "+record.getRateLimitKeyBase()+" in the last hour");
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

    private void updateExpiryTimes(RateLimitRecord val){
        getConnection().expire(val.getMinuteLimitKey(),60);
        getConnection().expire(val.getHourLimitKey(),3600);
    }

    private int countRequestsThisMinute(RateLimitRecord val){
        Set<String> minuteSet =  getConnection().zrange(val.getMinuteLimitKey(), 0,-1);
        return minuteSet.size();
    }

    private int countRequestsThisHour(RateLimitRecord val){
        Set<String> hourSet = getConnection().zrange(val.getHourLimitKey(), 0,-1);
        return hourSet.size();
    }

    private void storeCurrentRequest(RateLimitRecord val){
        //There will always be one new request
        getConnection().zadd(val.getMinuteLimitKey(), val.getCurrentTimeArg(), val.getMinuteLimitKey()+":"+val.getCurrentTimeFormatted());
        getConnection().zadd(val.getHourLimitKey(), val.getCurrentTimeArg(), val.getHourLimitKey()+":"+val.getCurrentTimeFormatted());
    }

    private void removeOldKeysByTime(RateLimitRecord val){
        //score == time  time grows constantly so older records have smaller values
        //we want to remove the smallest values up to the time factor ago everytime we are called
        //this creates a rolling window where only those scores with times greater than 1 time factor ago will
        //remain in Redis

        long countDeletedInMinuteKey = getConnection().zremrangeByScore(val.getMinuteLimitKey(),0,val.getLastMinute());
        long countDeletedInHourKey = getConnection().zremrangeByScore(val.getHourLimitKey(),0,val.getLastHour());
        System.out.println("\tDeleted "+countDeletedInMinuteKey+" scores from "+val.getMinuteLimitKey());
        System.out.println("\tDeleted "+countDeletedInHourKey+" scores from "+val.getHourLimitKey());
    }

    private Jedis getConnection(){
        if(null == connection) {
            connection = JedisConnectionFactory.getInstance().getJedisConnectionFromPool();
            if(connection.ping().length()<2){
                restartWebRateLimitService(); // this is designed to allow connections again after
                // a 500 InternalServer Error
            }
        }
        return this.connection;
    }

}
