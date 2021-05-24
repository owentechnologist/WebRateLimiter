package com.redislabs.sa.ot.webRateLimiter;

import java.text.SimpleDateFormat;

public class RateLimitRecord {

    private long lastMinute = System.currentTimeMillis()-60000;
    private long lastHour = System.currentTimeMillis()-(60000*60);
    private String minuteLimitKey = null;
    private String hourLimitKey = null;
    private long currentTimeArg = System.currentTimeMillis();
    private String rateLimitKeyBase = null;
    private String uniqueRequestKey = "";
    private String message = "You should never see this message like this!";

    public RateLimitRecord(String rateLimitKey) {
        minuteLimitKey = rateLimitKey + ":minute";
        hourLimitKey = rateLimitKey + ":hour";
        rateLimitKeyBase = rateLimitKey;
    }
    public String getRateLimitKeyBase(){return this.rateLimitKeyBase;}
    public String getMinuteLimitKey(){return this.minuteLimitKey;}
    public String getHourLimitKey(){return this.hourLimitKey;}
    public double getCurrentTimeArg() {return this.currentTimeArg;}
    public long getLastMinute() {return this.lastMinute;}
    public long getLastHour() { return this.lastHour; }
    public String getMessage(){return this.message;}
    public void setMessage(String val){this.message = val;}
    public String getCurrentTimeFormatted(){
        String currentTimeFormatted=null;
        try {
            currentTimeFormatted = new SimpleDateFormat("HH-mm-ss:SSS").format(currentTimeArg);
            System.out.println(currentTimeFormatted);
        }catch(Throwable t){
            t.printStackTrace();
            currentTimeFormatted = "bob";
        }
        return currentTimeFormatted;
    }

}
