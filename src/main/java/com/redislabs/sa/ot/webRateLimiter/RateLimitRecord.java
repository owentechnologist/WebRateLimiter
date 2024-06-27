package com.redislabs.sa.ot.webRateLimiter;

import java.text.SimpleDateFormat;

public class RateLimitRecord {

    private int lastMinute = (int)(System.currentTimeMillis()-60000);
    private int lastHour = (int)(System.currentTimeMillis()-(60000*60));
    private String minuteLimitKey = null;
    private String hourLimitKey = null;
    private long currentTimeArg = System.currentTimeMillis();
    private String rateLimitKeyBase = null;
    private String uniqueRequestKey = "";
    private String message = "You should never see this message like this!";
    private String acctID = "INVALID"; // acctID determines class / priority

    public RateLimitRecord(String rateLimitKey) {
        minuteLimitKey = rateLimitKey + ":minute";
        hourLimitKey = rateLimitKey + ":hour";
        rateLimitKeyBase = rateLimitKey;
        acctID = rateLimitKey.split("accountKey=")[1];
        acctID = acctID.split("&")[0];
    }
    public String getAccountIDString(){return this.acctID;}
    public String getRateLimitKeyBase(){return this.rateLimitKeyBase;}
    public String getMinuteLimitKey(){return this.minuteLimitKey;}
    public String getHourLimitKey(){return this.hourLimitKey;}
    public double getCurrentTimeArg() {return this.currentTimeArg;}
    public int getLastMinute() {return this.lastMinute;}
    public int getLastHour() { return this.lastHour; }
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
