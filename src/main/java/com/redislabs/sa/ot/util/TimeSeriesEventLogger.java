package com.redislabs.sa.ot.util;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.timeseries.TSCreateParams;

import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.demoservices.SharedConstants.STARTUPARGS;

/*
This implementation offers a 'shared label' by which many keys can be queried together
It also offers a 'custom label' by which the results can be grouped and distinguished
You may wish to add additional custom labels - to express more variance in the samples
 */
public class TimeSeriesEventLogger {

    JedisPooled jedisPooled = new JedisPooledGetter(STARTUPARGS).getJedisPooled();
    String tsKeyName = null;
    String customLabel = null; //countingTokensUsed,durationInMilliseconds
    String sharedLabel = "groupLabel"; // just a default - feel free to override
    boolean isReadyForEvents = false;

    public TimeSeriesEventLogger setSharedLabel(String label){
        this.sharedLabel = label;
        return this;
    }
    public TimeSeriesEventLogger setCustomLabel(String label){
        this.customLabel = label;
        return this;
    }

    //call this method after assigning a JedisPool object and the keyName to this instance
    public TimeSeriesEventLogger initTS(){
        if((null==tsKeyName)||(null==jedisPooled)){
            throw new RuntimeException("Must setJedis() and the TimeSeriesEventLogger.setTSKeyNameForMyLog()!");
        }
        Map<String,String> map = new HashMap<>();
        map.put("sharedlabel",sharedLabel);
        if(null!=customLabel) {
            map.put("customlabel", customLabel);
        }
        try{
            jedisPooled.ping();
        }catch(Throwable jce){
            jedisPooled = new JedisPooledGetter(STARTUPARGS).getJedisPooled();
        }
        if(!jedisPooled.exists(tsKeyName)) {
            jedisPooled.tsCreate(tsKeyName, TSCreateParams.createParams().labels(map).retention(((86400000l)*30)));//30 days retention
        }else{
            System.out.println("\t[debug] initTS() printing last recorded entry from "+tsKeyName+"  --> "+jedisPooled.tsGet(tsKeyName));
        }
        isReadyForEvents=true;
        return this;
    }

    public TimeSeriesEventLogger setJedis(JedisPooled jedis){
        try{
            if(!(this.jedisPooled.ping().equalsIgnoreCase("pong"))) {
                this.jedisPooled = jedis;
            }
        }catch(JedisConnectionException jce){
            this.jedisPooled = new JedisPooledGetter(STARTUPARGS).getJedisPooled();
        }
        return this;
    }

    public TimeSeriesEventLogger setTSKeyNameForMyLog(String tsKeyName){
        this.tsKeyName = tsKeyName;
        return this;
    }


    public void addEventToMyTSKey(double val){
        if((!isReadyForEvents)&&(!jedisPooled.exists(tsKeyName))){
            initTS();
        }
        try {
            jedisPooled.tsAdd(tsKeyName, val);
        }catch(redis.clients.jedis.exceptions.JedisConnectionException jce){
            // the Thread sleep below is included solely to produce more
            // interesting heartbeat variance across the services
            // there is no functional or system-related need to sleep
            try{ Thread.sleep((System.nanoTime()%10)*(12000));}
            catch(InterruptedException ie){ /*do nothing*/}
            jedisPooled = new JedisPooledGetter(STARTUPARGS).getJedisPooled();
        }
    }
}
