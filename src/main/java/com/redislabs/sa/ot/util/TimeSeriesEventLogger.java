package com.redislabs.sa.ot.util;

import com.redislabs.sa.ot.demoservices.Main;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.timeseries.TSCreateParams;

import java.util.HashMap;
import java.util.Map;

/*
This implementation offers a 'shared label' by which many keys can be queried together
It also offers a 'custom label' by which the results can be grouped and distinguished
You may wish to add additional custom labels - to express more variance in the samples
 */
public class TimeSeriesEventLogger {

    JedisPooled jedis = null;
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
        if((null==tsKeyName)||(null==jedis)){
            throw new RuntimeException("Must setJedis() and the TimeSeriesEventLogger.setTSKeyNameForMyLog()!");
        }
        Map<String,String> map = new HashMap<>();
        map.put("sharedlabel",sharedLabel);
        if(null!=customLabel) {
            map.put("customlabel", customLabel);
        }
        if(!jedis.exists(tsKeyName)) {
            jedis.tsCreate(tsKeyName, TSCreateParams.createParams().labels(map).retention(86400000l));
        }else{
            System.out.println("\t[debug] initTS() printing last recorded entry from "+tsKeyName+"  --> "+jedis.tsGet(tsKeyName));
        }
        isReadyForEvents=true;
        return this;
    }

    public TimeSeriesEventLogger setJedis(JedisPooled jedis){
        this.jedis=jedis;
        return this;
    }

    public TimeSeriesEventLogger setTSKeyNameForMyLog(String tsKeyName){
        this.tsKeyName = tsKeyName;
        return this;
    }

    public void addEventToMyTSKey(double val){
        if((!isReadyForEvents)&&(!jedis.exists(tsKeyName))){
            initTS();
        }
        try {
            jedis.tsAdd(tsKeyName, val);
        }catch(redis.clients.jedis.exceptions.JedisConnectionException jce){
            jedis = new JedisPooledGetter(Main.startupArgs).getJedisPooled();
        }
    }
}
