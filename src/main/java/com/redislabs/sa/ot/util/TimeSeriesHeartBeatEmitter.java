package com.redislabs.sa.ot.util;

import com.redislabs.redistimeseries.RedisTimeSeries;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
     * The following requires the presence of an active Redis TimeSeries module:
    Use commands like these to see what is being posted to the timeseries:
    TS.MRANGE - + AGGREGATION avg 60 FILTER measure=heartbeat
    TS.MGET WITHLABELS FILTER measure=heartbeat
*/
public class TimeSeriesHeartBeatEmitter {

    JedisPool pool = null;

    public TimeSeriesHeartBeatEmitter(JedisPool pool, String serviceName){
        this.pool=pool;
        RedisTimeSeries timeSeries = new RedisTimeSeries(pool);
        Map<String, String> labels = new HashMap<>();
        labels.put("measure","heartbeat");
        String keyName = "TS:"+serviceName;
        try{
            timeSeries.create(keyName, 24 * 2 * 3600, labels);
        }catch(Throwable t){
            if(t.getMessage().equalsIgnoreCase("ERR TSDB: key already exists")){
                timeSeries.alter(keyName, 24 * 2 * 3600, labels);
            }
            else {
                t.printStackTrace();
            }
        }
        startHeartBeat(timeSeries,keyName);
        System.out.println("startHeartBeat(keyName);" +keyName);
    }

    private void startHeartBeat(RedisTimeSeries timeSeries,String keyName){
        new Thread(new Runnable() {
            @Override
            public void run() {
                long secondsWithoutIncident = 0;
                try {
                    while(true) {
                        try{
                            timeSeries.add(keyName, secondsWithoutIncident++);
                        }catch(Throwable t){
                            System.out.println("Timestamp: "+System.currentTimeMillis());
                            secondsWithoutIncident =0;
                            t.printStackTrace();
                        }
                        Thread.sleep(10000);
                        secondsWithoutIncident = secondsWithoutIncident+10;
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }}).start();
    }
}
