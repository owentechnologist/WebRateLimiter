package com.redislabs.sa.ot.util;

import com.redislabs.redistimeseries.RedisTimeSeries;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
     * The following requires the presence of an active Redis TimeSeries module:
    Use commands like these to see what is being posted to the timeseries:
    TS.MRANGE - + AGGREGATION avg 60 FILTER measure=heartbeat
    TS.MRANGE - + AGGREGATION count 60000 FILTER measure=heartbeat GROUPBY servicename reduce avg
    TS.MGET WITHLABELS FILTER measure=heartbeat
*/
public class TimeSeriesHeartBeatEmitter {

    JedisPool pool = null;

    public TimeSeriesHeartBeatEmitter(JedisPool pool, String serviceName){
        this.pool=pool;
        RedisTimeSeries timeSeries = new RedisTimeSeries(pool);
        Map<String, String> labels = new HashMap<>();
        labels.put("measure","heartbeat");
        labels.put("servicename",serviceName);
        String keyName = "TS:WRL:"+serviceName;
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
                try {
                    while(true) {
                        try{
                            //each service checks in periodically -registering a value to indicate it is alive
                            timeSeries.add(keyName, 1);
                        }catch(Throwable t){
                            System.out.println("Timestamp: "+System.currentTimeMillis());
                            t.printStackTrace();
                        }
                        Thread.sleep(10000);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }}).start();
    }
}
