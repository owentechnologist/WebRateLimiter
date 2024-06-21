package com.redislabs.sa.ot.util;
import redis.clients.jedis.JedisPooled;

/**
     * The following requires the presence of an active Redis TimeSeries module:
    Use commands like these to see what is being posted to the timeseries:
    TS.MRANGE - + AGGREGATION avg 60 FILTER sharedlabel=heartbeat
    TS.MRANGE - + AGGREGATION count 60000 FILTER sharedlabel=heartbeat GROUPBY customlabel reduce avg
    TS.MGET WITHLABELS FILTER sharedlabel=heartbeat
*/
public class TimeSeriesHeartBeatEmitter {

    JedisPooled pool = null;

    public TimeSeriesHeartBeatEmitter(JedisPooled pool, String serviceName){
        this.pool=pool;
        String tsKeyName="TS:WRL:"+serviceName;
        TimeSeriesEventLogger eventLogger = new TimeSeriesEventLogger().
                setTSKeyNameForMyLog(tsKeyName).
                setCustomLabel(serviceName).
                setSharedLabel("heartbeat");
        eventLogger.setJedis(pool).initTS();
        startHeartBeat(eventLogger);
        System.out.println("startHeartBeat(keyName);" +tsKeyName);
    }

    private void startHeartBeat(TimeSeriesEventLogger eventLogger){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(true) {
                        try{
                            //each service checks in periodically -registering a value to indicate it is alive
                            eventLogger.addEventToMyTSKey(1);
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
