package com.redislabs.sa.ot.city.dedup;

import com.redislabs.sa.ot.demoservices.SharedConstants;
import com.redislabs.sa.ot.util.JedisPooledGetter;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.city.dedup.DedupMain.jedisPooled;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;

public class DedupMain {

    static JedisPooled jedisPooled = new JedisPooledGetter(STARTUPARGS).getJedisPooled();
    static RedisStreamAdapter redisStreamAdapter = new RedisStreamAdapter(GARBAGE_CITY_STREAM_NAME, jedisPooled);
    static String cfIndexName = "CF_BAD_SPELLING_SUBMISSIONS";
    static CityNameDeduper cityNameDeduper = new CityNameDeduper();

    public static void main(String [] args){
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPooled, DedupMain.class.getCanonicalName());
        System.out.println("Starting deduper...");
        //cityNameDeduper.cleanupCF();  We want to retain the deduping info between executions
        cityNameDeduper.createCF(); // this only has an effect if the filter does not exist
        redisStreamAdapter.listenToStream(cityNameDeduper);
    }
}

class CityNameDeduper implements StreamEventMapProcessor {

    static void cleanupCF(){
        try{
            jedisPooled.del(DedupMain.cfIndexName);
        }catch(Throwable t){t.printStackTrace();}
    }

    static void createCF(){
        if (!jedisPooled.exists(DedupMain.cfIndexName)) {
            jedisPooled.cfReserve(DedupMain.cfIndexName, 100000);
        }
    }

    public  CityNameDeduper(){
    }

    public Map<String,String> valuesForDedup(String payload){
        String payload1 = payload.split(",")[0];
        String cityName = "";
        String requestID = "";
        String payload2 = payload.split(",")[1];
        int indexOfCityStart = 0;
        if(payload1.indexOf("spellCheckMe=")>0){
            //payload1 contains cityName  payload1 ends with the last letter of the cityName
            cityName = payload1.split("spellCheckMe=")[1];
            String tempID = payload2.split("ID=")[1];
            requestID = tempID.substring(0,tempID.lastIndexOf("}"));
        }else{
            //payload1 contains requestID   payload1 ends with the last letter of the requestID
            requestID = payload1.split("requestID=")[1];
            String tempCityName = payload2.split("Me=")[1];
            cityName = tempCityName.substring(0,tempCityName.lastIndexOf("}"));
        }
        System.out.println("Received cityname = "+cityName+"  requestID = "+requestID);

        HashMap<String,String> values = new HashMap<String,String>();
        values.put("requestID",requestID);
        values.put("cityName",cityName);
        return values;
    }

    public boolean shouldAdd(String cityName){
        // there is also the cfADDNX option: (worth looking into as it might avoid an extra call)
        // DedupMain.cfClient.cfAddNx("CF:dedupFilter","entry:"+x+""+y);
        boolean shouldAdd = true;
        try {
            if(jedisPooled.cfExists(DedupMain.cfIndexName,cityName)){
                shouldAdd=false;
            }else{
                jedisPooled.cfAdd(DedupMain.cfIndexName,cityName);
            }
        }catch(Throwable t){
            t.printStackTrace();
            shouldAdd=false;
        }
        System.out.println("CityNameDeduper.processMap().shouldAdd("+cityName+")"+ jedisPooled + "  --> "+shouldAdd);
        return shouldAdd;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        StreamEntry content = payload.get(payload.keySet().toArray()[0]);
        System.out.println(content);
        Long origTimeStamp = content.getID().getTime();

        String cityName = content.getFields().get("spellCheckMe");
        System.out.println("CityNameDeduper.processMap(): cityName: "+cityName+
                "   OriginalTimeStamp: "+origTimeStamp);
        if(shouldAdd(cityName)) {
            System.out.println("CityNameDeduper.processMap(): " + payload);
            Map<String, String> data = content.getFields();
            data.put("OriginalTimeStamp", "" + origTimeStamp);
            StreamEntryID streamEntryID = StreamEntryID.NEW_ENTRY;
            jedisPooled.xadd(dedupedCityNameRequests,streamEntryID, data);
        }
    }
}
