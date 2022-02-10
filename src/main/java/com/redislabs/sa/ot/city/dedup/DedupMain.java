package com.redislabs.sa.ot.city.dedup;

import com.redislabs.sa.ot.util.RedisStreamAdapter;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import io.rebloom.client.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;

import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;

public class DedupMain {

    static RedisStreamAdapter redisStreamAdapter = new RedisStreamAdapter(GARBAGE_CITY_STREAM_NAME,jedisPool);
    static Client cfClient = new Client(jedisPool);
    static String cfIndexName = "CF_BAD_SPELLING_SUBMISSIONS";
    static CityNameDeduper cityNameDeduper = new CityNameDeduper();

    public static void main(String [] args){
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool, DedupMain.class.getCanonicalName());
        System.out.println("Starting deduper...");
        cityNameDeduper.cleanupCF();
        cityNameDeduper.createCF();
        redisStreamAdapter.listenToStream(cityNameDeduper);
    }
}

class CityNameDeduper implements StreamEventMapProcessor {

    static void cleanupCF(){
        try (Jedis connection = jedisPool.getResource()){
            connection.del(DedupMain.cfIndexName);
        }catch(Throwable t){t.printStackTrace();}
    }

    static void createCF(){
        try (Jedis connection = jedisPool.getResource()) {
            if (!connection.exists(DedupMain.cfIndexName)) {
                DedupMain.cfClient.cfCreate(DedupMain.cfIndexName, 100000);
            }
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
        boolean shouldAdd = true;
        try {
            if(DedupMain.cfClient.cfExists(DedupMain.cfIndexName,cityName)){
                shouldAdd=false;
            }else{
                DedupMain.cfClient.cfAdd(DedupMain.cfIndexName,cityName);
            }
        }catch(Throwable t){
            t.printStackTrace();
            shouldAdd=false;
        }
        System.out.println("CityNameDeduper.processMap().shouldAdd("+cityName+")"+ jedisPool + "  --> "+shouldAdd);
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
        if(shouldAdd(cityName))
            try(Jedis connection = jedisPool.getResource()){
                System.out.println("CityNameDeduper.processMap(): "+payload);
                Map<String, String> data = content.getFields();
                data.put("OriginalTimeStamp",""+origTimeStamp);
                connection.xadd(dedupedCityNameRequests,null,data);
            }catch(Throwable t){t.printStackTrace();}
    }
}
