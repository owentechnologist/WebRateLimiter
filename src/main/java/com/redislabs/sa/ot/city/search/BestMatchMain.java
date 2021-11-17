package com.redislabs.sa.ot.city.search;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntry;

import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;

public class BestMatchMain {

    static io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jedisPool);
    static CityNameLookupMatcher cityNameLookupMatcher = new CityNameLookupMatcher();
    static RedisStreamAdapter redisStreamAdapter = new RedisStreamAdapter(dedupedCityNameRequests,jedisPool);

    public static void main(String [] args){
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool, BestMatchMain.class.getCanonicalName());
        System.out.println("Starting city matcher...");
        //cityNameLookupMatcher.cleanupCF();
        cityNameLookupMatcher.createCF();
        redisStreamAdapter.listenToStream(cityNameLookupMatcher);
    }
}

class CityNameLookupMatcher implements StreamEventMapProcessor {

    public CityNameLookupMatcher(){
    }

    static void cleanupCF(){
        try (Jedis jedis = jedisPool.getResource()){
            jedis.del(cfIndexName);
        }catch(Throwable t){t.printStackTrace();}
    }

    static void createCF(){
        try(Jedis jedis = jedisPool.getResource()) {
            if (!jedis.exists(cfIndexName)) {
                BestMatchMain.cfClient.cfCreate(cfIndexName, 100000);
            }
        }
    }

    boolean isDuplicate(String value){
        System.out.println("Deduping? "+value);
        return BestMatchMain.cfClient.cfExists(cfIndexName,value);
    }

    public String findBestMatch(String cityName){
        System.out.println("CityNameLookupMatcher.processMap().findBestMatch("+cityName+")"+ jedisPool );
        boolean shouldAdd = true;
        String bestMatch="No Match Found";
        try {
            io.redisearch.client.Client searchClient = new Client(citySearchIndex, jedisPool);
            Query query = new Query(cityName).setSortBy("name_length",true).returnFields("city").limit(0,1);
            SearchResult result = searchClient.search(query);
            System.out.println("findBestMatch(cityName) "+result.totalResults);
            if(result.totalResults>0) {
                System.out.println(result.docs.get(0));
                System.out.println("result.docs " + result.docs.toArray()[0]);
                Map<String, String> map = mapifyJSONRecord(result.docs.toArray()[0].toString());
                Object o = map.get("properties");
                System.out.println("o.getClass() " + o.getClass());
                LinkedTreeMap<String, String> linkedTreeMap = (LinkedTreeMap<String, String>) o;
                System.out.println("linkedTreeMap.get(city)  " + linkedTreeMap.get("city"));
                bestMatch = linkedTreeMap.get("city");
            }
        }catch(Throwable t){t.printStackTrace();}
        return bestMatch;
    }

    static Map<String,String> mapifyJSONRecord(String json){
        Gson gson = new Gson();
        Map<String,String> payload = gson.fromJson(json,Map.class);
        return payload;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        StreamEntry content = payload.get(payload.keySet().toArray()[0]);
        String cityName = content.getFields().get("spellCheckMe");
        String requestID = content.getFields().get("requestID");
        try{
            System.out.println("CityNameLookupMatcher.processMap(): "+payload);
            HashMap<String,String> values = new HashMap<String,String>();
            values.put("requestID",requestID);
            values.put("originalCityName",cityName);
            values.put("bestMatch",findBestMatch(cityName));

            String dedupValue = values.get("originalCityName")+":"+values.get("bestMatch");
            try(Jedis jedis = jedisPool.getResource()) {
                if (!isDuplicate(dedupValue)) {
                    jedis.xadd(bestMatchedCityNamesBySearch, null, values);
                    BestMatchMain.cfClient.cfAdd(cfIndexName, dedupValue);
                }
            }
        }catch(Throwable t){t.printStackTrace();}

    }
}


