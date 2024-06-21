package com.redislabs.sa.ot.city.search;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.redislabs.sa.ot.demoservices.SharedConstants;
import com.redislabs.sa.ot.util.StreamEventMapProcessor;
import com.redislabs.sa.ot.util.RedisStreamAdapter;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;

public class BestMatchMain {

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
        try {
            jedisPool.del(cfIndexName);
        }catch(Throwable t){t.printStackTrace();}
    }
    static void createCF(){
        if (!jedisPool.exists(cfIndexName)) {
                jedisPool.cfReserve(cfIndexName, 100000);
        }
    }

    boolean isDuplicate(String value){
        System.out.println("Deduping? "+value);
        return jedisPool.cfExists(cfIndexName,value);
    }

    public String findBestMatch(String cityName){
        System.out.println("CityNameLookupMatcher.processMap().findBestMatch("+cityName+")"+ jedisPool );
        boolean shouldAdd = false;
        String bestMatch="No Match Found";
        String score = "";
        try {
            Query query = new Query(cityName).
                    setSortBy("name_length",true).
                    returnFields("city").limit(0,1).
                    setWithScores();
            SearchResult result = jedisPool.ftSearch(SharedConstants.citySearchIndex,query);
            System.out.println("findBestMatch(cityName) "+result.getTotalResults());
            if(result.getTotalResults()>0) {
                System.out.println(result.getDocuments().get(0));
                System.out.println("result.docs " + result.getDocuments().toArray()[0]);
                Map<String, String> map = mapifyJSONRecord(result.getDocuments().toArray()[0].toString());
                Object o = map.get("properties");
                System.out.println("o.getClass() " + o.getClass());
                Object ino = ((ArrayList<Object>)o).get(0);
                System.out.println("ino.getClass() " + ino.getClass());
                bestMatch = ((String)ino).split("=")[1];
                //LinkedTreeMap<String, String> linkedTreeMap = (LinkedTreeMap<String, String>) o;
                //System.out.println("linkedTreeMap.get(city)  " + linkedTreeMap.get("city"));
                //bestMatch = linkedTreeMap.get("city");
                score = ((Object)map.get("score")).toString(); //Returns a double for the score
                System.out.println("Score for best Match is: "+score);
                if(Float.parseFloat(score)>1) {
                    shouldAdd = true;
                }
            }
        }catch(Throwable t){t.printStackTrace();}
        if(shouldAdd){
            addBestMatchToHistory(cityName,bestMatch,score);
        }
        return bestMatch;
    }

    // creates a hash where the Hash key name equals
    // history:<the bestMatchFound> (a city name)
    // example... history:Toronto
    // containing the cityNameProvided and the score search gave the result
    // this allows observers to see what search results come from
    // historical inputs
    // note that due to the deduping logic using Cuckoo filters we should never
    // see the same cityNameProvided show up more than one time
    private void addBestMatchToHistory(String cityNameProvided,String bestMatchFound,String score) {
        Map<String,String> values = new HashMap<String,String>();
        values.put(cityNameProvided,score);
        jedisPool.hset("history:"+bestMatchFound,values);
    }

    static String jsonifySearchResultRecord(String searchResultRecord){
        //id:city_1840034030, score: 5.0, properties:[city=Brooklyn]
        //{"id""city_1840034030": " score"" 5.0"" properties"""[city=Brooklyn]}
        String result ="{";
        String[] contents = searchResultRecord.split(",");
        for(int x=0;x<contents.length;x++) {
            String[] nested = contents[x].split(":");
            for(int y=0;y<nested.length;y++) {
                String currentToken = nested[y];
                System.out.println("debug: currentToken == "+currentToken);
                String str = "";
                if(nested[y].charAt(0)=='['){
                    //we have an array add the contents of the array as a single string
                    //insert " after the [ and before the ]
                    currentToken = nested[y];
                    System.out.println("debug: currentToken [] == "+currentToken);
                    str = new StringBuilder(currentToken).insert(currentToken.length()-1, "\"").toString();
                    str = new StringBuilder(str).insert(1,"\"").toString();
                }else{
                    //we have a normal key:value pair member like 'score'
                    //wrap it in " front and back:
                    currentToken = nested[y];
                    str = "\""+currentToken.trim()+"\"";
                    System.out.println("debug: currentToken == "+str);
                }
                result += str;
                if(result.charAt(result.length()-1)==']'){
                    //do nothing: we are at the end of the result
                }else {
                    if (y % 2 == 0) {
                        //We are processing the first of two tokens
                        result += ": ";
                    }else{
                        result += ", ";
                    }
                }
            }
        }
        result+="}";
        System.out.println("attempt to jsonify..."+result);
        return result;
    }

    static Map<String,String> mapifyJSONRecord(String jsonCandidate){
        System.out.println("\nmapifyJSONRecord input looks like this: \n"+jsonCandidate+"\n");
        jsonCandidate = jsonifySearchResultRecord(jsonCandidate);
        Gson gson = new Gson();
        Map<String,String> payload = gson.fromJson(jsonCandidate,Map.class);
        return payload;
    }

    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {
        StreamEntry content = payload.get(payload.keySet().toArray()[0]);
        String cityName = content.getFields().get("spellCheckMe");
        String requestID = content.getFields().get("requestID");
        String origTimeStamp = content.getFields().get("OriginalTimeStamp");
        try{
            System.out.println("CityNameLookupMatcher.processMap(): "+payload);
            HashMap<String,String> values = new HashMap<String,String>();
            values.put("requestID",requestID);
            values.put("originalCityName",cityName);
            values.put("bestMatch",findBestMatch(cityName));
            values.put("OriginalTimeStamp",origTimeStamp);

            String dedupValue = values.get("originalCityName")+":"+values.get("bestMatch");
            if (!isDuplicate(dedupValue)) {
                jedisPool.xadd(bestMatchedCityNamesBySearch, StreamEntryID.NEW_ENTRY, values);
                jedisPool.cfAdd(cfIndexName, dedupValue);
            }
        }catch(Throwable t){t.printStackTrace();}

    }
}


