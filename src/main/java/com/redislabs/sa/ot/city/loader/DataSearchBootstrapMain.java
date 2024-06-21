package com.redislabs.sa.ot.city.loader;

import com.redislabs.sa.ot.demoservices.SharedConstants;
import com.redislabs.sa.ot.util.TimeSeriesHeartBeatEmitter;
import redis.clients.jedis.search.IndexDefinition;
import redis.clients.jedis.search.IndexOptions;
import redis.clients.jedis.search.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.redislabs.sa.ot.demoservices.Main.jedisPool;
import static com.redislabs.sa.ot.demoservices.SharedConstants.*;

/**
 * Prepares the database with the hashes of city information and creates the search index
 * mvn exec:java -Dexec.mainClass="com.redislabs.sa.ot.city.citysearch.BootstrapMain"
 */

public class DataSearchBootstrapMain {

    static boolean isNoDupeCallFailure = false;

    public static void main(String[] args){
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool,DataSearchBootstrapMain.class.getCanonicalName());
        // cleanupIDX();
        // cleanupCF();
        createCF();
        loadCitiesAsHashes();
        createCitySearchIndex();
        System.out.println("index created");
    }

    static void cleanupCF(){
        try {
            jedisPool.del(cfCitiesList);
        }catch(Throwable t){t.printStackTrace();}
    }

    static void createCF(){
        try {
            jedisPool.cfReserve(cfCitiesList, 100000);
        }catch(Throwable t){
            t.printStackTrace();
        }
    }

    static void cleanupIDX(){
        try{
            jedisPool.ftDropIndex(citySearchIndex);
        }catch(Throwable t){t.printStackTrace();}
    }

    static boolean noDupe(String cityRecord){
        boolean isNoDupe = true;
        if(! isNoDupeCallFailure) {
            try {
                if (jedisPool.cfExists(cfCitiesList, cityRecord)) {
                    isNoDupe = false;
                }
            } catch (Throwable t) {
                t.printStackTrace();
                isNoDupeCallFailure = true; // prevent future calls to this API as it failed
            }
        }
        return isNoDupe;
    }

    static void loadCitiesAsHashes(){
        long indexSize=0;
        try {
            indexSize = jedisPool.ftSearch(citySearchIndex, "*").getTotalResults();
        }catch(Throwable t){t.getMessage();}
        if(indexSize<1739){
            System.out.println("\n*****\nLoading cities from CSV file...\n*****\n");
            CSVCities cHelper = CSVCities.getInstance();
            ArrayList<City> cities = cHelper.getCities();
            for (City c : cities) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("city", c.getCityName());
                map.put("name_length", c.getCityName().length() + "");
                map.put("state_or_province", c.getProvinceID());
                map.put("geopoint", "" + c.getLng() + "," + c.getLat());
                String zipPostalCodes = "";
                for (String cd : c.getPostalCodes()) {
                    zipPostalCodes += cd + " , ";
                }
                map.put("zip_codes_or_postal_codes", zipPostalCodes);
                if (noDupe("city_" + c.getId())) {
                    jedisPool.hmset("city_" + c.getId(), map);
                }
            }
        }
        System.out.println("********** \nCities loaded\n**********");
    }

    static void createCitySearchIndex(){
        Schema sc = new Schema()
                .addField(new Schema.TextField("city", 5.0, false, false, false, "dm:en"))
                .addTextField("state_or_province", 1.0)
                .addTextField("zip_codes_or_postal_codes",10.0)
                .addGeoField("geopoint")
                .addSortableNumericField("name_length");
        IndexDefinition def = new IndexDefinition()
                .setPrefixes(new String[] {"city_"});
        try {
            jedisPool.ftCreate(citySearchIndex, IndexOptions.defaultOptions().setDefinition(def), sc);
        }catch(redis.clients.jedis.exceptions.JedisDataException jde){
            System.out.println(jde.getMessage());
        }
    }
}


