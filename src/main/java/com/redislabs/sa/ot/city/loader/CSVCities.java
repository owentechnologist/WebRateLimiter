package com.redislabs.sa.ot.city.loader;

import com.redislabs.sa.ot.util.PropertyFileFetcher;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;

public class CSVCities {

    private static CSVCities instance = null;
    public static int TORONTO_INDEX = 0;
    public static int VANCOUVER_INDEX = 0;
    public static int MONTREAL_INDEX = 0;

    String cityDataFileName = PropertyFileFetcher.loadProps("dataload.properties").getProperty("city.data.filename");

    private ArrayList<City> cities = new ArrayList<City>();

    public void loadCities(){
        Reader in = null;
        try {
            System.out.println("LOADING City Data FILE: "+cityDataFileName+" USING CLASSLOADER...");
            InputStream inputStream = PropertyFileFetcher.class.getClassLoader().getResourceAsStream(cityDataFileName);
            System.out.println("inputStream is now: "+inputStream);
            in = new BufferedReader(new InputStreamReader(inputStream));
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                City city = new City();
                city.setCityName(record.get(1));
                city.setProvinceID(record.get(2));
                city.setProvinceName(record.get(3));
                city.setLat(record.get(4));
                city.setLng(record.get(5));
                city.setPostalCodes(record.get(10).split(" "));
                city.setId(record.get(11));
                if(!city.getCityName().equalsIgnoreCase("trenton")){
                    cities.add(city);
                }
                if(city.cityName.equalsIgnoreCase("Toronto")&&city.getProvinceID().equalsIgnoreCase("ON")){
                    TORONTO_INDEX= cities.size()-1;
                    System.out.println("TORONTO: "+cities.get(TORONTO_INDEX).cityName);
                }
                if(city.cityName.equalsIgnoreCase("Vancouver")&&city.getProvinceID().equalsIgnoreCase("BC")){
                    VANCOUVER_INDEX= cities.size()-1;
                    System.out.println("VANCOUVER: "+cities.get(VANCOUVER_INDEX).cityName);
                }
                if(city.cityName.equalsIgnoreCase("Montreal")&&city.getProvinceID().equalsIgnoreCase("QC")){
                    MONTREAL_INDEX= cities.size()-1;
                    System.out.println("MONTREAL: "+cities.get(MONTREAL_INDEX).cityName);
                }
            }
        }catch(Throwable t ){
            t.printStackTrace();
        }
    }

    //"city","city_ascii","province_id","province_name","lat","lng",
    // "population","density","timezone","ranking","postal","id"

    public ArrayList<City> getCities(){
        if(cities.size()<1){
            loadCities();
            System.out.println("cities.size() == "+cities.size());
        }
        return cities;
    }

    public CSVCities() {
    }

    public static CSVCities getInstance() {
        if (instance == null) {
            instance = new CSVCities();
        }
        return instance;
    }


}
