package com.redislabs.sa.ot.demoservices;

public class SharedConstants {

    public static final String GARBAGE_CITY_STREAM_NAME="X:GBG:CITY";
    public static final String dedupedCityNameRequests = "X:DEDUPED_CITY_NAME_SPELLCHECK_REQUESTS";
    public static final String BEST_MATCHED_CITY_NAMES_STREAM_NAME="X:BEST_MATCHED_CITY_NAMES_BY_SEARCH";


    public static final String bestMatchedCityNamesBySearch = "X:BEST_MATCHED_CITY_NAMES_BY_SEARCH";
    public static final String citySearchIndex = "IDX_cities";
    public static final String cfIndexName = "CF_BEST_MATCH_SUBMISSIONS";
    public static final String cfCitiesList = "CF_CITIES_LIST";
    public static final String WEB_LISTENER_PORT = "weblistenerport";
    public static final String WEB_LISTENER_HOST_VALUE = "localhost";
    public static String[] STARTUPARGS;


}
