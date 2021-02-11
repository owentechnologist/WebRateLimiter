package com.redislabs.sa.ot.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileFetcher {

    private static String pathToPropertiesFile;

    public static Properties loadProps(String propertyFileName){
        boolean foundPathToPropertyFile = false;
        Properties p=null;
        InputStream inputStream = null;
        try{
            System.out.println("LOADING PROPERTIES FILE: "+propertyFileName+" USING FileInputStream and this path: "+pathToPropertiesFile+System.getProperty("file.separator")+propertyFileName);
            inputStream = new FileInputStream(pathToPropertiesFile+System.getProperty("file.separator")+propertyFileName);
            foundPathToPropertyFile = true;
        }catch(FileNotFoundException t){
            t.printStackTrace();
        }
        if(!foundPathToPropertyFile){
            System.out.println("LOADING PROPERTIES FILE: "+propertyFileName+" USING CLASSLOADER...");
            inputStream = PropertyFileFetcher.class.getClassLoader().getResourceAsStream(propertyFileName);
            System.out.println("inputStream is now: "+inputStream);
        }
        if(null != inputStream) {
            p = new Properties();
            try {
                p.load(inputStream);
                if(foundPathToPropertyFile){
                    System.out.println("\t:) LOADED PROPERTIES FILE USING FileinputStream and provided path arg: "+pathToPropertiesFile+
                            "\nEXAMPLE PROPERTY RETRIEVED: REDIS_HOST = " + p.getProperty("REDIS_HOST"));
                }else {
                    System.out.println("! --> CLASSLOADER LOAD OF PROPERTIES FILES...EXAMPLE PROP RETRIEVED: REDIS_HOST = " + p.getProperty("REDIS_HOST"));
                }
            } catch (IOException e) {
                if(!foundPathToPropertyFile) {
                    System.out.println("! --> CLASSLOADER LOAD OF PROPERTIES FILES...FAILED");
                }
                e.printStackTrace();
            }
        }
        return p;
    }

    public static void setPropertiesFilePath(String path){
        pathToPropertiesFile = path;
    }
}
