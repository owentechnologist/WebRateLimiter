package com.redislabs.sa.ot;

public class Main {

    public static void main(String[] args) {
        initPropertyFetcher(args);
        WebRateLimitService service = WebRateLimitService.getInstance();
        System.out.println("Kicked Off Webserver listening on port 4567");
        System.out.println("To test your rate limiting ... use http://[host]:4567?accountKey=[yourKey]");
        System.out.println("Example ...  http://127.0.0.1:4567?accountKey=007");
    }

    public static void initPropertyFetcher(String[] args){
        String pathToPropertiesFiles = "."; //current working directory
        if (args.length>0){
            pathToPropertiesFiles=args[0];
        }
        PropertyFileFetcher.setPropertiesFilePath(pathToPropertiesFiles);
    }

}