package com.redislabs.sa.ot.util;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;

import java.util.ArrayList;
import java.util.Arrays;

public class JedisPooledGetter {
    private JedisPooled jedis=null;

    public JedisPooledGetter(String[]args){
        long startTime = System.currentTimeMillis();
        DefaultJedisClientConfig jedisConfig = null;
        String password = "";
        String userName = "default";
        boolean usePassword=false;
        String host = "localhost";
        int port = 7001;
        HostAndPort hnp = null;

        ArrayList argsList = new ArrayList(Arrays.asList(args));

        if (argsList.contains("-s")) {
            password = (String) argsList.get(argsList.indexOf("-s") + 1);
            usePassword = true;
        }
        if (argsList.contains("-u")) {
            userName = (String) argsList.get(argsList.indexOf("-u") + 1);
        }
        if (argsList.contains("-h")) {
            host = (String) argsList.get(argsList.indexOf("-h") + 1);
            if (argsList.contains("-p")) {
                port = Integer.parseInt((String) argsList.get(argsList.indexOf("-p") + 1));
                hnp = new HostAndPort(host, port);
            } else {
                System.out.println("Sorry - I won't continue without -h <host> and -p <port> for redis...");
                System.exit(0);
            }
        }
        if (usePassword) {
            jedisConfig = DefaultJedisClientConfig.builder().password(password).user(userName).build();
        } else {
            jedisConfig = DefaultJedisClientConfig.builder().build();
        }

        this.jedis = new JedisPooled(hnp, jedisConfig);
        System.out.println("\n\t\tTIME in MILLISECONDS TAKEN TO CREATE REDIS CONNECTION - "+(System.currentTimeMillis()-startTime));
    }

    public JedisPooled getJedisPooled(){
        return this.jedis;
    }
}
