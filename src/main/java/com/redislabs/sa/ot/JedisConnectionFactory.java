package com.redislabs.sa.ot;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Properties;

public class JedisConnectionFactory {
    public static final String hostPropertyName = "REDIS_HOST";
    public static final String portPropertyName = "REDIS_PORT";
    public static final String passwordPropertyName = "REDIS_PASSWORD";
    public static final String timeoutPropertyName = "REDIS_TIME_OUT";
    private static JedisPool jedisPool;
    private static Properties config = null;
    private static JedisConnectionFactory instance = null;
    private HostAndPort hostAndPort = null;

    public JedisConnectionFactory() {
        String host = "localhost";
        String port = "6379";
        config = com.redislabs.sa.ot.PropertyFileFetcher.loadProps("jedisconnectionfactory.properties");
        if(null == config){
            config = new Properties();
            config.put(hostPropertyName,host);
            config.put(portPropertyName,port);
            config.put(passwordPropertyName,"");
            config.put(timeoutPropertyName,"15000");
        }
        host = (String) config.get(hostPropertyName);
        port = (String) config.get(portPropertyName);
        hostAndPort = new HostAndPort(host,Integer.parseInt(port) );
        initPool(hostAndPort);
    }

    private void initPool(HostAndPort hostAndPort){
        jedisPool = new JedisPool(hostAndPort.getHost(), hostAndPort.getPort());
    }

    public Jedis getJedisConnectionFromPool(){
        System.out.println(this.getClass()+"  getJedisConnectionFromPool() called");
        Jedis connection = null;
        try {
            connection = jedisPool.getResource();
            assignJedisPassword(connection);
            assignJedisTimeout(connection);
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return connection;
    }

    private void assignJedisTimeout(Jedis connection){
        try {
            if (config.get(timeoutPropertyName).toString().length() > 0) {
                connection.configSet("timeout", config.getProperty(timeoutPropertyName));
            }
        }catch(Throwable t){
            System.out.println("Issue with setting timeout - does the property "+timeoutPropertyName+" exist?");
            t.printStackTrace();
        }
    }

    private void assignJedisPassword(Jedis connection){
        try{
            if(config.get(passwordPropertyName).toString().length()>0){
                connection.auth(config.getProperty(passwordPropertyName));
            }
        }catch(Throwable t){
            System.out.println("Issue with setting password - does the property "+passwordPropertyName+" exist?");
            t.printStackTrace();
        }
    }

    public static JedisConnectionFactory getInstance() {
        if (instance == null) {
            instance = new JedisConnectionFactory();
        }
        return instance;
    }

}

