package com.redislabs.sa.ot.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

public class JedisConnectionFactory {
    public static final String hostPropertyName = "REDIS_HOST";
    public static final String portPropertyName = "REDIS_PORT";
    public static final String userPropertyName = "REDIS_USER";
    public static final String passwordPropertyName = "REDIS_PASSWORD";
    public static final String timeoutPropertyName = "REDIS_TIME_OUT";
    public static final String maxIdlePropertyName = "REDIS_MAX_IDLE";
    public static final String minIdlePropertyName = "REDIS_MIN_IDLE";
    public static final String maxTotalPropertyName = "REDIS_MAX_TOTAL";
    public static final String maxWaitPropertyName = "REDIS_MAX_WAIT";

    private static JedisPool jedisPool;
    private static Properties config = null;
    private static JedisConnectionFactory instance = null;
    private HostAndPort hostAndPort;

    public JedisConnectionFactory() {
        config = PropertyFileFetcher.loadProps("jedisconnectionfactory.properties");
        if(null == config){
            System.out.println("\t[JedisConnectionFactory] USING HARD-CODED CONFIGURATION ... properties files not loaded...");
            config = new Properties();
            config.put(hostPropertyName,"localhost");
            config.put(portPropertyName,"6379");
            config.put(userPropertyName,"default");
            config.put(passwordPropertyName,"");
            config.put(timeoutPropertyName,"15000");
            config.put(maxIdlePropertyName,"8");
            config.put(minIdlePropertyName,"2");
            config.put(maxTotalPropertyName,"8");
            config.put(maxWaitPropertyName,"5000");
        }
        String host = (String) config.get(hostPropertyName);
        String port = (String) config.get(portPropertyName);
        hostAndPort = new HostAndPort(host,Integer.parseInt(port) );
        initPool(hostAndPort);
    }

    private JedisPoolConfig initPoolConfig(){
        JedisPoolConfig poolConf = new JedisPoolConfig();
        poolConf.setMaxTotal(Integer.parseInt(config.getProperty(maxTotalPropertyName)));
        /* maximum active connections
            Redis Enterprise can handle significantly more connections so make this number high
            If using threads 2-3x the thread count is probably a safe rule of thumb
            be sure to return connections to the pool
        */
        poolConf.setMaxIdle(Integer.parseInt(config.getProperty(maxIdlePropertyName)));
        /* The maximum number of connections that should be kept in the idle pool if isPoolSweeperEnabled() returns false.
           Connections start getting closed here if idle if you have long running idle connections consider matching setMaxTotal
        */
        poolConf.setMinIdle(Integer.parseInt(config.getProperty(minIdlePropertyName)));
        /* The minimum number of established connections that should be kept in the pool at all times.
           If using threads 1.25-1.5x the number of threads is safe
           This will ensure that connections are kept to the back end so they will recycle quickly
        */
        poolConf.setTestOnBorrow(false);
        /* when true - send a ping before when we attempt to grab a connection from the pool
           Generally not recommended as while the PING command (https://redis.io/commands/PING) is relatively lightweight
           if there is much borrowing happening this can increase traffic if the number of operations per connection is low
        */
        poolConf.setTestOnReturn(false);
        /* when true - send a ping upon returning a pool connection
           I cannot imagine a scenario where this would be useful
        */
        poolConf.setTestWhileIdle(true);
        /* when true - send ping from idle resources in the pool
           Again the ping is not expensive
           Recommend setting this to true if you have a firewall between client and server that disconnects idle TCP connections
           Also common issue on the cloud with load balancers (https://aws.amazon.com/blogs/aws/elb-idle-timeout-control/)
        */
        poolConf.setMaxWaitMillis(Integer.parseInt(config.getProperty(maxWaitPropertyName)));
        /* set max timeout in milliseconds for write operations
           default is -1 which means wait forever
           Tune this carefully - often a good idea to slightly exceed your redis SLOWLOG settings,
           so you can view what is taking so long (https://redis.io/commands/slowlog)
        */
        return poolConf;
    }

    private void initPool(HostAndPort hostAndPort) {

        String user, password;
        password = config.get(passwordPropertyName).toString();
        if (password.isEmpty()) {
            user = null;
            password = null;
        } else {
            user = config.getProperty(userPropertyName);
            if (user.isEmpty()) {
                user = null;
            }
        }

        String timeoutStr = config.get(timeoutPropertyName).toString();
        int timeout;
        if (!timeoutStr.isEmpty()) {
            timeout = Integer.parseInt(timeoutStr);
        } else {
            timeout = redis.clients.jedis.Protocol.DEFAULT_TIMEOUT;
        }

        jedisPool = new JedisPool(initPoolConfig(), hostAndPort.getHost(), hostAndPort.getPort(), timeout, user, password);
    }

    public JedisPool getJedisPool(){
        return jedisPool;
    }

    public static JedisConnectionFactory getInstance() {
        if (instance == null) {
            instance = new JedisConnectionFactory();
        }
        return instance;
    }

}