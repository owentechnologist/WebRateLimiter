package com.redislabs.sa.ot.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.StreamEntryID.*;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamAdapter {

    private JedisPool connectionPool;
    private String streamName;
    private String consumerGroupName;

    public RedisStreamAdapter(String streamName, JedisPool connectionPool){
        this.connectionPool=connectionPool;
        this.streamName=streamName;
    }

    // this classes' constructor determines the target StreamName
    // we need to only provide the consumer group name
    // Maybe? This consumer should begin with id 0 to get all data from the beginning of the stream
    // using id $ (LAST_ENTRY) will end up with only new data being transmitted from the time the group is created
    public void createConsumerGroup(String consumerGroupName){
        this.consumerGroupName = consumerGroupName;
        StreamEntryID nextID = StreamEntryID.LAST_ENTRY;
        try {
            String thing = this.connectionPool.getResource().xgroupCreate(this.streamName, this.consumerGroupName, nextID, true);
        }catch(JedisDataException jde){
            System.out.println("ConsumerGroup "+consumerGroupName+ " already exists -- continuing");
        }
    }

    // This Method can be invoked multiple times each time with a unique consumerName
    //Assumes The group has been created - now we want a single named consumer to start
    // using 0 will grab any pending messages for that listener in case it failed mid-processing
    public void namedGroupConsumerStartListening(String consumerName, StreamEventMapProcessor streamEventMapProcessor){
        new Thread(new Runnable() {
            @Override
            public void run() {
                String key = "0"; // get all data for this consumer in case it is in recovery mode
                List<StreamEntry> streamEntryList = null;
                StreamEntry value = null;
                StreamEntryID lastSeenID = null;
                System.out.println("RedisStreamAdapter.namedGroupConsumerStartListening(--> "+consumerName+"  <--): Actively Listening to Stream "+streamName);
                long counter = 0;
                Map.Entry<String, StreamEntryID> streamQuery = null;
                long blockTime = Long.MAX_VALUE-200000; // on some OS MAX_VALUE results in a negative value! (overflow)
                while(true) {
                    try (Jedis streamReader = connectionPool.getResource();) {
                        //grab one entry from the target stream at a time
                        //block for long time between attempts
                        List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                streamReader.xreadGroup(consumerGroupName, consumerName,
                                        1, blockTime, false, new AbstractMap.SimpleEntry(streamName,StreamEntryID.UNRECEIVED_ENTRY));
                        key = streamResult.get(0).getKey(); // name of Stream
                        streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update
                        value = streamEntryList.get(0);// entry written to stream
                        System.out.println("ConsumerGroup "+consumerGroupName+" and Consumer "+consumerName+" has received... "+key+" "+value);
                        Map<String,StreamEntry> entry = new HashMap();
                        entry.put(key+":"+value.getID()+":"+consumerName,value);
                        lastSeenID = value.getID();
                        streamEventMapProcessor.processStreamEventMap(entry);
                        streamReader.xack(key, consumerGroupName, lastSeenID);
                        streamReader.xdel(key,lastSeenID);// delete test
                    }
                }
            }
        }).start();
    }

    // this does not use consumer groups - all messages are consumed from target stream
    public void listenToStream(StreamEventMapProcessor streamEventMapProcessor){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try (Jedis streamListener =  connectionPool.getResource();){
                        String key = "";
                        List<StreamEntry> streamEntryList = null;
                        StreamEntry value = null;

                        StreamEntryID nextID = new StreamEntryID("0-0");
                        System.out.println("main.kickOffStreamListenerThread: Actively Listening to Stream "+streamName);
                        Map.Entry<String, StreamEntryID> streamQuery = null;
                        while(true){
                            try {
                                streamQuery = new AbstractMap.SimpleImmutableEntry<>(
                                        streamName, nextID);
                                List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                         streamListener.xread(1,Long.MAX_VALUE,streamQuery);// <--  has to be Long.MAX_VALUE to work
                                key = streamResult.get(0).getKey(); // name of Stream
                                streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update

                                value = streamEntryList.get(0);// entry written to stream
                                System.out.println("StreamListenerThread: received... " + key + " " + value);
                                Map<String, StreamEntry> entry = new HashMap();
                                entry.put(key + ":" + value.getID(), value);
                                streamEventMapProcessor.processStreamEventMap(entry);
                                nextID = value.getID();
                            }catch(java.lang.IndexOutOfBoundsException iobe){}//ignore
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }).start();
    }

}
