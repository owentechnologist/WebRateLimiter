package com.redislabs.sa.ot.util;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import redis.clients.jedis.exceptions.JedisDataException;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class RedisStreamAdapter {

    private JedisPooled jedisPooled;
    private String streamName;
    private String consumerGroupName;

    public RedisStreamAdapter(String streamName, JedisPooled jedisPooled){
        this.jedisPooled = jedisPooled;
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
            String thing = this.jedisPooled.xgroupCreate(this.streamName, this.consumerGroupName, nextID, true);
        }catch(JedisDataException jde){
            System.out.println("ConsumerGroup "+consumerGroupName+ " already exists -- continuing");
        }
    }

    // This Method can be invoked multiple times each time with a unique consumerName
    // Assumes The group has been created - now we want a single named consumer to start
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
                int blockTime = Integer.MAX_VALUE-2000; // on some OS MAX_VALUE results in a negative value! (overflow)
                while(true) {
                    //grab one entry from the target stream at a time
                    //block for long time between attempts
                    XReadGroupParams xReadGroupParams = new XReadGroupParams().count(1).block(blockTime);
                    List<Map.Entry<String, List<StreamEntry>>> streamResult =
                            jedisPooled.xreadGroup(consumerGroupName.getBytes(StandardCharsets.UTF_8),
                                    consumerName.getBytes(StandardCharsets.UTF_8),
                                    xReadGroupParams,
                                    new AbstractMap.SimpleEntry(streamName,StreamEntryID.UNRECEIVED_ENTRY));
                    key = streamResult.get(0).getKey(); // name of Stream
                    streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update
                    value = streamEntryList.get(0);// entry written to stream
                    System.out.println("ConsumerGroup "+consumerGroupName+" and Consumer "+consumerName+" has received... "+key+" "+value);
                    Map<String,StreamEntry> entry = new HashMap();
                    entry.put(key+":"+value.getID()+":"+consumerName,value);
                    lastSeenID = value.getID();
                    streamEventMapProcessor.processStreamEventMap(entry);
                    jedisPooled.xack(key, consumerGroupName, lastSeenID);
                    jedisPooled.xdel(key,lastSeenID);// delete test
                }
            }
        }).start();
    }

    // this does not use consumer groups - all messages are consumed from target stream
    public void listenToStream(StreamEventMapProcessor streamEventMapProcessor){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String key = "";
                    List<StreamEntry> streamEntryList = null;
                    StreamEntry value = null;

                    StreamEntryID nextID = new StreamEntryID("0-0");
                    System.out.println("main.kickOffStreamListenerThread: Actively Listening to Stream " + streamName);
                    HashMap<String, StreamEntryID> streamQuery = new HashMap<String,StreamEntryID>();
                    while (true) {
                        try {
                            streamQuery = new HashMap();
                            streamQuery.put(streamName,nextID);
                            XReadParams xReadParams = new XReadParams().block(Integer.MAX_VALUE).count(1);
                            //HashMap<String,StreamEntryID> targetStreams = new HashMap<String,StreamEntryID>(){{}}
                            List<Map.Entry<String, List<StreamEntry>>> streamResult =
                                    jedisPooled.xread(xReadParams, streamQuery);
                            key = streamResult.get(0).getKey(); // name of Stream
                            streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update

                            value = streamEntryList.get(0);// entry written to stream
                            System.out.println("StreamListenerThread: received... " + key + " " + value);
                            Map<String, StreamEntry> entry = new HashMap();
                            entry.put(key + ":" + value.getID(), value);
                            streamEventMapProcessor.processStreamEventMap(entry);
                            nextID = value.getID();
                        } catch (java.lang.IndexOutOfBoundsException iobe) {
                        }catch(Throwable t){
                            t.printStackTrace();
                        }
                    }
                }
        }).start();
    }
}