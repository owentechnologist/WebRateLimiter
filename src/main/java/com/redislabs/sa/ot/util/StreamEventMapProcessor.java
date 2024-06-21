package com.redislabs.sa.ot.util;

import redis.clients.jedis.resps.StreamEntry;

import java.util.Map;

public interface StreamEventMapProcessor {
    public void processStreamEventMap(Map<String,StreamEntry> payload);
}
