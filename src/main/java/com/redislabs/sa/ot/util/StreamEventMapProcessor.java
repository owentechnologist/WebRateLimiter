package com.redislabs.sa.ot.util;

import redis.clients.jedis.StreamEntry;

import java.util.Map;

public interface StreamEventMapProcessor {
    public void processStreamEventMap(Map<String,StreamEntry> payload);
}
