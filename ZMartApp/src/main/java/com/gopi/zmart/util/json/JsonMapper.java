package com.gopi.zmart.util.json;

import com.gopi.zmart.util.json.Json;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Author: Gopinathan Munappy
 *  Date : 16/11/2018
 *  Time : 11.45 AM
 *
 */

public class JsonMapper implements ValueMapper<String, Json> {

    private static final Logger log = LoggerFactory.getLogger(JsonMapper.class);

    private JsonParser jsonParser = new JsonParser();

    @Override
    public Json apply(String value) {
        Json json = new Json(jsonParser.parse(value).getAsJsonObject());

        log.info("Processing EventID={}", json.propertyLongValue("eventId"));
        return json;
    }
}