package com.nandish.code.kafka.stream.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.nandish.code.kafka.stream.config.AutoConfig;

public class JsonPOJOSerializer<T> implements Serializer<T> {
	
    private Gson gson = AutoConfig.gson();
    private  Logger logger = LoggerFactory.getLogger(JsonPOJOSerializer.class);
    private Map<String,?> props;
    
    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
    }
    
    public JsonPOJOSerializer(Map<String, ?> props) {
    	this.props=props;
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    	this.props=props;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;
        try {
            return gson.toJson(data).getBytes();
        } catch (Exception e) {
        	logger.error("Error serializing JSON message: "+e);
        	return null;
        }
    }

    @Override
    public void close() {
    }

}
