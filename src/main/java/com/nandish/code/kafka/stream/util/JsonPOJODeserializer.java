package com.nandish.code.kafka.stream.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;
import org.springframework.util.StringUtils;

import com.google.gson.Gson;
import com.nandish.code.kafka.stream.config.AutoConfig;
import com.nandish.code.kafka.stream.model.Parent;

public class JsonPOJODeserializer<T> implements Deserializer<T>,Constants {
	
	private Gson gson = AutoConfig.gson();
    private Map<String,?> props;
    
    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
    }
    public JsonPOJODeserializer(Map<String, ?> props) {this.props = props;}

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
    	this.props=props;
    }

    @SuppressWarnings("unchecked")
	@Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;
        Class<T> tClass = (Class<T>) props.get(topic);
    	String dataStr = new String(bytes);
    	String classType = topic;
        T data = null;
        try {
        	if(StringUtils.isEmpty(dataStr)){
        		return null;
        	}
        	if(tClass == null){
        		JSONObject jObject  = new JSONObject(dataStr);
        		String type = (String) jObject.get("type");
        		tClass = (Class<T>) props.get(type);
        		classType = type;
        	}
        	if(tClass == null){
        		return null;
        	}
        	data = gson.fromJson(dataStr, tClass);
        	if(((Parent) data).getType()==null){
        		((Parent) data).setType(classType);
        	}
        } catch (Exception e) {
        	System.out.println("Error while deserializing: "+e);
        	System.out.println("Not able to deserialize value:"+dataStr);
        	e.printStackTrace();
        	return null;
        }
        return data;
    }

    @Override
    public void close() {
    	
    }
    
}
