package com.nandish.code.kafka.stream.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.nandish.code.kafka.stream.config.KafkaStreamConfig;

@SuppressWarnings("rawtypes")
public class JsonSerde implements Serde {
	private JsonPOJOSerializer serializer = new JsonPOJOSerializer();
	private JsonPOJODeserializer deserializer = new JsonPOJODeserializer();

	public JsonSerde(){}
	
    @SuppressWarnings({ "unchecked" })
	@Override
    public void configure(Map configs, boolean isKey) {
        serializer.configure(KafkaStreamConfig.getSerdeProps(), isKey);
        deserializer.configure(KafkaStreamConfig.getSerdeProps(), isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

	@Override
    public Serializer serializer() {
        return serializer;
    }

	@Override
    public Deserializer deserializer() {
        return deserializer;
    }

}