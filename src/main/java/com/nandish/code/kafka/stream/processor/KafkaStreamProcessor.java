package com.nandish.code.kafka.stream.processor;

public interface KafkaStreamProcessor {
	
	public void process(String threadName);

}
