package com.nandish.code.kafka.stream.processor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nandish.code.click.ClickStreamProcessor;
import com.nandish.code.kafka.stream.util.Constants.KafkaStreamType;

@Component
public class KafkaStreamProcessorFactory {
	
	@Autowired
	private ClickStreamProcessor clickStreamProcessor;
	
	public KafkaStreamProcessor getProcessor(String streamType){
		switch(streamType){
			case KafkaStreamType.click:
				return clickStreamProcessor;
			default:	
				return clickStreamProcessor;
		}
	}

}