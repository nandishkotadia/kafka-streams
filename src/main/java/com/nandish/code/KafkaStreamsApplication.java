package com.nandish.code;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.nandish.code.kafka.stream.processor.KafkaStreamProcessor;
import com.nandish.code.kafka.stream.processor.KafkaStreamProcessorFactory;
import com.nandish.code.kafka.stream.util.Constants.KafkaStreamType;


@SpringBootApplication
public class KafkaStreamsApplication implements CommandLineRunner{

	private  Logger logger = LoggerFactory.getLogger(KafkaStreamsApplication.class);
	
	@Autowired
	private KafkaStreamProcessorFactory kafkaStreamProcessorFactory;
	
	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsApplication.class, args);
	}
	
	@Override
	public void run(String... args) throws Exception {
		
		String streamType = args.length>0?args[0]:KafkaStreamType.impression;
		String threadName = args.length>1?args[1]:"common";
		switch(streamType){
			case  KafkaStreamType.impression:
			case  KafkaStreamType.click:
				logger.info("Starting kafka stream for type: "+streamType+" & threadName: "+threadName);
				KafkaStreamProcessor streamProcessor = kafkaStreamProcessorFactory.getProcessor(streamType);
				streamProcessor.process(threadName);
				break;
			default:
				break;
		}
		
	}
}
