package com.nandish.code.click;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.nandish.code.kafka.stream.config.KafkaStreamConfig;
import com.nandish.code.kafka.stream.model.Click;
import com.nandish.code.kafka.stream.model.ClickImpRecord;
import com.nandish.code.kafka.stream.model.Impression;
import com.nandish.code.kafka.stream.processor.KafkaStreamProcessor;
import com.nandish.code.kafka.stream.util.Constants;


@Component
public class ClickStreamProcessor implements Constants,KafkaStreamProcessor{

	@Autowired
	private KafkaStreamConfig kafkaStreamConfig;
	
	private long clickJoinWindowTimeMs = 12*60*60*1000;//12 hours
	
	@Autowired
	private ClickValueJoiner clickImpValueJoiner;
	
	private  Logger logger = LoggerFactory.getLogger(ClickStreamProcessor.class);

	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	@Override
	public void process(String threadName) {
		Properties streamsConfiguration = kafkaStreamConfig.getStreamConfiguration(KafkaStreamType.click, threadName);
		
		StreamsBuilder builder = new StreamsBuilder();
		
		KStream impressionStream = builder.stream(KafkaTopic.Impression)
				.filter((k,v) -> v!=null).selectKey((k, v) -> getImpressionKeyForJoin((Impression) v));
		impressionStream.print();
		
		KStream clickStream = builder.stream(KafkaTopic.Click)
				.filter((k,v) -> v!=null).selectKey((k, v) -> getClickKeyForJoin((Click) v));
//		clickStream.print(Printed.toSysOut().withLabel("").withKeyValueMapper(null));
		
		ValueJoiner<Click, Impression, ClickImpRecord> clickImpJoiner = clickImpValueJoiner.getClickImpJoiner();
		KStream<String, ClickImpRecord> clickImpStream = clickStream.join(impressionStream, clickImpJoiner, 
				(JoinWindows) JoinWindows.of(clickJoinWindowTimeMs));
		
		clickImpStream.to(KafkaTopic.ClickImp);
		clickImpStream.print();
		KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.start();
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}

	private String getImpressionKeyForJoin(Impression i) {
		return i.getImpressionId();
	}

	private String getClickKeyForJoin(Click v) {
		return v.getImpressionId();
	}
	
	
}
