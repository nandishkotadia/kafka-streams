package com.nandish.code.kafka.stream.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.nandish.code.kafka.stream.model.Click;
import com.nandish.code.kafka.stream.model.Impression;
import com.nandish.code.kafka.stream.util.Constants.KafkaTopic;
import com.nandish.code.kafka.stream.util.JsonPOJODeserializer;
import com.nandish.code.kafka.stream.util.JsonPOJOSerializer;
import com.nandish.code.kafka.stream.util.JsonSerde;

@Component
public class KafkaStreamConfig {

	@Value("${bootstrap.servers}")
	private String bootstrapServers;
	
	@Value("${stream.commit.interval.ms}")
	private String streamCommitIntervalMs;
	
	@Value("${stream.threads.count}")
	private String numberStreamThreads;
	
	@Value("${replication.factor}")
	private Integer replicationFactor;
	
	@SuppressWarnings("rawtypes")
	@Autowired
	private JsonPOJOSerializer serializer;
	
	@SuppressWarnings("rawtypes")
	@Autowired
	private JsonPOJODeserializer deserializer;
	
	public Properties getStreamConfiguration(String applicationId, String threadName) {
		Properties streamsConfiguration = new Properties();
	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
	    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, applicationId+"-"+threadName);
	    
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
	    streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicationFactor);
	    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
	    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, streamCommitIntervalMs);
	    streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberStreamThreads);
	    streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
	    
	    streamsConfiguration.put(StreamsConfig.RECEIVE_BUFFER_CONFIG,"1048576");
	    streamsConfiguration.put(StreamsConfig.SEND_BUFFER_CONFIG,"1048576");
	    
	    streamsConfiguration.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "65000");
	    streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
	    //streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "15048576");
	    //streamsConfiguration.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,Arrays.asList(org.apache.kafka.streams.processor.internals.assignment.StickyTaskAssignor.class));
	    
	    //streamsConfiguration.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"15048576");
	    //streamsConfiguration.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,net.media.max.kafka.stream.config.RoundRobinPartitioner.class);
	    //streamsConfiguration.put("rebalance.max.retries", "16");
	    //streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
	    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/"+threadName);
	    streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");
	    
	    return streamsConfiguration;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Serde getSerde() {
		Map<String, Object> serdeProps = getSerdeProps();
		serializer.configure(serdeProps, false);
		deserializer.configure(serdeProps, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	public static Map<String, Object> getSerdeProps() {
		Map<String, Object> serdeProps = new HashMap<>();
		serdeProps.put(KafkaTopic.Click, Click.class);
		serdeProps.put(KafkaTopic.Impression, Impression.class);
		
		return serdeProps;
	}
	
}
