package com.ibm.kafkastream.wordcount.service;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WordCountKafkaStreamOperator {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountKafkaStreamOperator.class);

    private final KafkaStreams streams;
    private final Topology topology;
    final StreamsBuilder builder = new StreamsBuilder();
    Properties props = new Properties();


    
    public WordCountKafkaStreamOperator() {
        defineStream();
        topology = builder.build();
        streams = new KafkaStreams(topology, props);
        LOGGER.info("WordCountKafkaStreamOperator has been initialized and is running");
    }
    
    private void defineStream() {
    	
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //props = getKafkaConfiguration().getStreamingProperties("SampleStreamer");
    	        
        KStream<String, String> source = builder.stream("streams-wordcount-plaintext-input");
       
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
              .groupBy((key, value) -> value)
              .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
              .toStream()
              .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
    }
	
    public void start() {
    	LOGGER.info("WordCount Stream Operator started.");
        try {
            streams.start();
        } catch (final Throwable e) {
            LOGGER.error("Error starting the WordCount Stream Operator.", e);
        }
    }

    public void stop() {
    	LOGGER.info("WordCount Stream Operator stopped");
        streams.close();
    }
}
