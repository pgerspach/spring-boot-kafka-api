package com.ibm.kafkastream.wordcount.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.ibm.kafkastream.wordcount.config.WordCountKafkaConfiguration;

@Service
public class WordCountProducerService implements EventEmitter {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountProducerService.class);

	private WordCountKafkaConfiguration kafkaConfiguration;

	private KafkaProducer<String, String> wordCountProducerKafkaTemplate;

	@Autowired
	public WordCountProducerService(WordCountKafkaConfiguration kafkaConfiguration,
			@Qualifier("wordCountProducerKafkaTemplateA") KafkaProducer<String, String> wordCountProducerKafkaTemplate) {
		super();
		this.kafkaConfiguration = kafkaConfiguration;
		this.wordCountProducerKafkaTemplate = wordCountProducerKafkaTemplate;
	}

	@Override
	public void emit(String words)  {
		LOGGER.info("##### Sending to kafka topic:{} the following words: {} ",kafkaConfiguration.getWordCountProducerTopicName(), words);
		//String key = orderEvent.getPayload().getOrderID();

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaConfiguration.getWordCountProducerTopicName(), words);
		Future<RecordMetadata> send = wordCountProducerKafkaTemplate.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					LOGGER.error("##### Error sending record: {} to Kafka topic: {}", words, kafkaConfiguration.getWordCountProducerTopicName(), exception);
				} else {
					LOGGER.info("##### Successfully sent to topic: {} , the words: {}.  The offset is: {}. ", kafkaConfiguration.getWordCountProducerTopicName(), words, metadata.offset());
				}
			}
		});
		try {
			send.get(WordCountKafkaConfiguration.PRODUCER_TIMEOUT_SECS, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException | TimeoutException e) {
			LOGGER.error("##### Error sending record: {} to Kafka topic: {}", words, kafkaConfiguration.getWordCountProducerTopicName(), e);
		}
	}

	@Override
	public void safeClose() {
		wordCountProducerKafkaTemplate.close();
	}
}
