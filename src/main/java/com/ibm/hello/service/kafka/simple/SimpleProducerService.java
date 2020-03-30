package com.ibm.hello.service.kafka.simple;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ibm.hello.config.kafka.simple.SimpleKafkaConstants;
import com.ibm.hello.model.kafka.simple.SimpleKafkaProducerResponse;


@Service
public class SimpleProducerService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerService.class);

	Producer<Long, String> kafkaProducer;
	
	@Autowired
	public SimpleProducerService(Producer<Long, String> kafkaProducer) {
		super();
		this.kafkaProducer = kafkaProducer;
	}

	public SimpleKafkaProducerResponse runProducer() {

		int successCounter = 0;
		int failCounter = 0;
		for (int index = 0; index < SimpleKafkaConstants.MESSAGE_COUNT; index++) {
			//final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(SimpleKafkaConstants.TOPIC_NAME,"This is record " + index);
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(SimpleKafkaConstants.TOPIC_NAME, Long.valueOf(index), "This is record " + index);
			try {
				RecordMetadata metadata = kafkaProducer.send(record).get();
				LOGGER.info("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
				successCounter++;
			} catch (ExecutionException | InterruptedException e) {
				LOGGER.info("Error in sending record", e);
				failCounter++;
			}
		}
		return new SimpleKafkaProducerResponse(successCounter, failCounter);
	}

}
