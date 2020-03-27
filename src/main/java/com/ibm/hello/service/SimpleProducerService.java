package com.ibm.hello.service;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ibm.hello.config.SimpleKafkaConstants;


@Service
public class SimpleProducerService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducerService.class);

	
	public void runProducer() {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < SimpleKafkaConstants.MESSAGE_COUNT; index++) {
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(SimpleKafkaConstants.TOPIC_NAME,
					"This is record " + index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				LOGGER.info("Record sent with key " + index + " to partition " + metadata.partition()
						+ " with offset " + metadata.offset());
			} catch (ExecutionException e) {
				LOGGER.info("Error in sending record", e);
			} catch (InterruptedException e) {
				LOGGER.info("Error in sending record",e);
			}
		}
	}

}
