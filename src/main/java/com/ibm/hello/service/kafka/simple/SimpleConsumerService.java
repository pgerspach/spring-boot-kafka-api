package com.ibm.hello.service.kafka.simple;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ibm.hello.config.kafka.simple.SimpleKafkaConstants;

@Service
public class SimpleConsumerService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerService.class);

	
	public void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000)); 
			
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > SimpleKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				LOGGER.info("Record Key " + record.key());
				LOGGER.info("Record value " + record.value());
				LOGGER.info("Record partition " + record.partition());
				LOGGER.info("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}

}
