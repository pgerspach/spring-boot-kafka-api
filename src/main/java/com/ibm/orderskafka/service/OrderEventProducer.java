package com.ibm.orderskafka.service;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.orderskafka.config.KafkaConfiguration;
import com.ibm.orderskafka.model.OrderEvent;

@Service
public class OrderEventProducer implements EventEmitter {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventProducer.class);

	private KafkaConfiguration kafkaConfiguration;

	private KafkaProducer<String, String> orderProducerKafkaTemplate;

	@Autowired
	public OrderEventProducer(KafkaConfiguration kafkaConfiguration,
			@Qualifier("orderProducerKafkaTemplateA") KafkaProducer<String, String> orderProducerKafkaTemplate) {
		super();
		this.kafkaConfiguration = kafkaConfiguration;
		this.orderProducerKafkaTemplate = orderProducerKafkaTemplate;
	}

	@Override
	public void emit(OrderEvent orderEvent) throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		String value = objectMapper.writeValueAsString(orderEvent);
		LOGGER.info("Send " + value);
		String key = orderEvent.getPayload().getOrderID();
		
		LOGGER.info("##### Sending to kafka topic:{} the following value: {} ",kafkaConfiguration.getOrdersTopicName(), value);


		ProducerRecord<String, String> record = new ProducerRecord<>(kafkaConfiguration.getOrdersTopicName(), key,
				value);

		Future<RecordMetadata> send = orderProducerKafkaTemplate.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					LOGGER.error("##### Error sending record: {} to Kafka topic: {}", value, kafkaConfiguration.getOrdersTopicName(), exception);
				} else {
					LOGGER.info("##### Successfully sent to topic: {} , the words: {}.  The offset is: {}. ", kafkaConfiguration.getOrdersTopicName(), value, metadata.offset());
				}
			}
		});
		try {
			send.get(KafkaConfiguration.PRODUCER_TIMEOUT_SECS, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException | TimeoutException e) {
			LOGGER.error("##### Error sending record: {} to Kafka topic: {}", value, kafkaConfiguration.getOrdersTopicName(), e);
		}
	}

	@Override
	public void safeClose() {
		orderProducerKafkaTemplate.close();
	}
}
