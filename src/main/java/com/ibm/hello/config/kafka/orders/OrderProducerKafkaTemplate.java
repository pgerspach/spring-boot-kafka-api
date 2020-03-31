package com.ibm.hello.config.kafka.orders;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OrderProducerKafkaTemplate {

	@Autowired
	private KafkaConfiguration kafkaConfiguration;

	@Bean //("orderProducerKafkaTemplate")
	public KafkaProducer<String, String> getOrderProducerKafkaTemplate() {
		Properties properties = kafkaConfiguration.getProducerProperties("order-event-producer");
		return new KafkaProducer<String, String>(properties);
	}
}
