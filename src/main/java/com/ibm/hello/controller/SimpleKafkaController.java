package com.ibm.hello.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.hello.model.kafka.simple.SimpleKafkaConsumerResponse;
import com.ibm.hello.model.kafka.simple.SimpleKafkaProducerResponse;
import com.ibm.hello.service.kafka.simple.SimpleConsumerService;
import com.ibm.hello.service.kafka.simple.SimpleProducerService;

@RestController
public class SimpleKafkaController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaController.class);
	
	SimpleProducerService simpleProducerService;
	
	SimpleConsumerService simpleConsumerService;
	
	@Autowired
	public SimpleKafkaController(SimpleProducerService simpleProducerService,
			SimpleConsumerService simpleConsumerService) {
		super();
		this.simpleProducerService = simpleProducerService;
		this.simpleConsumerService = simpleConsumerService;
	}
	
    @GetMapping(value = "/simple-kafka-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public SimpleKafkaProducerResponse simpleProducer() {
    	LOGGER.info("In Controller for the simple producer");
    	return simpleProducerService.runProducer();
    }
    
    @GetMapping(value = "/simple-kafka-consumer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public SimpleKafkaConsumerResponse simpleConsumer() {
    	LOGGER.info("In Controller for the simple consumer");
    	return simpleConsumerService.runConsumer();
    }

}
