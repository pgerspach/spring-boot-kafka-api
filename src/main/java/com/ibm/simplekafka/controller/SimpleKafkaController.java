package com.ibm.simplekafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.simplekafka.model.SimpleKafkaConsumerResponse;
import com.ibm.simplekafka.model.SimpleKafkaProducerResponse;
import com.ibm.simplekafka.service.SimpleConsumerService;
import com.ibm.simplekafka.service.SimpleProducerService;

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
