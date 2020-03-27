package com.ibm.hello.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.hello.service.SimpleConsumerService;
import com.ibm.hello.service.SimpleProducerService;

@RestController
public class SimpleKafkaController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaController.class);

	
	@Autowired
	SimpleProducerService simpleProducerService;
	
	@Autowired
	SimpleConsumerService simpleConsumerService;
	
    @GetMapping(value = "/simple-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String simpleProducer() {
    	LOGGER.info("In Controller for the simple producer");
    	simpleProducerService.runProducer();
    	return "Success";
    	
    }
    
    @GetMapping(value = "/simple-consumer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public String simpleConsumer() {
    	LOGGER.info("In Controller for the simple consumer");
    	simpleConsumerService.runConsumer();
    	return "Success";
    }

}
