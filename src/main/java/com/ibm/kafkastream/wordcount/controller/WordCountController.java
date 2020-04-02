package com.ibm.kafkastream.wordcount.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.kafkastream.wordcount.model.WordCountProducerResponse;
import com.ibm.kafkastream.wordcount.service.WordCountProducerService;

@RestController
public class WordCountController {

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountController.class);

	@Autowired
	WordCountProducerService wordCountProducerService;

	@PostMapping(value = "/word-count-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public WordCountProducerResponse orderProducer(String input) {
		LOGGER.info("##### In Controller for the word count producer");
		wordCountProducerService.emit(input);
		return new WordCountProducerResponse("Success");
	}
}
