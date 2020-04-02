package com.ibm.kafkastream.wordcount.model;

public class WordCountProducerResponse {
	private String status;
	
	public WordCountProducerResponse(String status) {
		super();
		this.status = status;
	}

	public String getStatus() {
		return status;
	}
}
