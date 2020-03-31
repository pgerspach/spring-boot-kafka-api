package com.ibm.simplekafka.model;

import java.util.List;

public class SimpleKafkaConsumerResponse {
	
	List<SimpleKafkaConsumerResponseRecord> simpleKafkaConsumerResponse;
	
	public SimpleKafkaConsumerResponse(List<SimpleKafkaConsumerResponseRecord> simpleKafkaConsumerResponseRecord) {
		super();
		this.simpleKafkaConsumerResponse = simpleKafkaConsumerResponseRecord;
	}

	public List<SimpleKafkaConsumerResponseRecord> getSimpleKafkaConsumerResponseRecord() {
		return simpleKafkaConsumerResponse;
	}
	
}
