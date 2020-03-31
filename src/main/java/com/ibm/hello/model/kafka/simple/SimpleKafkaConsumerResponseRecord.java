package com.ibm.hello.model.kafka.simple;

public class SimpleKafkaConsumerResponseRecord {
	long kafkaKey;
	String kafkaValue;
	int kafkaPartition;
	long kafkaOffset;
		
	public SimpleKafkaConsumerResponseRecord(long kafkaKey, String kafkaValue, int kafkaPartition, long kafkaOffset) {
		super();
		this.kafkaKey = kafkaKey;
		this.kafkaValue = kafkaValue;
		this.kafkaPartition = kafkaPartition;
		this.kafkaOffset = kafkaOffset;
	}
	
	public long getKafkaKey() {
		return kafkaKey;
	}
	public String getKafkaValue() {
		return kafkaValue;
	}
	public int getKafkaPartition() {
		return kafkaPartition;
	}
	public long getKafkaOffset() {
		return kafkaOffset;
	}

}
