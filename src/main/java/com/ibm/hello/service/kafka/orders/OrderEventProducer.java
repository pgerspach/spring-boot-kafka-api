package com.ibm.hello.service.kafka.orders;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.hello.configuration.KafkaConfiguration;
import com.ibm.hello.model.kafka.orders.OrderEvent;
import com.ibm.hello.task.kafka.orders.OrderService;


@Service
public class OrderEventProducer implements EventEmitter {
		private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventProducer.class);

		@Autowired
		private KafkaConfiguration kafkaConfiguration;
		
		private ObjectMapper objectMapper = new ObjectMapper();
	    private KafkaProducer<String, String> kafkaProducer;


	    public OrderEventProducer() {
	        init();
	    }
	    
	    protected void init() {
	    	Properties properties = getKafkaConfiguration().getProducerProperties("order-event-producer");
	        kafkaProducer = new KafkaProducer<String, String>(properties);
	    	
	    }
	    
	    public KafkaConfiguration getKafkaConfiguration() {
	    	// to bypass some strange NPE, as inject does not seem to work
	    	if (kafkaConfiguration == null ) {
	    		kafkaConfiguration = new KafkaConfiguration();
	    	}
	    	return kafkaConfiguration;
	    }
	    
	    @Override
	    public void emit(OrderEvent orderEvent) throws Exception  {
	    	String value = objectMapper.writeValueAsString(orderEvent);
	    	LOGGER.info("Send " + value);
	    	String key = orderEvent.getPayload().getOrderID();
	        ProducerRecord<String, String> record = new ProducerRecord<>(getKafkaConfiguration().getOrdersTopicName(), key, value);

	        Future<RecordMetadata> send = kafkaProducer.send(record, 
	        		new Callback() {

						@Override
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							if (exception != null) {
								exception.printStackTrace();
							} else {
							   System.out.println("The offset of the record just sent is: " + metadata.offset());
				                   
							}
						}
	        		}
	        );
	        try {
				send.get(KafkaConfiguration.PRODUCER_TIMEOUT_SECS, TimeUnit.SECONDS);
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
	    }

		@Override
		public void safeClose() {
			kafkaProducer.close();
		}
}
