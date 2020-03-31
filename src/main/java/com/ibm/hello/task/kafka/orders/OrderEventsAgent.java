package com.ibm.hello.task.kafka.orders;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.hello.config.kafka.orders.KafkaConfiguration;
import com.ibm.hello.model.kafka.orders.OrderEvent;


/**
 * Base runnable agent to continuously listen to event on the main topic 
 *
 */
@Service
public class OrderEventsAgent implements Runnable {
	
	private boolean running = true;
	
	private KafkaConfiguration kafkaConfiguration;
	
	private KafkaConsumer<String, String> orderConsumerKafkaTemplate;
	
	@Autowired
	public OrderEventsAgent(KafkaConfiguration kafkaConfiguration,
			KafkaConsumer<String, String> orderConsumerKafkaTemplate) {
		super();
		this.kafkaConfiguration = kafkaConfiguration;
		this.orderConsumerKafkaTemplate = orderConsumerKafkaTemplate;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventsAgent.class);
		
	@Override
	public void run() {
		init();
		while (this.running) {
			try {
				Queue<OrderEvent> events = poll();
				for (OrderEvent event : events) {
					LOGGER.info("##################################  THIS IS THE EVENT HANDLER CODE #########################");
					LOGGER.info("##### Event to process with business logic: {}",  event.getPayload().getOrderID());
				}
			} catch (KafkaException  ke) {
				LOGGER.info("There was a problem getting the order event from the topic.", ke);
				// when issue on getting data from topic we need to reconnect
				stop();
			}
		}
		stop();
	}
	
	public void stop() {
		this.running = false;
		try {
			if (orderConsumerKafkaTemplate != null)
				orderConsumerKafkaTemplate.close(KafkaConfiguration.CONSUMER_CLOSE_TIMEOUT);
        } catch (Exception e) {
            LOGGER.info("Failed closing Consumer " +  e.getMessage());
        }
	}
		
    private void init() {   	
    	orderConsumerKafkaTemplate.subscribe(Collections.singletonList(kafkaConfiguration.getOrdersTopicName()));
    	LOGGER.info("OrderEventsAgent has beein initialized.");
    }

	private Queue<OrderEvent> poll(){
		 ConsumerRecords<String, String> recs = orderConsumerKafkaTemplate.poll(KafkaConfiguration.CONSUMER_POLL_TIMEOUT);
	     Queue<OrderEvent> result = new LinkedList<OrderEvent>();
	        for (ConsumerRecord<String, String> rec : recs) {
	        	OrderEvent event = deserialize(rec.value());
	            result.add(event);
	        }
	        return result;
	}
	
	private OrderEvent deserialize(String eventAsString) {
		try {
			LOGGER.info("Message to deserialize: {}", eventAsString);
			ObjectMapper objectMapper = new ObjectMapper();
			return objectMapper.readValue(eventAsString, OrderEvent.class);
		} catch (IOException e) {
			LOGGER.info("Error deserializing order event.", e);
		}
		return null;
	}
}
