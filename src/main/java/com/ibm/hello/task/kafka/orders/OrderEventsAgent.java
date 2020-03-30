package com.ibm.hello.task.kafka.orders;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.hello.configuration.KafkaConfiguration;
import com.ibm.hello.listener.kafka.orders.AgentsInitializer;
import com.ibm.hello.model.kafka.orders.OrderEvent;


/**
 * Base runnable agent to continuously listen to event on the main topic 
 *
 */
@Service
public class OrderEventsAgent implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventsAgent.class);

	private KafkaConsumer<String, String> kafkaConsumer = null;
	
	private ObjectMapper objectMapper = new ObjectMapper();
	//private Jsonb jsonb = JsonbBuilder.create();
	
	private boolean running = true;
	
	@Autowired
	private KafkaConfiguration kafkaConfiguration;
	
	public OrderEventsAgent() {
    }
    
    public OrderEventsAgent(KafkaConsumer<String, String> kafkaConsumer) {
    	this.kafkaConsumer = kafkaConsumer;
    }
    
    public boolean isRunning() {
    	return running;
    }
    
    private void init() {
    	// if we need to have multiple threads then the clientId needs to be different
    	// auto commit is set to true, and read from the last not committed offset
    	Properties properties = kafkaConfiguration.getConsumerProperties(
          		"OrderEventsAgent",	
          		true,  
          		"latest" );
        this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
    	this.kafkaConsumer.subscribe(Collections.singletonList(kafkaConfiguration.getOrdersTopicName()));
    	LOGGER.info("OrderEventsAgent has beein initialized.");
    }
    
	@Override
	public void run() {
		init();
		while (this.running) {
			try {
				Queue<OrderEvent> events = poll();
				for (OrderEvent event : events) {
					LOGGER.info("Calling event handler: {}", event.getPayload().getOrderID());
					handle(event);
				}
			} catch (KafkaException  ke) {
				ke.printStackTrace();
				// when issue on getting data from topic we need to reconnect
				stop();
			}
		}
		stop();
	}

	public void stop() {
		this.running = false;
		try {
			if (kafkaConsumer != null)
				kafkaConsumer.close(KafkaConfiguration.CONSUMER_CLOSE_TIMEOUT);
        } catch (Exception e) {
            LOGGER.info("Failed closing Consumer " +  e.getMessage());
        }
	}

	private Queue<OrderEvent> poll(){
		 ConsumerRecords<String, String> recs = kafkaConsumer.poll(KafkaConfiguration.CONSUMER_POLL_TIMEOUT);
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
			return objectMapper.readValue(eventAsString, OrderEvent.class);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private void handle(OrderEvent event) {
		LOGGER.info("Event to process with business logic: {}",  event.getPayload().getOrderID());
	}
}
