package com.ibm.orderskafka.task;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ibm.orderskafka.model.OrderEntity;
import com.ibm.orderskafka.model.OrderEvent;
import com.ibm.orderskafka.model.OrderPayload;
import com.ibm.orderskafka.service.OrderEventProducer;

@Service
public class OrderService {
	private static final Logger LOGGER = LoggerFactory.getLogger(OrderService.class);

	private OrderEventProducer eventProducer;


	@Autowired
	public OrderService(OrderEventProducer eventProducer) {
		this.eventProducer = eventProducer;
	}

	public void createOrder(OrderEntity order) {
		OrderEvent orderEvent = new OrderEvent(System.currentTimeMillis(), OrderEvent.TYPE_ORDER_CREATED);
		OrderPayload orderPayload = new OrderPayload(order.getOrderID(), order.getProductID(), order.getCustomerID(),
				order.getQuantity(), order.getStatus(), order.getDeliveryAddress());
		orderEvent.setPayload(orderPayload);
		try {
			LOGGER.info("emit event for " + order.getOrderID());
			eventProducer.emit(orderEvent);
		} catch (Exception e) {
			LOGGER.error("Error:", e);
		}
	}

}
