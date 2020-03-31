package com.ibm.hello.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.hello.model.kafka.orders.Address;
import com.ibm.hello.model.kafka.orders.OrderEntity;
import com.ibm.hello.model.kafka.orders.OrderFactory;
import com.ibm.hello.model.kafka.orders.OrderParameters;
import com.ibm.hello.task.kafka.orders.OrderService;

@RestController
public class OrderKafkaController {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(OrderKafkaController.class);

	OrderService orderService;

	@Autowired
	public OrderKafkaController(OrderService orderService) {
		super();
		this.orderService = orderService;
	}

	@GetMapping(value = "/simple-order-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public OrderEntity simpleOrderProducer() {
		LOGGER.info("In Controller for the simple order producer");
		Address address = new Address("123 main street", "ny", "usa", "ny", "10001");
		OrderParameters orderParameters = new OrderParameters("12345", "12345", 1, address);
		OrderEntity order = OrderFactory.createNewOrder(orderParameters);
		orderService.createOrder(order);
		return order;

	}
}
