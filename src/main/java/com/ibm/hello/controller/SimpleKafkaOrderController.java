package com.ibm.hello.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.hello.model.Address;
import com.ibm.hello.model.OrderEntity;
import com.ibm.hello.model.OrderFactory;
import com.ibm.hello.model.OrderParameters;
import com.ibm.hello.task.OrderService;

@RestController
public class SimpleKafkaOrderController {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaOrderController.class);

	@Autowired
	OrderService orderService;

	@GetMapping(value = "/simple-order-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public OrderEntity simpleOrderProducer() {
		LOGGER.info("In Controller for the simple order producer");
		Address address = new Address("123 main street", "ny", "usa", "ny", "10001");
		OrderParameters orderParameters = new OrderParameters("12345", "12345", 1, address);
		OrderEntity order = OrderFactory.createNewOrder(orderParameters);
		orderService.createOrder(order);
		return order;

	}
	
	@GetMapping(value = "/simple-order-consumer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public OrderEntity simpleOrderConsumer() {
		LOGGER.info("In Controller for the simple order consumer");
		Address address = new Address("123 main street", "ny", "usa", "ny", "10001");
		OrderParameters orderParameters = new OrderParameters("12345", "12345", 1, address);
		OrderEntity order = OrderFactory.createNewOrder(orderParameters);
		orderService.createOrder(order);
		return order;

	}
}
