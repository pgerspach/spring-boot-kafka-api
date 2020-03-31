package com.ibm.orderskafka.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ibm.orderskafka.model.Address;
import com.ibm.orderskafka.model.OrderEntity;
import com.ibm.orderskafka.model.OrderFactory;
import com.ibm.orderskafka.model.OrderParameters;
import com.ibm.orderskafka.task.OrderService;

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
	
	@PostMapping(value = "/order-producer", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
	public OrderEntity orderProducer(OrderParameters orderParameters) {
		LOGGER.info("In Controller for the simple order producer");
		OrderEntity order = OrderFactory.createNewOrder(orderParameters);
		orderService.createOrder(order);
		return order;
	}
}
