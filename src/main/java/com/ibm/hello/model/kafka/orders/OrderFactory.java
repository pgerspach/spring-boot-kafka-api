package com.ibm.hello.model.kafka.orders;

import java.util.UUID;

public class OrderFactory {

	public static OrderEntity createNewOrder(OrderParameters dto) {
		OrderEntity order = new OrderEntity(UUID.randomUUID().toString(),
                dto.getProductID(),
                dto.getCustomerID(),
                dto.getQuantity(),
                dto.getDestinationAddress(),
                OrderEntity.PENDING_STATUS);
	   return order;
	}
}
