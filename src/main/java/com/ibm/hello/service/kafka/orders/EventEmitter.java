package com.ibm.hello.service.kafka.orders;

import com.ibm.hello.model.kafka.orders.OrderEvent;

public interface EventEmitter {

    public void emit(OrderEvent event) throws Exception;
    public void safeClose();

}
