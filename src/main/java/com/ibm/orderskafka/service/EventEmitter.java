package com.ibm.orderskafka.service;

import com.ibm.orderskafka.model.OrderEvent;

public interface EventEmitter {

    public void emit(OrderEvent event) throws Exception;
    public void safeClose();

}
