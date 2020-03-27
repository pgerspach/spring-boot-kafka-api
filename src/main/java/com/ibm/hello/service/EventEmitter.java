package com.ibm.hello.service;

import com.ibm.hello.model.OrderEvent;

public interface EventEmitter {

    public void emit(OrderEvent event) throws Exception;
    public void safeClose();

}
