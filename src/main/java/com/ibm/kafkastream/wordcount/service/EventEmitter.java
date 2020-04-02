package com.ibm.kafkastream.wordcount.service;

public interface EventEmitter {

    public void emit(String words) throws Exception;
    public void safeClose();

}
