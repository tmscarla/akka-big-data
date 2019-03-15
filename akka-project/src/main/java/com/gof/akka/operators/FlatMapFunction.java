package com.gof.akka.operators;

import com.gof.akka.messages.Message;

import java.util.List;

public interface FlatMapFunction {
    public List<Message> process(String key, String value);

}
