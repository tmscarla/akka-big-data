package com.gof.akka.functions;

import com.gof.akka.messages.Message;

public interface MapFunction extends AbstractFunction {
    public Message process(String key, String value);
}
