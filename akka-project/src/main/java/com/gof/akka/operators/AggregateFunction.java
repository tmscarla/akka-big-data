package com.gof.akka.operators;

import com.gof.akka.messages.Message;
import java.util.List;

public interface AggregateFunction {
    public Message process(String key, List<String> values);
}
