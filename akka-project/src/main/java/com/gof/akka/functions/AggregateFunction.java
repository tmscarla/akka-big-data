package com.gof.akka.functions;

import com.gof.akka.messages.Message;
import java.util.List;

public interface AggregateFunction extends AbstractFunction {
    public Message process(String key, List<String> values);
}
