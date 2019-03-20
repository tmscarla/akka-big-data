package com.gof.akka.functions;

public interface FilterFunction extends AbstractFunction {
    public Boolean predicate(String key, String value);
}
