package com.gof.akka.operators;

public interface FilterFunction extends AbstractFunction {
    public Boolean predicate(String key, String value);
}
