package com.gof.akka.operators;

public interface FilterFunction {
    public Boolean predicate(String key, String value);
}
