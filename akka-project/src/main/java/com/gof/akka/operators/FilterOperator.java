package com.gof.akka.operators;

import com.gof.akka.functions.FilterFunction;


public class FilterOperator extends Operator {
    public FilterFunction fun;

    public FilterOperator(String name, int batchSize) {
        super(name, batchSize);
        this.fun = fun;
    }
}
