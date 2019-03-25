package com.gof.akka.operators;

import com.gof.akka.functions.FilterFunction;


public class FilterOperator extends Operator {
    public FilterFunction fun;

    public FilterOperator(String name, int batchSize, FilterFunction fun) {
        super(name, batchSize);
        this.fun = fun;
    }
}
