package com.gof.akka.operators;

import com.gof.akka.functions.FilterFunction;


public class FilterOperator extends Operator {
    public FilterFunction fun;

    public FilterOperator(String name, int batchSize, FilterFunction fun) {
        super(name, batchSize);
        this.fun = fun;
    }

    @Override
    public FilterOperator clone() {
        return new FilterOperator(this.name, this.batchSize, this.fun);
    }
}
