package com.gof.akka.operators;

import com.gof.akka.functions.FlatMapFunction;


public class FlatMapOperator extends Operator {
    public FlatMapFunction fun;

    public FlatMapOperator(String name, int batchSize, FlatMapFunction fun) {
        super(name, batchSize);
        this.fun = fun;
    }

    @Override
    public FlatMapOperator clone() {
        return new FlatMapOperator(this.name, this.batchSize, this.fun);
    }
}
