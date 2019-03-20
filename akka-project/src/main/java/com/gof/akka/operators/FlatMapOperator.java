package com.gof.akka.operators;

import com.gof.akka.functions.FlatMapFunction;


public class FlatMapOperator extends Operator {
    public FlatMapFunction fun;

    public FlatMapOperator(String name, int batchSize) {
        super(name, batchSize);
        this.fun = fun;
    }
}
