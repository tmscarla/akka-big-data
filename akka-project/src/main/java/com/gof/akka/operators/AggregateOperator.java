package com.gof.akka.operators;

import com.gof.akka.functions.AggregateFunction;


public class AggregateOperator extends Operator {
    public AggregateFunction fun;
    public int windowSize;
    public int windowSlide;

    public AggregateOperator(String name, int batchSize, AggregateFunction fun, int windowSize, int windowSlide) {
        super(name, batchSize);
        this.fun = fun;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }
}