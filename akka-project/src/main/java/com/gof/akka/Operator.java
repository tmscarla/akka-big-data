package com.gof.akka;

import com.gof.akka.operators.AbstractFunction;

public class Operator {
    public final String name;
    public final int batchSize;
    public final AbstractFunction fun;

    public Operator(String name, int batchSize, AbstractFunction fun) {
        this.name = name;
        this.batchSize = batchSize;
        this.fun = fun;
    }

}
