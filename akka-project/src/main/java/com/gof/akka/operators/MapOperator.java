package com.gof.akka.operators;

import com.gof.akka.functions.MapFunction;

public class MapOperator extends Operator {
    public MapFunction fun;

    public MapOperator(String name, int batchSize, MapFunction fun) {
        super(name, batchSize);
        this.fun = fun;
    }

    @Override
    public MapOperator clone() {
        return new MapOperator(this.name, this.batchSize, this.fun);
    }
}
