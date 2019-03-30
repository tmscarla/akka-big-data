package com.gof.akka.operators;

public class SplitOperator extends Operator {
    public SplitOperator(String name, int batchSize) {
        super(name, batchSize);
    }

    @Override
    public SplitOperator clone(){
        return new SplitOperator(this.name, this.batchSize);
    }
}
