package com.gof.akka.operators;

public class MergeOperator extends Operator {
    public MergeOperator(String name, int batchSize) {
        super(name, batchSize);
    }

    @Override
    public MergeOperator clone() {
        return new MergeOperator(this.name, this.batchSize);
    }
}
