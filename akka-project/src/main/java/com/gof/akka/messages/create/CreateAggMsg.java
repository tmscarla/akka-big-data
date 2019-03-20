package com.gof.akka.messages.create;

import akka.actor.Address;
import com.gof.akka.functions.AggregateFunction;


public class CreateAggMsg extends CreateMsg {
    private AggregateFunction fun;
    private int windowSize;
    private int windowSlide;

    public CreateAggMsg(String name, boolean isLocal, Address address, int batchSize, final AggregateFunction fun,
                        int windowSize, int windowSlide) {
        super(name, isLocal, address, batchSize);
        this.fun = fun;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
    }

    public AggregateFunction getFun() { return fun; }

    public int getWindowSize() { return windowSize; }

    public int getWindowSlide() { return windowSlide; }

    @Override
    public String toString() {
        return ""; //TODO
    }

}
