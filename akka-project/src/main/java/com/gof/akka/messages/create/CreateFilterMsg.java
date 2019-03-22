package com.gof.akka.messages.create;

import akka.actor.Address;
import com.gof.akka.functions.FilterFunction;

import java.io.Serializable;

public class CreateFilterMsg extends CreateMsg {
    private FilterFunction fun;

    public CreateFilterMsg(String name, String color, int posStage, boolean isLocal,
                           Address address, int batchSize, final FilterFunction fun) {
        super(name, color, posStage, isLocal, address, batchSize);
        this.fun = fun;
    }

    public FilterFunction getFun() {
        return fun;
    }

    @Override
    public String toString() {
        return ""; //TODO
    }

}
