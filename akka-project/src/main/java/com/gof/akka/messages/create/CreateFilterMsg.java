package com.gof.akka.messages.create;

import akka.actor.Address;
import com.gof.akka.functions.FilterFunction;

import java.io.Serializable;

public class CreateFilterMsg extends CreateMsg {
    private FilterFunction fun;

    public CreateFilterMsg(String name, boolean isLocal, Address address, int batchSize, final FilterFunction fun) {
        super(name, isLocal, address, batchSize);
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
