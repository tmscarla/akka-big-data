package com.gof.akka.messages.create;

import java.io.Serializable;

import akka.actor.Address;
import com.gof.akka.functions.MapFunction;


public class CreateMapMsg extends CreateMsg {
    private MapFunction fun;

    public CreateMapMsg(String name, boolean isLocal, Address address, int batchSize, final MapFunction fun) {
        super(name, isLocal, address, batchSize);
        this.fun = fun;
    }

    public MapFunction getFun() {
        return fun;
    }

    @Override
    public String toString() {
        return ""; //TODO
    }

}

