package com.gof.akka.messages.create;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import akka.actor.ActorRef;
import akka.actor.Address;
import com.gof.akka.operators.MapFunction;


public class CreateMapMsg implements Serializable {
    private boolean isLocal;
    private Address address;

    private int batchSize;
    private MapFunction fun;

    public CreateMapMsg(boolean isLocal, Address address, int batchSize, final MapFunction fun) {
        super();
        this.isLocal = isLocal;
        this.address = address;
        this.batchSize = batchSize;
        this.fun = fun;
    }

    public boolean isLocal() { return isLocal; }

    public Address getAddress() {
        return address;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public MapFunction getFun() {
        return fun;
    }

    @Override
    public String toString() {
        return ""; //TODO
    }

}
