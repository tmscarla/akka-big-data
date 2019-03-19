package com.gof.akka.messages.create;

import akka.actor.Address;
import com.gof.akka.operators.FilterFunction;

import java.io.Serializable;

public class CreateFilterMsg implements Serializable {
    private boolean isLocal;
    private Address address;

    private int batchSize;
    private FilterFunction fun;

    public CreateFilterMsg(boolean isLocal, Address address, int batchSize, final FilterFunction fun) {
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

    public FilterFunction getFun() {
        return fun;
    }

    @Override
    public String toString() {
        return ""; //TODO
    }

}
