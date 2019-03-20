package com.gof.akka.messages.create;

import akka.actor.Address;

import java.io.Serializable;

public abstract class CreateMsg implements Serializable {
    protected String name;
    protected boolean isLocal;
    protected Address address;
    protected int batchSize;

    public CreateMsg(String name, boolean isLocal, Address address, int batchSize) {
        this.name = name;
        this.isLocal = isLocal;
        this.address = address;
        this.batchSize = batchSize;
    }

    public String getName() {
        return name;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public Address getAddress() {
        return address;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
