package com.gof.akka.messages.create;

import akka.actor.ActorRef;
import akka.actor.Address;

import java.io.Serializable;
import java.util.List;

public class CreateSplitMsg implements Serializable {
    private boolean isLocal;
    private Address address;

    private List<List<ActorRef>> downstream;
    private int batchSize;

    public CreateSplitMsg(boolean isLocal, Address address, int batchSize) {
        this.isLocal = isLocal;
        this.address = address;
        this.batchSize = batchSize;
    }

    public boolean isLocal() { return isLocal; }

    public Address getAddress() {
        return address;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
