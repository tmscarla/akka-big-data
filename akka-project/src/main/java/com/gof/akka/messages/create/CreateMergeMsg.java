package com.gof.akka.messages.create;

import akka.actor.ActorRef;
import akka.actor.Address;

import java.io.Serializable;
import java.util.List;

public class CreateMergeMsg implements Serializable {
    private boolean isLocal;
    private Address address;
    private List<ActorRef> downstream;
    private int batchSize;

    public CreateMergeMsg(boolean isLocal, Address address, List<ActorRef> downstream, int batchSize) {
        this.isLocal = isLocal;
        this.address = address;
        this.downstream = downstream;
        this.batchSize = batchSize;
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
