package com.gof.akka.messages.create;

import akka.actor.Address;

import java.io.Serializable;

public abstract class CreateMsg implements Serializable {
    protected String name;
    protected String color;
    protected int posStage;
    protected boolean isLocal;
    protected Address address;
    protected int batchSize;

    public CreateMsg(String name, String color, int posStage, boolean isLocal, Address address, int batchSize) {
        this.name = name;
        this.color = color;
        this.posStage = posStage;
        this.isLocal = isLocal;
        this.address = address;
        this.batchSize = batchSize;
    }

    public String getName() {
        return name;
    }

    public String getColor() { return color; }

    public int getPosStage() { return posStage; }

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
