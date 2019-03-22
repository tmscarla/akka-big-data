package com.gof.akka.messages.create;

import akka.actor.Address;


public class CreateSplitMsg extends CreateMsg {

    public CreateSplitMsg(String name, String color, int posStage, boolean isLocal, Address address, int batchSize) {
        super(name, color, posStage, isLocal, address, batchSize);
    }

}
