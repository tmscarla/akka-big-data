package com.gof.akka.messages.create;

import akka.actor.Address;


public class CreateSplitMsg extends CreateMsg {

    public CreateSplitMsg(String name, boolean isLocal, Address address, int batchSize) {
        super(name, isLocal, address, batchSize);
    }

}
