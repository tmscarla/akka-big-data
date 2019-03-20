package com.gof.akka.messages.create;

import akka.actor.Address;


public class CreateMergeMsg extends CreateMsg {

    public CreateMergeMsg(String name, boolean isLocal, Address address, int batchSize) {
        super(name, isLocal, address, batchSize);
    }

}
