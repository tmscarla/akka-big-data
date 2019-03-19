package com.gof.akka.messages.create;

import akka.actor.ActorRef;

import java.io.Serializable;

public class SinkMsg implements Serializable {
    private ActorRef sinkRef;

    public SinkMsg(ActorRef sinkRef) {
        this.sinkRef = sinkRef;
    }

    public ActorRef getSinkRef() {
        return sinkRef;
    }
}
