package com.gof.akka.messages.create;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class SourceMsg implements Serializable {
    private ActorRef sourceRef;
    private List<ActorRef> downstream;

    public SourceMsg(ActorRef sinkRef) {
        this.sourceRef = sinkRef;
    }

    public SourceMsg(List<ActorRef> downstream) { this.downstream = downstream;}

    public ActorRef getSourceRef() {
        return sourceRef;
    }

    public List<ActorRef> getDownstream() { return downstream; }
}
