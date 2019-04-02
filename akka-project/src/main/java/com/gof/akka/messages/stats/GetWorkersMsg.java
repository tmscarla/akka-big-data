package com.gof.akka.messages.stats;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class GetWorkersMsg implements Serializable {
    List<ActorRef> workers;

    public GetWorkersMsg() {
    }

    public GetWorkersMsg(List<ActorRef> workers) {
        this.workers = workers;
    }

    public List<ActorRef> getWorkers() {
        return workers;
    }
}
