package com.gof.akka.messages.stats;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.List;

public class RequestStatsMsg implements Serializable {
    private String result;
    private ActorRef source;
    private ActorRef sink;
    private List<ActorRef> workers;

    // Request
    public RequestStatsMsg(ActorRef source, ActorRef sink, List<ActorRef> workers) {
        this.source = source;
        this.sink = sink;
        this.workers = workers;
    }

    // Response
    public RequestStatsMsg(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }

    public ActorRef getSource() {
        return source;
    }

    public ActorRef getSink() {
        return sink;
    }

    public List<ActorRef> getWorkers() {
        return workers;
    }

}
