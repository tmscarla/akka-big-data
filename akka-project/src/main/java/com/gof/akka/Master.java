package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;

import java.util.ArrayList;

public class Master extends AbstractActor {
    private ArrayList<ActorRef> children = null;


    // OneForOneStrategy: the strategy applies only to the crashed child
    // OneForAllStrategy: the strategy applies to all children

    // We apply the strategy at most 10 times in a window of 10 seconds.
    // If in the last 10 seconds the child crashed more than 10 times, then
    // we do not apply any strategy anymore.

    // We can also select a different strategy for each type of exception/error.

    // Possible strategies: resume, restart, stop, escalate.
    // Try them out!

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }

    public static Props props() {
        return Props.create(Master.class);
    }


}
