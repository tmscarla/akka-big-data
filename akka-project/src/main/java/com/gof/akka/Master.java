package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Address;
import akka.remote.RemoteScope;
import akka.actor.Deploy;

import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.CreateMapMsg;
import com.gof.akka.operators.MapFunction;
import com.gof.akka.workers.MapWorker;

import java.util.ArrayList;
import java.util.List;

public class Master extends AbstractActor {
    private ArrayList<ActorRef> children = new ArrayList<>();

    // OneForOneStrategy: the strategy applies only to the crashed child
    // OneForAllStrategy: the strategy applies to all children

    // We apply the strategy at most 10 times in a window of 10 seconds.
    // If in the last 10 seconds the child crashed more than 10 times, then
    // we do not apply any strategy anymore.

    // We can also select a different strategy for each type of exception/error.

    // Possible strategies: resume, restart, stop, escalate.
    // Try them out!


    public ArrayList<ActorRef> getChildren() {
        return children;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(CreateMapMsg.class, this::onCreateMapMsg)
                .build();
    }

    private void onCreateMapMsg(CreateMapMsg mapMsg) {
        Address address = mapMsg.getAddress();
        List<ActorRef> downstream = mapMsg.getDownstream();
        int batchSize = mapMsg.getBatchSize();
        MapFunction fun = mapMsg.getFun();

        // Local or remote deployment
        if(mapMsg.getIsLocal()) {
            ActorRef mapWorker = getContext().actorOf(MapWorker.props(downstream, batchSize, fun));
            children.add(mapWorker);
            mapWorker.tell(new Message("ciao", "pippo"), self());
        } else {
            ActorRef mapWorker = getContext().actorOf(MapWorker.props(downstream, batchSize, fun)
                                                .withDeploy(new Deploy(new RemoteScope(address))));
            children.add(mapWorker);
        }

        System.out.println("DONE");

    }

    public static Props props() {
        return Props.create(Master.class);
    }


}
