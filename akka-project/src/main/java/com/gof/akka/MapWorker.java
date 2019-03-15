package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.gof.akka.messages.Message;
import com.gof.akka.operators.MapFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapWorker extends AbstractActor {
    private final ActorRef downstream;
    private final MapFunction fun;

    public MapWorker(final ActorRef downstream, final MapFunction fun) {
        this.downstream = downstream;
        this.fun = fun;
    }

    private final void onMessage(Message message) {
        // Get message key and value
        final String key = message.getKey();
        final String value = message.getVal();

        // Perform Map
        final Message result = fun.process(key, value);

        // Send result to downstream worker
        downstream.tell(result, self());

        // final int receiver = Math.abs(result.getKey().hashCode()) % downstream.size();
        // downstreamWorkers.get(receiver).tell(result, self());

    }

    @Override
    public Receive createReceive() {
        return null;
    }

    public static final Props props() {
        return Props.create(MapWorker.class);
    }


}
