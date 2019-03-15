package com.gof.akka.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import com.gof.akka.messages.BatchMessage;
import com.gof.akka.operators.AggregateFunction;
import com.gof.akka.messages.Message;


public class AggregateWorker extends Worker {
    private final int windowSize;
    private final int windowSlide;
    private final AggregateFunction fun;
    private final Map<String, List<String>> windows = new HashMap<>();

    public AggregateWorker(final List<ActorRef> downstream, final int batchSize,
                           final int windowSize, final int windowSlide, final AggregateFunction fun) {
        this.downstream = downstream;
        this.batchSize = batchSize;
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
        this.fun = fun;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(Message.class, this::onMessage) //
                .match(BatchMessage.class, this::onBatchMessage) //
                .build();
    }

    @Override
    protected final void onMessage(Message message) {
        final String key = message.getKey();
        final String value = message.getVal();

        List<String> keyWin = windows.get(key);
        if (keyWin == null) {
            keyWin = new ArrayList<>();
            windows.put(key, keyWin);
        }
        keyWin.add(value);

        // If the size is reached
        if (keyWin.size() == windowSize) {
            final Message result = fun.process(key, keyWin);
            final int receiver = Math.abs(result.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(result, self());
            // Slide
            windows.put(key, keyWin.subList(windowSlide, keyWin.size()));
        }
    }


    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {

    }

    static final Props props(List<ActorRef> downstream, int size, int slide, final AggregateFunction fun) {
        return Props.create(AggregateWorker.class, downstream, size, slide, fun);
    }
}
