package com.gof.akka.workers;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.functions.FilterFunction;

import java.util.List;

public class FilterWorker extends Worker {
    private final FilterFunction fun;

    public FilterWorker(String color, int stagePos, final List<ActorRef> downstream, final int batchSize, final FilterFunction fun) {
        super(color, stagePos, downstream, batchSize);
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
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);
        // Evaluate filter predicate
        final Boolean predicateResult = fun.predicate(message.getKey(), message.getVal());
        System.out.println("Evaluated filter");

        // If predicate is true, send message to downstream worker
        if(predicateResult) {
            final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(message, self());
        }

    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);
        // Evaluate predicate on each received message of the batch and add result to batchQueue
        for(Message message : batchMessage.getMessages()) {
            final Boolean predicateResult = fun.predicate(message.getKey(), message.getVal());

            if(predicateResult) {
                batchQueue.add(message);

                if (batchQueue.size() == batchSize) {
                    // Use the key of the first message to determine the right partition
                    final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                    downstream.get(receiver).tell(new BatchMessage(batchQueue), self());

                    // Empty queue
                    batchQueue.clear();
                }
            }
        }
    }

    public static Props props(String color, int stagePos, List<ActorRef> downstream,
                              final int batchSize, final FilterFunction fun) {
        return Props.create(FilterWorker.class, color, stagePos, downstream, batchSize, fun);
    }
}
