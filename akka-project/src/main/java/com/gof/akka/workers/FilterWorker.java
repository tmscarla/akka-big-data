package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.operators.FilterFunction;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.List;

public class FilterWorker extends Worker {
    private final FilterFunction fun;

    public FilterWorker(final List<ActorRef> downstream, final int batchSize, final FilterFunction fun) {
        this.downstream = downstream;
        this.batchSize = batchSize;
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
        // Evaluate filter predicate
        final Boolean predicateResult = fun.predicate(message.getKey(), message.getVal());

        // If predicate is true, send message to downstream worker
        if(predicateResult) {
            final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(message, self());
        }

    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        // Perform Map on each received message of the batch and add result to batchQueue
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

    public static Props props(List<ActorRef> downstream, final int batchSize, final FilterFunction fun) {
        return Props.create(FilterWorker.class, downstream, batchSize, fun);
    }
}
