package com.gof.akka.workers;

import java.util.List;

import akka.actor.ActorRef;
import akka.actor.Props;

import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.operators.FlatMapFunction;


public class FlatMapWorker extends Worker {
    private final FlatMapFunction fun;

    public FlatMapWorker(final List<ActorRef> downstream, final int batchSize, final FlatMapFunction fun) {
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
        // Perform Map on received message
        final List<Message> result = fun.process(message.getKey(), message.getVal());

        // Send result messages to downstream workers
        for(Message m : result) {
            final int receiver = Math.abs(m.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(m, self());
        }
    }

    @Override
    protected final void onBatchMessage(BatchMessage batchMessage) {
        // Perform Map on each received message of the batch and add result to batchQueue
        for(Message message : batchMessage.getMessages()) {
            final List<Message> result = fun.process(message.getKey(), message.getVal());

            for(Message m : result) {
                batchQueue.add(m);

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

    public static Props props(List<ActorRef> downstream, final int batchSize, final FlatMapFunction fun) {
        return Props.create(FlatMapWorker.class, downstream, batchSize, fun);
    }


}

