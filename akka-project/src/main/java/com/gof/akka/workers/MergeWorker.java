package com.gof.akka.workers;

import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;

public class MergeWorker extends Worker {

    public MergeWorker(List<ActorRef> downstream, int batchSize) {
        this.downstream = downstream;
        this.batchSize = batchSize;
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create() //
                .match(Message.class, this::onMessage)
                .match(BatchMessage.class, this::onBatchMessage)
                .build();
    }

    @Override
    protected void onMessage(Message message) {
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);
        // Send result to downstream worker
        final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
        downstream.get(receiver).tell(message, self());
    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);
        // Accumulate messages
        for(Message message : batchMessage.getMessages()) {
            batchQueue.add(message);

            if(batchQueue.size() == batchSize) {
                // Use the key of the first message to determine the right partition
                final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                downstream.get(receiver).tell(new BatchMessage(batchQueue), self());

                // Empty queue
                batchQueue.clear();
            }
        }
    }

    public static Props props(List<ActorRef> downstream, final int batchSize) {
        return Props.create(MapWorker.class, downstream, batchSize);
    }
}
