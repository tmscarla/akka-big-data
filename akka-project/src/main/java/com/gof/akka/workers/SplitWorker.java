package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;

import java.util.List;

public class SplitWorker extends Worker {

    private List<List<ActorRef>> downstream;

    public SplitWorker(List<List<ActorRef>> downstream, int batchSize) {
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
    public void onMessage(Message message) {
        // For each group of workers of the same operator
        for(List<ActorRef> workers : downstream) {
            // Send result to downstream worker
            final int receiver = Math.abs(message.getKey().hashCode()) % workers.size();
            workers.get(receiver).tell(message, self());
        }
    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        for(Message message : batchMessage.getMessages()) {
            batchQueue.add(message);

            if(batchQueue.size() == batchSize) {
                // For each group of workers of the same operator
                for(List<ActorRef> workers : downstream) {

                    // Use the key of the first message of the batch to determine the right partition
                    final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % workers.size();
                    workers.get(receiver).tell(new BatchMessage(batchQueue), self());

                    // Empty queue
                    batchQueue.clear();
                }
            }
        }
    }

}
