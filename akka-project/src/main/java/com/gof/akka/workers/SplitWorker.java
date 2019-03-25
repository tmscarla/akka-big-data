package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;

import java.util.ArrayList;
import java.util.List;

public class SplitWorker extends Worker {

    private List<List<ActorRef>> downstream = new ArrayList<>();

    public SplitWorker(String color, int stagePos, List<List<ActorRef>> downstream, int batchSize) {
        this.color = color;
        this.stagePos = stagePos;
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
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);
        // For each operator forward message to the right worker
        for(int i=0; i < downstream.get(0).size(); i++) {
            final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).get(i).tell(message, self());
        }
    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);
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

    public static Props props(String color, int stagePos, List<List<ActorRef>> downstream, final int batchSize) {
        return Props.create(SplitWorker.class, color, stagePos, downstream, batchSize);
    }

}
