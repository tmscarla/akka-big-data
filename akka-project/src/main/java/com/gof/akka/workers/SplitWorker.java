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
    public void onMessage(Message message) {
        long startTime = System.nanoTime();
        singleRecMsg++;
        recMsg++;
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);

        // Simulate crash
        simulateCrash(100);

        // For each operator forward message to the right worker
        for(int i=0; i < downstream.get(0).size(); i++) {
            final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).get(i).tell(message, self());
            sentMsg++;
        }

        processingTime = avgProcTime(System.nanoTime() - startTime);
    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        long startTime = System.nanoTime();
        recBatches++;
        recMsg += batchMessage.getMessages().size();
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);

        // Simulate crash
        simulateCrash(100);

        for(Message message : batchMessage.getMessages()) {
            batchQueue.add(message);

            if(batchQueue.size() == batchSize) {
                for(int i=0; i < downstream.get(0).size(); i++) {
                    final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                    downstream.get(receiver).get(i).tell(new BatchMessage(batchQueue), self());
                    sentBatches++;
                    sentMsg += batchSize;
                }

                // Empty queue
                batchQueue.clear();
            }
        }

        processingBatchTime = avgProcBatchTime(System.nanoTime() - startTime);
    }

    public static Props props(String color, int stagePos, List<List<ActorRef>> downstream, final int batchSize) {
        return Props.create(SplitWorker.class, color, stagePos, downstream, batchSize);
    }

}
