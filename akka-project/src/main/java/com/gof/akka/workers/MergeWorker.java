package com.gof.akka.workers;

import java.util.List;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;

public class MergeWorker extends Worker {

    public MergeWorker(String color, int stagePos, List<ActorRef> downstream, int batchSize) {
        super(color, stagePos, downstream, batchSize);
    }

    @Override
    protected void onMessage(Message message) {
        long startTime = System.nanoTime();
        singleRecMsg++;
        recMsg++;
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);

        // Send result to downstream worker
        final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
        downstream.get(receiver).tell(message, self());
        sentMsg++;

        processingTime = avgProcTime((System.nanoTime() - startTime) / 1000);
    }

    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        long startTime = System.nanoTime();
        recBatches++;
        recMsg += batchMessage.getMessages().size();
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);

        // Accumulate messages
        for(Message message : batchMessage.getMessages()) {
            batchQueue.add(message);

            if(batchQueue.size() == batchSize) {
                // Use the key of the first message to determine the right partition
                final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                downstream.get(receiver).tell(new BatchMessage(batchQueue), self());
                sentBatches++;
                sentMsg += batchSize;

                // Empty queue
                batchQueue.clear();
            }
        }

        processingBatchTime = avgProcBatchTime((System.nanoTime() - startTime) / 1000);
    }

    public static Props props(String color, int stagePos, List<ActorRef> downstream, final int batchSize) {
        return Props.create(MergeWorker.class, color, stagePos, downstream, batchSize);
    }
}
