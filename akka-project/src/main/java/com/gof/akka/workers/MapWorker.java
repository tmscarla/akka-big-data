package com.gof.akka.workers;

import java.util.List;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.Props;

import akka.persistence.AbstractPersistentActor;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.functions.MapFunction;


public class MapWorker extends Worker {
    private final MapFunction fun;

    public MapWorker(String color, int stagePos, final List<ActorRef> downstream,
                     final int batchSize, final MapFunction fun) {
        super(color, stagePos, downstream, batchSize);
        this.fun = fun;
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
        if(message.isPresent()) {
            self().tell(message.get(), self());
        }
    }

    @Override
    protected final void onMessage(Message message) {
        long startTime = System.nanoTime();
        singleRecMsg++;
        recMsg++;

        // FIXME IMPLEMENT CRASH
        if (recMsg % 100 == 0) {
            throw new RuntimeException(color + self().path().name() + " Crashed!");
        } else {
            System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);
            // Perform Map on received message
            final Message result = fun.process(message.getKey(), message.getVal());

            // Send result to downstream worker
            final int receiver = Math.abs(result.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(result, self());
            sentMsg++;
        }

        processingTime = avgProcTime(System.nanoTime() - startTime);
    }

    @Override
    protected final void onBatchMessage(BatchMessage batchMessage) {
        long startTime = System.nanoTime();
        recBatches++;
        recMsg += batchMessage.getMessages().size();
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);

        // Perform Map on each received message of the batch and add result to batchQueue
        for(Message message : batchMessage.getMessages()) {
            final Message result = fun.process(message.getKey(), message.getVal());
            batchQueue.add(result);

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

        processingBatchTime = avgProcBatchTime(System.nanoTime() - startTime);
    }

    public static Props props(String color, int stagePos, List<ActorRef> downstream,
                              final int batchSize, final MapFunction fun) {
        return Props.create(MapWorker.class, color, stagePos, downstream, batchSize, fun);
    }


}
