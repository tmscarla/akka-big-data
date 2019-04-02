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
    protected final void onMessage(Message message) {
        long startTime = System.nanoTime();
        singleRecMsg++;
        recMsg++;
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);

        // Evaluate filter predicate
        final Boolean predicateResult = fun.predicate(message.getKey(), message.getVal());

        // If predicate is true, send message to downstream worker
        if(predicateResult) {
            final int receiver = Math.abs(message.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(message, self());
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

        // Evaluate predicate on each received message of the batch and add result to batchQueue
        for(Message message : batchMessage.getMessages()) {
            final Boolean predicateResult = fun.predicate(message.getKey(), message.getVal());

            if(predicateResult) {
                batchQueue.add(message);

                if (batchQueue.size() == batchSize) {
                    // Use the key of the first message to determine the right partition
                    final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                    downstream.get(receiver).tell(new BatchMessage(batchQueue), self());
                    sentBatches++;
                    sentMsg += batchSize;

                    // Empty queue
                    batchQueue.clear();
                }
            }
        }

        processingBatchTime = avgProcBatchTime(System.nanoTime() - startTime);
    }

    public static Props props(String color, int stagePos, List<ActorRef> downstream,
                              final int batchSize, final FilterFunction fun) {
        return Props.create(FilterWorker.class, color, stagePos, downstream, batchSize, fun);
    }
}
