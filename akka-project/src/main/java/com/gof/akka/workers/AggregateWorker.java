package com.gof.akka.workers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.Props;

import com.gof.akka.messages.BatchMessage;
import com.gof.akka.functions.AggregateFunction;
import com.gof.akka.messages.Message;


public class AggregateWorker extends Worker {
    private final int windowSize;
    private final int windowSlide;
    private final AggregateFunction fun;

    // Uses a map to group messages with the same hash code of the key.
    // If the size is reached, compute aggregation and then slide.
    private final Map<Integer, List<String>> windows = new HashMap<>();

    public AggregateWorker(String color, int stagePos, final List<ActorRef> downstream, final int batchSize,
                            final AggregateFunction fun, final int windowSize, final int windowSlide) {
        super(color, stagePos, downstream, batchSize);
        this.windowSize = windowSize;
        this.windowSlide = windowSlide;
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
        long startTime = System.nanoTime();
        singleRecMsg++;
        recMsg++;
        System.out.println(color + self().path().name() + "(" + stagePos + ") received: " + message);

        // Get key and value of the message
        final String key = message.getKey();
        final String value = message.getVal();

        // List of current values of the window
        List<String> winValues = windows.get(key.hashCode());
        if (winValues == null) {
            winValues = new ArrayList<>();
            windows.put(key.hashCode(), winValues);
        }
        winValues.add(value);

        // If the size is reached
        if (winValues.size() == windowSize) {
            final Message result = fun.process(key, winValues);
            final int receiver = Math.abs(result.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(result, self());
            sentMsg++;

            // Slide window
            windows.put(key.hashCode(), winValues.subList(windowSlide, winValues.size()));
        }
    }


    @Override
    protected void onBatchMessage(BatchMessage batchMessage) {
        long startTime = System.nanoTime();
        recBatches++;
        recMsg += batchMessage.getMessages().size();
        System.out.println(color + self().path().name() + "(" + stagePos + ") received batch: " + batchMessage);

        // For each message in the batch
        for(Message message : batchMessage.getMessages()) {
            // Get key and value of the message
            final String key = message.getKey();
            final String value = message.getVal();

            // List of current values of the window
            List<String> winValues = windows.get(key.hashCode());
            if (winValues == null) {
                winValues = new ArrayList<>();
                windows.put(key.hashCode(), winValues);
            }
            winValues.add(value);

            // If window size is reached
            if (winValues.size() == windowSize) {
                final Message result = fun.process(key, winValues);
                batchQueue.add(result);

                // Slide window
                windows.put(key.hashCode(), winValues.subList(windowSlide, winValues.size()));

                // If batch size is reached
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
        }

        processingBatchTime = avgProcBatchTime(System.nanoTime() - startTime);
    }

    static public final Props props(String color, int stagePos, List<ActorRef> downstream, int batchSize,
                                    final AggregateFunction fun, int size, int slide) {
        return Props.create(AggregateWorker.class, color, stagePos, downstream, batchSize, fun, size, slide);
    }
}
