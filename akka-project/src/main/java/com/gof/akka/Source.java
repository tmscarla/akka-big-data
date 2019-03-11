package com.gof.akka;

import akka.actor.ActorRef;

import java.util.List;
import java.util.Random;

import com.gof.akka.messages.Message;

public class Source implements Runnable {
    private final List<ActorRef> downstream;
    private final int sleepTime;
    private final int keySize;
    private final int valueSize;
    private final Random rand = new Random();

    private volatile boolean stop = false;

    public Source(final List<ActorRef> downstream, final int keySize, final int valueSize, final int sleepTime) {
        this.downstream = downstream;
        this.sleepTime = sleepTime;
        this.keySize = keySize;
        this.valueSize = valueSize;
    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            final int key = rand.nextInt(keySize);
            final int value = rand.nextInt(valueSize);
            final int receiver = key % downstream.size();
            final Message<Integer, Integer> msg = new Message<>(key, value);
            System.out.println("Source sending " + msg);
            downstream.get(receiver).tell(msg, ActorRef.noSender());
            try {
                Thread.sleep(sleepTime);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
