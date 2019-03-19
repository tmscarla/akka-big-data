package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import akka.actor.Props;
import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.SourceMsg;

public class Source  extends AbstractActor implements Runnable {
    private List<ActorRef> downstream = new ArrayList<>();
    private String sourceFilePath = "";

    private final Random rand = new Random();

    private volatile boolean stop = false;

    public Source() {
        super();
    }

    public Source(final List<ActorRef> downstream, final String sourceFilePath) {
        this.downstream = downstream;
        this.sourceFilePath = sourceFilePath;
    }

    public void stop() {
        stop = true;
    }

    @Override
    public void run() {
        while (!stop) {
            randomMessage(20, 400, 1);
        }
    }

    // Generate random (key, value) pairs of strings in an incremental way
    private void randomMessage(int keySize, int valueSize, int sleepTime) {
        final int keyInt = rand.nextInt(keySize);
        final String key = Integer.toString(keyInt);
        final String value = Integer.toString(rand.nextInt(valueSize));

        final int receiver = keyInt % downstream.size();
        final Message msg = new Message(key, value);
        System.out.println("Source sending " + msg);
        downstream.get(receiver).tell(msg, ActorRef.noSender());
        try {
            Thread.sleep(sleepTime);
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(SourceMsg.class, this::setDownstream) //
                .build();
    }

    public void setDownstream(SourceMsg sourceMsg) {
        this.downstream = sourceMsg.getDownstream();
        downstream.get(0).tell(new Message("cascas", "zio"), self());
    }

    static final Props props() {
        return Props.create(Source.class);
    }
}
