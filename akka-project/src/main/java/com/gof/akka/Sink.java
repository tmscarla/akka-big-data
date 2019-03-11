package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.Props;

import com.gof.akka.messages.Message;

public class Sink<Vin, Kin> extends AbstractActor {
    private String filePath;

    public Sink(String filePath) {
        this.filePath = filePath;
    }

    private final void onMessage(Message<Kin, Vin> message) {
        System.out.println("Sink received: " + message + " --");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(Message.class, this::onMessage) //
                .build();
    }

    static final <Kin, Vin, Kout, Vout> Props props() {
        return Props.create(Sink.class);
    }
}
