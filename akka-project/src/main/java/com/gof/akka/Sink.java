package com.gof.akka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import akka.actor.AbstractActor;
import akka.actor.Props;

import com.gof.akka.messages.Message;

public class Sink<Vin, Kin> extends AbstractActor {
    private String filePath;
    private Boolean verbose = true;
    private Boolean firstWrite = false;

    public Sink(String filePath) {
        this.filePath = filePath;
    }

    private final void writeMessage(Message<Kin, Vin> message) {
        try (PrintWriter writer = new PrintWriter(new File(filePath))) {

            StringBuilder sb = new StringBuilder();

            // If is first write, write header first
            if(firstWrite) {
                sb.append("key,");
                sb.append(',');
                sb.append("value");
                sb.append('\n');
                firstWrite = false;
            }

            // Write message on csv
            sb.append(String.valueOf(message.getKey()));
            sb.append(',');
            sb.append(String.valueOf(message.getVal()));
            sb.append('\n');

            writer.write(sb.toString());

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    private final void onMessage(Message<Kin, Vin> message) {
        System.out.println("Sink received: " + message + " --");
        writeMessage(message);
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
