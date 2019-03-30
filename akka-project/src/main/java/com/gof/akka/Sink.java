package com.gof.akka;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import akka.actor.AbstractActor;
import akka.actor.Props;

import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.utils.ConsoleColors;

import javax.swing.plaf.basic.BasicTabbedPaneUI;

public class Sink extends AbstractActor {
    private String filePath;
    private Boolean verbose = true;
    private Boolean firstWrite = false;

    private int total = 0;
    private int totalBatches = 0;

    public Sink() {}

    public Sink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(Message.class, this::onMessage) //
                .match(BatchMessage.class, this::onBatchMessage) //
                .build();
    }

    private final void onMessage(Message message) {
        System.out.println(ConsoleColors.PURPLE_BOLD_BRIGHT + "Sink received: " + message);
        total++;
        System.out.println(String.format("Total messages: %d", total));
        // writeMessage(message);
    }

    private final void onBatchMessage(BatchMessage batchMessage) {
        System.out.println(ConsoleColors.PURPLE_BOLD_BRIGHT + "Sink received batch: " + batchMessage);
        totalBatches++;
        total += batchMessage.getMessages().size();
        System.out.println(String.format("Total batches: %d", totalBatches));
        System.out.println(String.format("Total messages: %d", total));
    }

    private final void writeMessage(Message message) {
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
            sb.append(message.getKey());
            sb.append(',');
            sb.append(message.getVal());
            sb.append('\n');

            writer.write(sb.toString());

        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
    }

    static final Props props() {
        return Props.create(Sink.class);
    }
}
