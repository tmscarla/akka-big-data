package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import akka.actor.Props;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.SourceMsg;
import com.gof.akka.messages.source.*;
import com.gof.akka.utils.ConsoleColors;

public class Source  extends AbstractActor {
    private List<ActorRef> downstream = new ArrayList<>();
    private String sourceFilePath;

    private final Random rand = new Random();
    private boolean loadFromFile = false;
    private int sleepTime = 400;
    private volatile boolean suspended = false;
    private int total = 0;
    private int totalBatches = 0;

    private int batchSize = 3;
    private static boolean batchMode = false;
    private List <Message> batchQueue = new ArrayList<>();
    private volatile boolean running = true;

    public void stop() {
        running = false;
    }

    /* CONSTRUCTORS */

    public Source() {
        super();
    }

    public Source(final List<ActorRef> downstream, String sourceFilePath, int batchSize, int sleepTime) {
        this.downstream = downstream;
        this.sourceFilePath = sourceFilePath;
        this.batchSize = batchSize;
        this.sleepTime = sleepTime;
    }

    /* BEHAVIOR */

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(ChangeModeSourceMsg.class, this::changeMode) //
                .match(SourceMsg.class, this::setDownstream) //
                .match(SuspendSourceMsg.class, this::suspendSource) //
                .match(ResumeSourceMsg.class, this::resumeSource) //
                .match(StopSourceMsg.class, this::stopSource) //
                .match(RandSourceMsg.class, this::onRandomMsg) //
                .match(ReadSourceMsg.class, this::onReadMsg) //
                .build();
    }

    // Set downstream operators when the job is initialized
    private void setDownstream(SourceMsg sourceMsg) {
        this.downstream = sourceMsg.getDownstream();
    }

    // Switch mode from batch to stream and vice versa
    private void changeMode(ChangeModeSourceMsg message) {
        if(message.isBatchMode()) {
            System.out.println(ConsoleColors.RESET + "System switched to batch mode!");
        } else {
            System.out.println(ConsoleColors.RESET + "System switched to stream mode!");
        }
        batchMode = message.isBatchMode();
    }

    private void suspendSource(SuspendSourceMsg message) {
        suspended = true;
    }

    private void resumeSource(ResumeSourceMsg message) {
        suspended = false;
    }

    private void stopSource(StopSourceMsg message) { running = false; }

    // Start sending randomly generated messages
    private void onRandomMsg(RandSourceMsg message) {
        running = true;
        suspended = false;
        new Thread(() -> {
            while(running) {
                if(!suspended) {
                    try {
                        randomMessage(message.getKeySize(), message.getValueSize());
                    } catch (InterruptedException e) {
                        running = false;
                    }
                }
            }
        }).start();
    }

    // Start sending messages read from a source csv file
    private void onReadMsg(ReadSourceMsg message) {
        String a = "/Users/tommasoscarlatti/Desktop/PoliMi/akka-bigdata/akka-project/data/cuba_news.csv";
        running = true;
        suspended = false;
        new Thread(() -> {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(message.getFilePath()));
                String line;
                while(running) {
                    if(!suspended) {
                        try {
                            if((line = reader.readLine()) != null) {
                                readMessage(line);
                            }
                        } catch (IOException | InterruptedException e) {
                            running = false;
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                System.out.println("Source file not found!");
            }
        }).start();

    }

    /* MESSAGE CRAFTING */

    private void randomMessage(int keySize, int valueSize) throws InterruptedException {
        final int keyInt = rand.nextInt(keySize);
        final String key = Integer.toString(keyInt);
        final String value = Integer.toString(rand.nextInt(valueSize));

        final Message msg = new Message(key, value);
        sendMessage(msg);
    }

    private void readMessage(String line) throws InterruptedException {
        String [] content =line.trim().split(",");
        String key = content[0];
        String value = content[1];

        Message msg = new Message(key, value);
        sendMessage(msg);
    }

    private void sendMessage(Message msg) throws InterruptedException {
        // Batch
        if(batchMode) {
            batchQueue.add(msg);
            if(batchQueue.size() == batchSize) {
                BatchMessage batchMsg = new BatchMessage(batchQueue);
                totalBatches++;
                total += batchMsg.getMessages().size();

                System.out.println(ConsoleColors.YELLOW_BOLD_BRIGHT + "Source sending batch " + batchMsg);
                System.out.println(String.format("Total messages: %d", total));
                System.out.println(ConsoleColors.YELLOW_BOLD_BRIGHT + String.format("Total batches: %d", totalBatches));

                final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                downstream.get(receiver).tell(batchMsg, self());
                batchQueue.clear();
                Thread.sleep(sleepTime);
            }
        }
        // Streaming
        else {
            total++;
            System.out.println(ConsoleColors.YELLOW_BOLD_BRIGHT + "Source sending " + msg);
            System.out.println(String.format("Total messages: %d", total));

            final int receiver = Math.abs(msg.getKey().hashCode()) % downstream.size();
            downstream.get(receiver).tell(msg, ActorRef.noSender());
            Thread.sleep(sleepTime);
        }
    }

    /* PROPS */

    static final Props props() {
        return Props.create(Source.class);
    }

}
