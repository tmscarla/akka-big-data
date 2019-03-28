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

    private int batchSize = 3;
    private static boolean batchMode = false;
    private List <Message> batchQueue = new ArrayList<>();

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
                .match(RandSourceMsg.class, this::onRandomMsg) //
                .match(LoadSourceMsg.class, this::onLoadMsg) //
                .build();
    }

    // Set downstream operators when the job is initialized
    private void setDownstream(SourceMsg sourceMsg) {
        this.downstream = sourceMsg.getDownstream();
    }

    // Switch mode from batch to stream and vice versa
    private void changeMode(ChangeModeSourceMsg message) {
        System.out.println(ConsoleColors.RESET + "Mode changed!");
        batchMode = message.isBatchMode();
    }

    private void suspendSource(SuspendSourceMsg message) {
        suspended = true;
    }

    private void resumeSource(ResumeSourceMsg message) {
        suspended = false;
    }

    // Start sending randomly generated messages
    private void onRandomMsg(RandSourceMsg message) {
        new Thread(() -> {
            while(true) {
                while (!suspended) {
                    try {
                        randomMessage(message.getKeySize(), message.getValueSize());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    // Start sending messages read from a source csv file
    private void onLoadMsg(LoadSourceMsg message) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(message.getFilePath()));
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    readMessage(line);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
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
                System.out.println(ConsoleColors.YELLOW_BOLD_BRIGHT + "Source sending batch " + batchMsg);
                final int receiver = Math.abs(batchQueue.get(0).getKey().hashCode()) % downstream.size();
                downstream.get(receiver).tell(batchMsg, self());
                batchQueue.clear();
                Thread.sleep(sleepTime);
            }
        }
        // Streaming
        else {
            System.out.println(ConsoleColors.YELLOW_BOLD_BRIGHT + "Source sending " + msg);
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
