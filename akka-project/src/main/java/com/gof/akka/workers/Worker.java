package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.utils.ConsoleColors;

import java.util.ArrayList;
import java.util.List;

public abstract class Worker extends AbstractActor {
    protected String color = ConsoleColors.WHITE;
    protected int stagePos;
    protected List<ActorRef> downstream = new ArrayList<>();
    protected int batchSize = 10;
    protected List<Message> batchQueue = new ArrayList<>();

    public Worker() {}

    public Worker(String color, int stagePos, List<ActorRef> downstream, int batchSize) {
        this.color = color;
        this.stagePos = stagePos;
        this.downstream = downstream;
        this.batchSize = batchSize;
    }

    protected abstract void onMessage(Message message);

    protected abstract void onBatchMessage(BatchMessage batchMessage);
}
