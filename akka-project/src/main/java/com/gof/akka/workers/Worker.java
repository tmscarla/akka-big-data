package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.utils.ConsoleColors;

import java.util.ArrayList;
import java.util.List;

public abstract class Worker extends AbstractActor {
    public String color = ConsoleColors.WHITE_BRIGHT;
    public String stage;

    protected List<ActorRef> downstream = new ArrayList<>();
    protected int batchSize = 10;
    protected List<Message> batchQueue = new ArrayList<>();

    protected abstract void onMessage(Message message);

    protected abstract void onBatchMessage(BatchMessage batchMessage);
}
