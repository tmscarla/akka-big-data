package com.gof.akka.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import com.gof.akka.messages.BatchMessage;
import com.gof.akka.messages.Message;
import com.gof.akka.messages.stats.RequestStatsMsg;
import com.gof.akka.messages.stats.StatsMsg;
import com.gof.akka.utils.ConsoleColors;

import java.util.ArrayList;
import java.util.List;

public abstract class Worker extends AbstractActor {
    protected String color = ConsoleColors.WHITE;
    protected int stagePos;
    protected List<ActorRef> downstream = new ArrayList<>();

    protected int batchSize = 10;
    protected List<Message> batchQueue = new ArrayList<>();

    protected int singleRecMsg = 0;
    protected int recMsg = 0;
    protected int sentMsg = 0;
    protected int recBatches = 0;
    protected int sentBatches = 0;
    protected long processingTime = 0;
    protected long processingBatchTime = 0;

    public Worker() {}

    public Worker(String color, int stagePos, List<ActorRef> downstream, int batchSize) {
        this.color = color;
        this.stagePos = stagePos;
        this.downstream = downstream;
        this.batchSize = batchSize;
    }

    protected long avgProcTime(long time) {
        if (processingTime == 0) {
            return time;
        } else {
            return ((processingTime * (singleRecMsg - 1)) + time) / singleRecMsg;
        }
    }

    protected long avgProcBatchTime(long time) {
        if (processingBatchTime == 0) {
            return time;
        } else {
            return ((processingBatchTime * (recBatches-1)) + time) / recBatches;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(Message.class, this::onMessage) //
                .match(BatchMessage.class, this::onBatchMessage) //
                .match(RequestStatsMsg.class, this::onRequestStatsMsg) //
                .build();
    }

    protected void onRequestStatsMsg(RequestStatsMsg message) {
        try {
            StatsMsg result = new StatsMsg(self().path().name(), stagePos, recMsg, sentMsg,
                    recBatches, sentBatches, processingTime, processingBatchTime);
            getSender().tell(result, getSelf());
        } catch (Exception e) {
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
            throw e;
        }
    }

    protected abstract void onMessage(Message message);

    protected abstract void onBatchMessage(BatchMessage batchMessage);
}
