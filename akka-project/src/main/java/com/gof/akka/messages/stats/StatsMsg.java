package com.gof.akka.messages.stats;

import java.io.Serializable;

public class StatsMsg implements Serializable {
    private String name;
    private int stagePos;
    private int recMsg;
    private int sentMsg;
    private int recBatches;
    private int sentBatches;
    private long processingTime;
    private long processingBatchTime;

    public StatsMsg(String name, int stagePos, int recMsg, int sentMsg, int recBatches, int sentBatches,
                    long processingTime, long processingBatchTime) {
        this.name = name;
        this.stagePos = stagePos;
        this.recMsg = recMsg;
        this.sentMsg = sentMsg;
        this.recBatches = recBatches;
        this.sentBatches = sentBatches;
        this.processingTime = processingTime;
        this.processingBatchTime = processingBatchTime;
    }

    public String getName() {
        return name;
    }

    public int getStagePos() {
        return stagePos;
    }

    public int getRecMsg() {
        return recMsg;
    }

    public int getSentMsg() {
        return sentMsg;
    }

    public int getRecBatches() {
        return recBatches;
    }

    public int getSentBatches() {
        return sentBatches;
    }

    public long getProcessingTime() {
        return processingTime;
    }

    public long getProcessingBatchTime() {
        return processingBatchTime;
    }
}
