package com.gof.akka.nodes;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.gof.akka.messages.stats.RequestStatsMsg;
import com.gof.akka.messages.stats.StatsMsg;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Collector extends AbstractActor {
    private String result;
    private SimpleDateFormat lastDate;
    private Map<Integer, List<StatsMsg>> globalStats;
    private StatsMsg sourceStats;
    private StatsMsg sinkStats;
    private Integer maxStage = 0;

    public Collector() {
    }

    public String getResult() {
        return result;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(RequestStatsMsg.class, this::onRequestStatsMsg) //
                .build();
    }

    public void onRequestStatsMsg(RequestStatsMsg message) {
        // Clean global stats
        globalStats = new HashMap<>();

        // Update date
        lastDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        // Source and sink stats
        try {
            final Future<Object> sourceReply = Patterns.ask(message.getSource(), message, 10000);
            sourceStats = (StatsMsg) Await.result(sourceReply, Duration.Inf());
            final Future<Object> sinkReply = Patterns.ask(message.getSink(), message, 10000);
            sinkStats = (StatsMsg) Await.result(sinkReply, Duration.Inf());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Collect stats from workers
        for(ActorRef worker : message.getWorkers()) {
            final Future<Object> reply = Patterns.ask(worker, message, 10000);
            try {
                StatsMsg statsWorker = (StatsMsg) Await.result(reply, Duration.Inf());
                Integer stagePos = statsWorker.getStagePos();
                if (stagePos > maxStage) {
                    maxStage = stagePos;
                }

                List<StatsMsg> workersOnStage = globalStats.get(stagePos);
                if (workersOnStage == null) {
                    workersOnStage = new ArrayList<>();
                    globalStats.put(stagePos, workersOnStage);
                }
                workersOnStage.add(statsWorker);


            } catch (final Exception e) {
                getSender().tell(new akka.actor.Status.Failure(e), getSelf());
                e.printStackTrace();
            }
        }

        // Convert result in a string format
        String res = "\n\n#===== SYSTEM STATS | " + lastDate + "=====#\n\n";

        // Legend
        res = res.concat("Worker: (sent/rec) | (sentBatch/recBatch) | [avgTime|avgBatchTime]\n\n\n");

        // Source
        res = res.concat(String.format("== SOURCE ==\n\nMessages: %d\nBatches: %d\n\n",
                sourceStats.getSentMsg(), sourceStats.getSentBatches()));

        // Sink
        res = res.concat(String.format("== SINK ==\n\nMessages: %d \nBatches: %d\n\n",
                sinkStats.getRecMsg(), sinkStats.getRecBatches()));

        // Workers
        for(int i=1; i<=maxStage; i++) {
            res = res.concat(String.format("== STAGE %d ==\n\n", i));

            for(StatsMsg msg : globalStats.get(i)) {
                res = res.concat(String.format("%s: (%d/%d) | (%d/%d) | [%f|%f]\n", msg.getName(), msg.getSentMsg(),
                        msg.getRecMsg(), msg.getSentBatches(), msg.getRecBatches(),
                        (float)msg.getProcessingTime()/1000000, (float)msg.getProcessingBatchTime()/1000000));
            }

            res = res.concat("\n");
        }

        // Send result back in a string format
        result = res;
        sender().tell(new RequestStatsMsg(result), self());
    }

    public static final Props props() {
        return Props.create(Collector.class);
    }
}
