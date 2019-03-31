package com.gof.akka;

import akka.actor.*;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedStablePriorityMailbox;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;

import com.gof.akka.functions.AggregateFunction;
import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.*;
import com.gof.akka.functions.FilterFunction;
import com.gof.akka.functions.MapFunction;
import com.gof.akka.functions.FlatMapFunction;
import com.gof.akka.workers.*;
import com.sun.org.apache.bcel.internal.generic.ACONST_NULL;
import scala.concurrent.duration.Duration;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Master extends AbstractActor {

    static class RecoverMailbox extends UnboundedStablePriorityMailbox {
        public RecoverMailbox(ActorSystem.Settings settings, Config config) {
            // Create a new PriorityGenerator, lower priority means more important
            super(
                    new PriorityGenerator() {
                        @Override
                        public int gen(Object message) {
                            if (message.equals("recovered"))
                                return 0; // recovered messages should be put at the end of the queue
                            else return 1; // default are FIFO
                        }
                    });
        }
    }

    // Stage for split operator
    private List<List<ActorRef>> parallelStage = new ArrayList<>();

    // Stage for all other functions
    private List<ActorRef> stage = new ArrayList<>();
    private List<ActorRef> oldStage = new ArrayList<>();

    private boolean isParallel = false;
    private int numMachines = 1;
    private int currentMachine = 0;

    public int count = 0;

    // OneForOneStrategy: the strategy applies only to the crashed child
    // OneForAllStrategy: the strategy applies to all children

    // We apply the strategy at most 10 times in a window of 10 seconds.
    // If in the last 10 seconds the child crashed more than 10 times, then
    // we do not apply any strategy anymore.

    // We can also select a different strategy for each type of exception/error.

    // Possible strategies: resume, restart, stop, escalate.
    // Try them out!

    public Master(int numMachines) {
        this.numMachines = numMachines;

        for (int i = 0; i < numMachines; i++) {
            parallelStage.add(new ArrayList<>());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(ChangeStageMsg.class, this::onChangeStage) //
                .match(SourceMsg.class, this::onReceiveSource) //
                .match(SinkMsg.class, this::onReceiveSink) //
                .match(CreateMapMsg.class, this::onCreateMapMsg) //
                .match(CreateFlatMapMsg.class, this::onCreateFlatMapMsg) //
                .match(CreateAggMsg.class, this::onCreateAggMsg) //
                .match(CreateFilterMsg.class, this::onCreateFilterMsg) //
                .match(CreateMergeMsg.class, this::onCreateMergeMsg) //
                .match(CreateSplitMsg.class, this::onCreateSplitMsg) //
                .build();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new OneForOneStrategy(//
                -1, //
                Duration.Inf(), //
                DeciderBuilder //
                        .match(RuntimeException.class, ex -> SupervisorStrategy.restart()) //
                        .build());
    }

    private void onChangeStage(ChangeStageMsg changeStageMsg) {
        oldStage = stage;
        stage = new ArrayList<>();
    }

    private void onReceiveSink(SinkMsg sinkMsg) {
        stage = new ArrayList<>();
        stage.add(sinkMsg.getSinkRef());
    }

    private void onReceiveSource(SourceMsg sourceMsg) {
        sender().tell(new SourceMsg(oldStage), self());
    }

    private void onCreateMapMsg(CreateMapMsg mapMsg) {
        ActorRef mapWorker;

        // Local or remote deployment
        if (mapMsg.isLocal()) {
            mapWorker = getContext().actorOf(MapWorker.props(mapMsg.getColor(),
                    mapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mapMsg.getBatchSize(),
                    mapMsg.getFun()).withMailbox(""), mapMsg.getName());
        } else {
            mapWorker = getContext().actorOf(MapWorker.props(mapMsg.getColor(),
                    mapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mapMsg.getBatchSize(),
                    mapMsg.getFun())
                    .withDeploy(new Deploy(new RemoteScope(mapMsg.getAddress()))), mapMsg.getName());
        }

        // Update stage
        updateStage(mapWorker);

    }

    private void onCreateFlatMapMsg(CreateFlatMapMsg flatMapMsg) {
        ActorRef flatMapWorker;

        // Local or remote deployment
        if (flatMapMsg.isLocal()) {
            flatMapWorker = getContext().actorOf(FlatMapWorker.props(flatMapMsg.getColor(),
                    flatMapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    flatMapMsg.getBatchSize(),
                    flatMapMsg.getFun()), flatMapMsg.getName());
        } else {
            flatMapWorker = getContext().actorOf(FlatMapWorker.props(flatMapMsg.getColor(),
                    flatMapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    flatMapMsg.getBatchSize(),
                    flatMapMsg.getFun())
                    .withDeploy(new Deploy(new RemoteScope(flatMapMsg.getAddress()))), flatMapMsg.getName());
        }

        // Update stage
        updateStage(flatMapWorker);
    }

    private void onCreateAggMsg(CreateAggMsg aggMsg) {
        ActorRef aggWorker;

        // Local or remote deployment
        if (aggMsg.isLocal()) {
            aggWorker = getContext().actorOf(AggregateWorker.props(aggMsg.getColor(),
                    aggMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    aggMsg.getBatchSize(),
                    aggMsg.getFun(),
                    aggMsg.getWindowSize(),
                    aggMsg.getWindowSlide()), aggMsg.getName());
        } else {
            aggWorker = getContext().actorOf(AggregateWorker.props(aggMsg.getColor(),
                    aggMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    aggMsg.getBatchSize(),
                    aggMsg.getFun(),
                    aggMsg.getWindowSize(),
                    aggMsg.getWindowSlide())
                    .withDeploy(new Deploy(new RemoteScope(aggMsg.getAddress()))), aggMsg.getName());

        }

        // Update stage
        updateStage(aggWorker);
    }

    private void onCreateFilterMsg(CreateFilterMsg filterMsg) {
        ActorRef filterWorker;

        // Local or remote deployment
        if (filterMsg.isLocal()) {
            filterWorker = getContext().actorOf(FilterWorker.props(filterMsg.getColor(),
                    filterMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    filterMsg.getBatchSize(),
                    filterMsg.getFun()), filterMsg.getName());
        } else {
            filterWorker = getContext().actorOf(FilterWorker.props(filterMsg.getColor(),
                    filterMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    filterMsg.getBatchSize(),
                    filterMsg.getFun())
                    .withDeploy(new Deploy(new RemoteScope(filterMsg.getAddress()))), filterMsg.getName());
        }

        // Update stage
        updateStage(filterWorker);

    }

    public void onCreateSplitMsg(CreateSplitMsg splitMsg) {
        ActorRef splitWorker;

        // Deep copy for parallel stage
        List<List<ActorRef>> pStage = new ArrayList<>();
        for (List<ActorRef> list : parallelStage) {
            pStage.add(new ArrayList<>());
            for (ActorRef actor : list) {
                pStage.get(pStage.size() - 1).add(actor);
            }
        }

        // Local or remote deployment
        if (splitMsg.isLocal()) {
            splitWorker = getContext().actorOf(SplitWorker.props(splitMsg.getColor(),
                    splitMsg.getPosStage(),
                    pStage,
                    splitMsg.getBatchSize()), splitMsg.getName());
        } else {
            splitWorker = getContext().actorOf(SplitWorker.props(splitMsg.getColor(),
                    splitMsg.getPosStage(),
                    pStage,
                    splitMsg.getBatchSize())
                    .withDeploy(new Deploy(new RemoteScope(splitMsg.getAddress()))), splitMsg.getName());
        }

        // Update stage
        stage.add(splitWorker);
        if (stage.size() == numMachines) {
            isParallel = false;
            for (int i = 0; i < numMachines; i++) {
                parallelStage.get(i).clear();
            }
        }
    }

    private void onCreateMergeMsg(CreateMergeMsg mergeMsg) {
        ActorRef mergeWorker;

        // Local or remote deployment
        if (mergeMsg.isLocal()) {
            mergeWorker = getContext().actorOf(MergeWorker.props(mergeMsg.getColor(),
                    mergeMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mergeMsg.getBatchSize()), mergeMsg.getName());
        } else {
            mergeWorker = getContext().actorOf(MergeWorker.props(mergeMsg.getColor(),
                    mergeMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mergeMsg.getBatchSize())
                    .withDeploy(new Deploy(new RemoteScope(mergeMsg.getAddress()))), mergeMsg.getName());

        }

        // Update stage
        stage.add(mergeWorker);
        if (stage.size() == numMachines) {
            isParallel = true;
        }

    }


    public void updateStage(ActorRef actorRef) {
        if (isParallel) {
            parallelStage.get(currentMachine).add(actorRef);
            currentMachine++;
            if (currentMachine == numMachines) {
                currentMachine = 0;
            }
        } else {
            stage.add(actorRef);
        }
    }

    private List<ActorRef> stageDeepCopy(List<ActorRef> stage) {
        List<ActorRef> newStage = new ArrayList<>();
        for (ActorRef actor : stage) {
            newStage.add(actor);
        }
        return newStage;
    }

    public static Props props(int numMachines) {
        return Props.create(Master.class, numMachines);
    }


}
