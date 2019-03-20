package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Address;
import akka.remote.RemoteScope;
import akka.actor.Deploy;

import com.gof.akka.functions.AggregateFunction;
import com.gof.akka.messages.create.*;
import com.gof.akka.functions.FilterFunction;
import com.gof.akka.functions.MapFunction;
import com.gof.akka.functions.FlatMapFunction;
import com.gof.akka.workers.*;

import java.util.ArrayList;
import java.util.List;

public class Master extends AbstractActor {
    // Stage for split operator
    private List<List<ActorRef>> parallelStage = new ArrayList<>();

    // Stage for all other functions
    private List<ActorRef> stage = new ArrayList<>();
    private List<ActorRef> oldStage = new ArrayList<>();

    private boolean isParallel = false;
    private int numMachines = 1;
    private int currentMachine = 0;

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
        String name = mapMsg.getName();
        Address address = mapMsg.getAddress();
        int batchSize = mapMsg.getBatchSize();
        MapFunction fun = mapMsg.getFun();
        ActorRef mapWorker;

        // Local or remote deployment
        if(mapMsg.isLocal()) {
            mapWorker = getContext().actorOf(MapWorker.props(oldStage, batchSize, fun), name);
        } else {
            mapWorker = getContext().actorOf(MapWorker.props(oldStage, batchSize, fun)
                                                .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        updateStage(mapWorker);

    }

    private void onCreateFlatMapMsg(CreateFlatMapMsg flatMapMsg) {
        String name = flatMapMsg.getName();
        Address address = flatMapMsg.getAddress();
        int batchSize = flatMapMsg.getBatchSize();
        FlatMapFunction fun = flatMapMsg.getFun();
        ActorRef flatMapWorker;

        // Local or remote deployment
        if(flatMapMsg.isLocal()) {
            flatMapWorker = getContext().actorOf(FlatMapWorker.props(oldStage, batchSize, fun), name);
        } else {
            flatMapWorker = getContext().actorOf(FlatMapWorker.props(oldStage, batchSize, fun)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        updateStage(flatMapWorker);
    }

    private void onCreateAggMsg(CreateAggMsg aggMsg) {
        String name = aggMsg.getName();
        Address address = aggMsg.getAddress();
        int batchSize = aggMsg.getBatchSize();
        AggregateFunction fun = aggMsg.getFun();
        int windowSize = aggMsg.getWindowSize();
        int windowSlide = aggMsg.getWindowSlide();
        ActorRef aggWorker;

        // Local or remote deployment
        if(aggMsg.isLocal()) {
            aggWorker = getContext().actorOf(AggregateWorker.props(oldStage, batchSize, fun, windowSize, windowSlide), name);
        } else {
            aggWorker = getContext().actorOf(AggregateWorker.props(oldStage, batchSize, fun, windowSize, windowSlide)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        updateStage(aggWorker);
    }

    private void onCreateFilterMsg(CreateFilterMsg filterMsg) {
        String name = filterMsg.getName();
        Address address = filterMsg.getAddress();
        int batchSize = filterMsg.getBatchSize();
        FilterFunction fun = filterMsg.getFun();
        ActorRef filterWorker;

        // Local or remote deployment
        if(filterMsg.isLocal()) {
            filterWorker = getContext().actorOf(FilterWorker.props(oldStage, batchSize, fun), name);
        } else {
            filterWorker = getContext().actorOf(FilterWorker.props(oldStage, batchSize, fun)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        updateStage(filterWorker);

    }

    private void onCreateMergeMsg(CreateMergeMsg mergeMsg) {
        String name = mergeMsg.getName();
        Address address = mergeMsg.getAddress();
        int batchSize = mergeMsg.getBatchSize();
        ActorRef mergeWorker;

        // Local or remote deployment
        if(mergeMsg.isLocal()) {
            mergeWorker = getContext().actorOf(MergeWorker.props(oldStage, batchSize), name);
        } else {
            mergeWorker = getContext().actorOf(MergeWorker.props(oldStage, batchSize)
                            .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        stage.add(mergeWorker);
        if(stage.size() == numMachines) {
            isParallel = true;
        }

    }

    public void onCreateSplitMsg(CreateSplitMsg splitMsg) {
        String name = splitMsg.getName();
        Address address = splitMsg.getAddress();
        int batchSize = splitMsg.getBatchSize();
        ActorRef splitWorker;

        // Local or remote deployment
        if(splitMsg.isLocal()) {
            splitWorker = getContext().actorOf(SplitWorker.props(new ArrayList<>(), batchSize), name);
        } else {
            splitWorker = getContext().actorOf(SplitWorker.props(new ArrayList<>(), batchSize)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
        }

        // Update stage
        stage.add(splitWorker);
        if(stage.size() == numMachines) {
            isParallel = true;
        }

    }

    public void updateStage(ActorRef actorRef) {
        if(isParallel) {
            parallelStage.get(currentMachine).add(actorRef);
            if(parallelStage.size() == numMachines) {
                currentMachine++;
            }

        } else {
            stage.add(actorRef);
        }
    }

    public static Props props(int numMachines) {
        return Props.create(Master.class, numMachines);
    }


}
