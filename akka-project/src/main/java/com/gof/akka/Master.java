package com.gof.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Address;
import akka.remote.RemoteScope;
import akka.actor.Deploy;

import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.*;
import com.gof.akka.operators.FilterFunction;
import com.gof.akka.operators.MapFunction;
import com.gof.akka.workers.MapWorker;
import com.gof.akka.workers.MergeWorker;
import com.gof.akka.workers.SplitWorker;
import com.gof.akka.workers.FilterWorker;

import java.util.ArrayList;
import java.util.List;

public class Master extends AbstractActor {
    // Stage for split operator
    private List<List<ActorRef>> parallelStage = new ArrayList<>();

    // Stage for all other operators
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
        Address address = mapMsg.getAddress();
        int batchSize = mapMsg.getBatchSize();
        MapFunction fun = mapMsg.getFun();
        ActorRef mapWorker;

        // Local or remote deployment
        if(mapMsg.isLocal()) {
            mapWorker = getContext().actorOf(MapWorker.props(oldStage, batchSize, fun));
        } else {
            mapWorker = getContext().actorOf(MapWorker.props(oldStage, batchSize, fun)
                                                .withDeploy(new Deploy(new RemoteScope(address))));
        }

        // Update stage
        updateStage(mapWorker);

    }

    private void onCreateFilterMsg(CreateFilterMsg filterMsg) {
        Address address = filterMsg.getAddress();
        int batchSize = filterMsg.getBatchSize();
        FilterFunction fun = filterMsg.getFun();
        ActorRef filterWorker;

        // Local or remote deployment
        if(filterMsg.isLocal()) {
            filterWorker = getContext().actorOf(FilterWorker.props(oldStage, batchSize, fun));
        } else {
            filterWorker = getContext().actorOf(FilterWorker.props(oldStage, batchSize, fun)
                    .withDeploy(new Deploy(new RemoteScope(address))));
        }

        // Update stage
        updateStage(filterWorker);

    }

    private void onCreateMergeMsg(CreateMergeMsg mergeMsg) {
        Address address = mergeMsg.getAddress();
        int batchSize = mergeMsg.getBatchSize();
        ActorRef mergeWorker;

        // Local or remote deployment
        if(mergeMsg.isLocal()) {
            mergeWorker = getContext().actorOf(MergeWorker.props(oldStage, batchSize));
        } else {
            mergeWorker = getContext().actorOf(MergeWorker.props(oldStage, batchSize)
                            .withDeploy(new Deploy(new RemoteScope(address))));
        }

        // Update stage
        stage.add(mergeWorker);
        if(stage.size() == numMachines) {
            isParallel = true;
        }
        System.out.println("DONE MERGE");

    }

    public void onCreateSplitMsg(CreateSplitMsg splitMsg) {
        Address address = splitMsg.getAddress();
        int batchSize = splitMsg.getBatchSize();
        ActorRef splitWorker;

        // Local or remote deployment
        if(splitMsg.isLocal()) {
            splitWorker = getContext().actorOf(SplitWorker.props(new ArrayList<>(), batchSize));
        } else {
            splitWorker = getContext().actorOf(SplitWorker.props(new ArrayList<>(), batchSize)
                    .withDeploy(new Deploy(new RemoteScope(address))));
        }

        // Update stage
        stage.add(splitWorker);
        if(stage.size() == numMachines) {
            isParallel = true;
        }
        System.out.println("DONE SPLIT");

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
