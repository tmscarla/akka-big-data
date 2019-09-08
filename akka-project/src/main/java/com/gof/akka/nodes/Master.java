package com.gof.akka.nodes;

import akka.actor.*;
import akka.dispatch.PriorityGenerator;
import akka.dispatch.UnboundedStablePriorityMailbox;
import akka.japi.pf.DeciderBuilder;
import akka.remote.RemoteScope;

import com.gof.akka.messages.create.*;
import com.gof.akka.messages.stats.GetWorkersMsg;
import com.gof.akka.workers.*;
import scala.concurrent.duration.Duration;
import com.typesafe.config.Config;

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

    private List<ActorRef> children;

    public Master(int numMachines) {
        this.numMachines = numMachines;

        for (int i = 0; i < numMachines; i++) {
            parallelStage.add(new ArrayList<>());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder() //
                .match(GetWorkersMsg.class, this::onGetWorkersMsg) //
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

        // Update children workers
        children = new ArrayList<>();
        for (ActorRef a : getContext().getChildren())
            children.add(a);
    }

    private void onGetWorkersMsg(GetWorkersMsg message) {
        try {
            getSender().tell(new GetWorkersMsg(children), getSelf());
        } catch (Exception e) {
            getSender().tell(new akka.actor.Status.Failure(e), getSelf());
            throw e;
        }
    }

    /* CREATE WORKERS */

    private void onCreateMapMsg(CreateMapMsg mapMsg) {
        ActorRef mapWorker;

        // Local or remote deployment
        if (mapMsg.isLocal()) {
            mapWorker = getContext().actorOf(MapWorker.props(mapMsg.getColor(),
                    mapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mapMsg.getBatchSize(),
                    mapMsg.getFun()).withMailbox("recover-mailbox"), mapMsg.getName());
        } else {
            mapWorker = getContext().actorOf(MapWorker.props(mapMsg.getColor(),
                    mapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mapMsg.getBatchSize(),
                    mapMsg.getFun()).withMailbox("recover-mailbox")
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
                    flatMapMsg.getFun()).withMailbox("recover-mailbox"), flatMapMsg.getName());
        } else {
            flatMapWorker = getContext().actorOf(FlatMapWorker.props(flatMapMsg.getColor(),
                    flatMapMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    flatMapMsg.getBatchSize(),
                    flatMapMsg.getFun()).withMailbox("recover-mailbox")
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
                    aggMsg.getWindowSlide()).withMailbox("recover-mailbox"), aggMsg.getName());
        } else {
            aggWorker = getContext().actorOf(AggregateWorker.props(aggMsg.getColor(),
                    aggMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    aggMsg.getBatchSize(),
                    aggMsg.getFun(),
                    aggMsg.getWindowSize(),
                    aggMsg.getWindowSlide()).withMailbox("recover-mailbox")
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
                    filterMsg.getFun()).withMailbox("recover-mailbox"), filterMsg.getName());
        } else {
            filterWorker = getContext().actorOf(FilterWorker.props(filterMsg.getColor(),
                    filterMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    filterMsg.getBatchSize(),
                    filterMsg.getFun()).withMailbox("recover-mailbox")
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
                    splitMsg.getBatchSize()).withMailbox("recover-mailbox"), splitMsg.getName());
        } else {
            splitWorker = getContext().actorOf(SplitWorker.props(splitMsg.getColor(),
                    splitMsg.getPosStage(),
                    pStage,
                    splitMsg.getBatchSize()).withMailbox("recover-mailbox")
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
                    mergeMsg.getBatchSize()).withMailbox("recover-mailbox"), mergeMsg.getName());
        } else {
            mergeWorker = getContext().actorOf(MergeWorker.props(mergeMsg.getColor(),
                    mergeMsg.getPosStage(),
                    stageDeepCopy(oldStage),
                    mergeMsg.getBatchSize()).withMailbox("recover-mailbox")
                    .withDeploy(new Deploy(new RemoteScope(mergeMsg.getAddress()))), mergeMsg.getName());

        }

        // Update stage
        stage.add(mergeWorker);
        if (stage.size() == numMachines) {
            isParallel = true;
        }

    }

    /* STAGES MANAGER */

    private void updateStage(ActorRef actorRef) {
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

    /* PROPS */

    public static Props props(int numMachines) {
        return Props.create(Master.class, numMachines);
    }


}
