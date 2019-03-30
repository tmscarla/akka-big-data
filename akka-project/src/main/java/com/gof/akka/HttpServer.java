package com.gof.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.ContentType;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static akka.http.javadsl.server.Directives.route;

import akka.http.javadsl.common.JsonEntityStreamingSupport;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.marshalling.Marshaller;
import akka.http.javadsl.model.*;
import akka.http.javadsl.model.headers.Accept;
import akka.http.javadsl.server.*;
import akka.http.javadsl.unmarshalling.StringUnmarshallers;
import akka.http.javadsl.common.EntityStreamingSupport;
import akka.http.javadsl.unmarshalling.Unmarshaller;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.gof.akka.Starter;
import com.gof.akka.messages.source.ChangeModeSourceMsg;
import com.gof.akka.messages.source.ResumeSourceMsg;
import com.gof.akka.messages.source.StopSourceMsg;
import com.gof.akka.messages.source.SuspendSourceMsg;
import com.gof.akka.utils.ConsoleColors;
import scala.collection.script.Start;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static akka.http.javadsl.server.Directives.complete;
import static akka.http.javadsl.server.Directives.get;
import static akka.http.javadsl.server.Directives.path;
import static akka.http.javadsl.server.Directives.put;
import static akka.http.javadsl.server.Directives.route;

import static akka.http.javadsl.server.PathMatchers.integerSegment;
import static akka.http.javadsl.server.PathMatchers.segment;


// HttpServer definition
class HttpServer extends HttpApp {
    // Actor System on the starter node
    private static ActorSystem system;
    private static String starterNodeURI = "akka.tcp://sys@127.0.0.1:6000";
    private static List<String> collaboratorNodesURI = Arrays.asList("akka.tcp://sys@127.0.0.1:6120",
            "akka.tcp://sys@127.0.0.1:6121");

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Starting the system
        System.out.println(ConsoleColors.RESET + "Starting the system...");
        system = Starter.starterNode(starterNodeURI, collaboratorNodesURI, Job.jobOne.getOperators());
        System.out.println("done.");

        // Starting the server
        System.out.println(ConsoleColors.RESET + "Server online at http://localhost:8080/");
        final HttpServer myHttpServer = new HttpServer();
        myHttpServer.startServer("localhost", 8080);

    }

    /* UTILS */

    private ActorRef getSourceFromSystem(ActorSystem system) {
        try {
            ActorSelection source = system.actorSelection("/user/source");
            Future<ActorRef> future = source.resolveOne(new FiniteDuration(10, TimeUnit.SECONDS));
            ActorRef sourceRef = Await.result(future, new FiniteDuration(10, TimeUnit.SECONDS));
            return sourceRef;

        } catch (Exception e) {
            System.out.println("Exception looking up source!");
            return null;
        }
    }

    private void terminateActorSystem(ActorSystem system) {
        try {
            Future<Terminated> future = system.terminate();
            Terminated terminated = Await.result(future, new FiniteDuration(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            System.out.println("Exception terminating actor system!");
        }
    }

    /* ROUTES */

    @Override
    protected Route routes() {
        return pathPrefix("source", () ->
                concat(path("resume", () -> get(() -> {
                            try {
                                ActorRef sourceRef = getSourceFromSystem(system);
                                sourceRef.tell(new ResumeSourceMsg(), ActorRef.noSender());
                                return complete("Source resumed!\n");
                            } catch (Exception e) {
                                return complete("Exception!");
                            }
                        })),
                        path("suspend", () -> get(() -> {
                            try {
                                ActorRef sourceRef = getSourceFromSystem(system);
                                sourceRef.tell(new SuspendSourceMsg(), ActorRef.noSender());
                                return complete("Source suspended!\n");
                            } catch (Exception e) {
                                return complete("Exception!");
                            }
                        })),
                        path("mode", () -> post(() ->  entity(
                            Jackson.unmarshaller(SourceMode.class), mode -> {
                                try {
                                    ActorRef sourceRef = getSourceFromSystem(system);
                                    if(mode.getMode().equals("batch")) {
                                        sourceRef.tell(new ChangeModeSourceMsg(true), ActorRef.noSender());
                                        return complete("Source switched to batch mode!\n");
                                    } else {
                                        sourceRef.tell(new ChangeModeSourceMsg(false), ActorRef.noSender());
                                        return complete("Source switched to stream mode!\n");
                                    }
                                } catch (Exception e) {
                                    return complete("Exception!");
                                }
                        }))),
                        path("job", () -> post(() -> entity(
                                Jackson.unmarshaller(JobId.class), jobId -> {
                                    try {
                                        System.out.println(ConsoleColors.RESET + "Stopping previous job...");

                                        // Stop source thread
                                        ActorRef sourceRef = getSourceFromSystem(system);
                                        sourceRef.tell(new StopSourceMsg(), ActorRef.noSender());
                                        Thread.sleep(2000);

                                        // Terminate system
                                        terminateActorSystem(system);
                                        Thread.sleep(3000);

                                        // Start a new job
                                        if(jobId.getId() == 1) {
                                            system = Starter.starterNode(starterNodeURI, collaboratorNodesURI,
                                                    Job.jobOne.getOperators());
                                        } else if(jobId.getId() == 2) {
                                            system = Starter.starterNode(starterNodeURI, collaboratorNodesURI,
                                                    Job.jobTwo.getOperators());
                                        } else {
                                            system = Starter.starterNode(starterNodeURI, collaboratorNodesURI,
                                                    Job.jobTwo.getOperators());
                                        }

                                        return complete(String.format("Stopped previous Job. Started Job %d!\n",
                                                jobId.getId()));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        return complete("Exception while submitting new job.");
                                    }
                                }))
                        ))
        );
    }


    /* MODELS */


    private static class JobId {
        private final int id;

        @JsonCreator
        public JobId(@JsonProperty("id") int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

    }

    private static class SourceMode {
        private final String mode;

        @JsonCreator
        public SourceMode(@JsonProperty("mode") String mode) {
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }
    }

    private static class RandomSource {
        private int keySize;
        private int valueSize;

        public RandomSource(int keySize, int valueSize) {
            this.keySize = keySize;
            this.valueSize = valueSize;
        }

        public int getKeySize() {
            return keySize;
        }

        public int getValueSize() {
            return valueSize;
        }

    }
}



