package com.gof.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static akka.http.javadsl.server.Directives.route;

import akka.http.javadsl.marshallers.jackson.Jackson;

import com.gof.akka.messages.source.*;
import com.gof.akka.utils.ConsoleColors;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


class HttpServer extends HttpApp {
    // Actor System on the starter node
    private static ActorSystem system;
    private static String starterNodeURI = "akka.tcp://sys@127.0.0.1:6000";
    private static List<String> collaboratorNodesURI = Arrays.asList("akka.tcp://sys@127.0.0.1:6120",
                                                                     "akka.tcp://sys@127.0.0.1:6121");

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Starting the system
        System.out.println(ConsoleColors.RESET + "Starting the system...");
        system = Starter.starterNode(starterNodeURI, collaboratorNodesURI, Job.jobTwo.getOperators());

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
        // SOURCE
        return concat(pathPrefix("source", () ->
                concat(
                        // RESUME
                        path("resume", () -> get(() -> {
                            try {
                                ActorRef sourceRef = getSourceFromSystem(system);
                                sourceRef.tell(new ResumeSourceMsg(), ActorRef.noSender());
                                return complete("Source resumed!\n");
                            } catch (Exception e) {
                                return complete("Exception!");
                            }
                        })),

                        // SUSPEND
                        path("suspend", () -> get(() -> {
                            try {
                                ActorRef sourceRef = getSourceFromSystem(system);
                                sourceRef.tell(new SuspendSourceMsg(), ActorRef.noSender());
                                return complete("Source suspended!\n");
                            } catch (Exception e) {
                                return complete("Exception!");
                            }
                        })),

                        // MODE ['STREAM' | 'BATCH']
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

                        // RANDOM SOURCE
                        path("random", () -> post(() ->  entity(
                                Jackson.unmarshaller(RandomSource.class), randomSource -> {
                                    try {
                                        // Stop source thread
                                        ActorRef sourceRef = getSourceFromSystem(system);
                                        sourceRef.tell(new StopSourceMsg(), ActorRef.noSender());
                                        Thread.sleep(2000);

                                        // New random source
                                        int keySize = randomSource.getKeySize();
                                        int valueSize = randomSource.getValueSize();
                                        sourceRef.tell(new RandSourceMsg(keySize, valueSize), ActorRef.noSender());

                                        return complete("Initialized new random source with: " +
                                                String.format("keySize = %d, valueSize = %d\n", keySize, valueSize));

                                    } catch (Exception e) {
                                        return complete("Exception on initialize random source!");
                                    }
                                }))),

                        // READ FROM CSV
                        path("read", () -> post(() ->  entity(
                                Jackson.unmarshaller(ReadSource.class), readSource -> {
                                    try {
                                        // Stop source thread
                                        ActorRef sourceRef = getSourceFromSystem(system);
                                        sourceRef.tell(new StopSourceMsg(), ActorRef.noSender());
                                        Thread.sleep(2000);

                                        // Load (key, value) pairs from a csv file with two cols
                                        String filePath = readSource.getFilePath();
                                        sourceRef.tell(new ReadSourceMsg(filePath), ActorRef.noSender());

                                        return complete("Initialized new source reading from: " + filePath + "\n");

                                    } catch (Exception e) {
                                        return complete("Exception on initialize csv source!");
                                    }
                                }))),


                        // JOBS
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
                                                    Job.jobThree.getOperators());
                                        }

                                        return complete(String.format("Stopped previous Job. Started Job %d!\n",
                                                jobId.getId()));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        return complete("Exception while submitting new job.");
                                    }
                                }))
                        ))),

                        concat(

                        // STATS
                        path("stats", () -> get(() -> {
                            try {
                                return complete("Stats!\n");
                            } catch (Exception e) {
                                return complete("Exception!");
                            }
                        })))
                );
    }


    /* MODELS */

    private static class JobId {
        private final int id;

        @JsonCreator
        public JobId(@JsonProperty("id") int id) {
            this.id = id;
        }

        int getId() {
            return id;
        }

    }

    private static class SourceMode {
        private final String mode;

        @JsonCreator
        public SourceMode(@JsonProperty("mode") String mode) {
            this.mode = mode;
        }

        String getMode() {
            return mode;
        }
    }

    private static class RandomSource {
        private int keySize;
        private int valueSize;

        @JsonCreator
        public RandomSource(@JsonProperty("keySize") int keySize, @JsonProperty("valueSize") int valueSize) {
            this.keySize = keySize;
            this.valueSize = valueSize;
        }

        int getKeySize() {
            return keySize;
        }

        int getValueSize() {
            return valueSize;
        }

    }

    private static class ReadSource {
        private String filePath;

        @JsonCreator
        public ReadSource(@JsonProperty("filePath") String filePath) {
            this.filePath = filePath;
        }

        String getFilePath() {
            return filePath;
        }
    }
}



