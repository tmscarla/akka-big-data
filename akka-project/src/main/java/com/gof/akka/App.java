package com.gof.akka;

import akka.actor.ActorSystem;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.actor.ActorRef;

import com.gof.akka.Sink;
import com.gof.akka.Source;
import com.gof.akka.Master;

public class App {

    public static void main( String[] args ) throws InterruptedException, IOException {
        // Define the system where actors actually live
        final ActorSystem system = ActorSystem.create("System");
        System.out.println( "System created!" );

        // The ActorRef remains valid throughout the lifecycle of the actor even if the actor stops and restarts

        // Master (Supervisor)
        final ActorRef supervisor = system.actorOf(Master.props(), "supervisor");

        // Source
        final Source source = createSource(downstream);

        // Sink
        ActorRef sink = system.actorOf(Sink.props(), "sink");

    }

    private static final Source createSource(final List<ActorRef> downstream) {
        final Source src = new Source(downstream, numKeys, numValues, sleepTime);
        new Thread(src).start();
        return src;
    }

    private static final List<ActorRef> createOperator(final int stage, final ActorSystem sys,
                                                       final List<ActorRef> downstream, final int size,
                                                       final int slide, final AggregateFunction fun) {

        return IntStream.range(0, numPartitions). //
                mapToObj(i -> sys.actorOf(Processor.props(downstream, size, slide, fun), "Proc_" + stage + "_" + i)). //
                collect(Collectors.toList());
    }

    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props()));
    }
}
