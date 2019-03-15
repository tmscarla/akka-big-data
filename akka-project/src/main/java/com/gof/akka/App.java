package com.gof.akka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.Deploy;
import akka.remote.RemoteScope;
import akka.actor.AddressFromURIString;

import com.gof.akka.Sink;
import com.gof.akka.Source;
import com.gof.akka.Master;

import com.gof.akka.operators.AggregateFunction;

public class App {

    public static void main( String[] args ) throws InterruptedException, IOException {
        // Define the system where actors actually live
        final ActorSystem sys = ActorSystem.create("System");
        System.out.println( "System created!" );

        // The ActorRef remains valid throughout the lifecycle of the actor even if the actor stops and restarts

        // Master (Supervisor)
        final ActorRef supervisor = sys.actorOf(Master.props(), "master");

        // Source
        //final Source source = createSource(downstream, filePath);

        // Sink
        ActorRef sink = sys.actorOf(Sink.props(), "sink");

        /*
        To allow dynamically deployed systems, it is also possible to include deployment configuration
        in the Props which are used to create an actor: this information is the equivalent of a deployment
        section from the configuration file, and if both are given, the external configuration takes precedence.

        Address addr = new Address("akka.tcp", "sys", "host", 1234);
        ActorRef ref = sys.actorOf(Props.create(Sink.class).withDeploy(new Deploy(new RemoteScope(addr))));
        */
        Address addr = new Address("akka.tcp", "sys", "host", 1234);
    }

    public static final void starterNode(ArrayList<String> collaboratorNodesURI) {
        // Generate list of suitable addresses for collaborator nodes
        ArrayList<Address> collabAddresses = new ArrayList<Address>();
        for(String uri : collaboratorNodesURI) {
            collabAddresses.add(AddressFromURIString.parse(uri));
        }



    }

    public static final void collaboratorNode() {

    }


    private static final Source createSource(final List<ActorRef> downstream, final String filePath) {
        final Source src = new Source(downstream, filePath);
        new Thread(src).start();
        return src;
    }
    /*
    private static final List<ActorRef> createOperator(final int stage, final ActorSystem sys,
                                                       final List<ActorRef> downstream, final int size,
                                                       final int slide, final AggregateFunction fun) {

        return IntStream.range(0, numPartitions). //
                mapToObj(i -> sys.actorOf(Processor.props(downstream, size, slide, fun), "Proc_" + stage + "_" + i)). //
                collect(Collectors.toList());
    }
    */

    private static final void p() {}

    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props()));
    }
}
