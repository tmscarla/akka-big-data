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

import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.*;
import com.gof.akka.operators.AbstractFunction;
import com.gof.akka.operators.AggregateFunction;
import com.gof.akka.operators.MapFunction;
import com.sun.javaws.exceptions.InvalidArgumentException;
import scala.None;

public class App {

    public static void main( String[] args ) throws InterruptedException, IOException {
        // Define the system where actors actually live
        final ActorSystem sys = ActorSystem.create("System");
        System.out.println( "System created!" );

        // Sink
        List<ActorRef> sink = createSink(sys);
        System.out.println( "Sink created!" );

        // Master (Supervisor)
        final ActorRef master = sys.actorOf(Master.props(1), "master");
        System.out.println( "Master created!" );

        // Set sink as a downstream
        master.tell(new SinkMsg(sink.get(0)), ActorRef.noSender());

        // Operators

        MapFunction mapFun = (String key, String value) -> {return new Message("ale", "val");};

        Operator op = new Operator("map", 10, mapFun);


        master.tell(new ChangeStageMsg(), ActorRef.noSender());




        master.tell(new CreateMapMsg(true,
                                    new Address("akka.tcp", "sys", "host", 1234),
                                    op.batchSize,
                                    (MapFunction)op.fun), ActorRef.noSender());


        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        master.tell(new CreateFilterMsg(true, new Address("akka.tcp", "sys", "host", 1234),
                10, (String key, String value) -> true), ActorRef.noSender());
        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        // Source
        final ActorRef source = sys.actorOf(Source.props(), "source");
        master.tell(new SourceMsg(source), source);




        /*
        To allow dynamically deployed systems, it is also possible to include deployment configuration
        in the Props which are used to create an actor: this information is the equivalent of a deployment
        section from the configuration file, and if both are given, the external configuration takes precedence.

        Address addr = new Address("akka.tcp", "sys", "host", 1234);
        ActorRef ref = sys.actorOf(Props.create(Sink.class).withDeploy(new Deploy(new RemoteScope(addr))));
        */
        Address addr = new Address("akka.tcp", "sys", "host", 1234);
    }


    public static final void starterNode(ArrayList<String> collaboratorNodesURI, List<Operator> operators) {
        // Generate list of suitable addresses for collaborator nodes
        ArrayList<Address> collabAddresses = new ArrayList<Address>();
        for(String uri : collaboratorNodesURI) {
            collabAddresses.add(AddressFromURIString.parse(uri));
        }

        // Check operators chain
        boolean needMerge = false;
        for(Operator op : operators) {
            // Found a split, waiting for a matching merge
            if(op.name.equals("split") && !needMerge) {
                needMerge = true;
            }
            // Found two consecutive split
            else if(op.name.equals("split") && !needMerge) {
                throw new RuntimeException();
            }
            // Split matched by a merge!
            else if(op.name.equals("merge") && needMerge) {
                needMerge = false;
            }
            // Found merge first or two consecutive merge
            else if(op.name.equals("merge") && !needMerge) {
                throw new RuntimeException();
            }
        }
        // If not all split operators are matched by a merge operator
        if(needMerge) {
            throw new RuntimeException();
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


    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props()));
    }
}
