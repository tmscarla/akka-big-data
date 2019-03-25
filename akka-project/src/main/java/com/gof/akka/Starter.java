package com.gof.akka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import akka.actor.ActorSystem;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.AddressFromURIString;

import com.gof.akka.messages.Message;
import com.gof.akka.messages.create.*;
import com.gof.akka.functions.MapFunction;
import com.gof.akka.operators.*;
import com.gof.akka.utils.ConsoleColors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class Starter {
    public static void main( String[] args ) throws InterruptedException, IOException {
        String starterNodeURI = "akka.tcp://sys@127.0.0.1:6000";
        List<String> collaboratorNodesURI = Arrays.asList("akka.tcp://sys@127.0.0.1:6120",
                "akka.tcp://sys@127.0.0.1:6121");

        List<Operator> operators = new ArrayList<>();
        Operator map = new MapOperator("Map", 10,
                (String k, String v) -> new Message(k, v+"remoto"));
        Operator filter = new FilterOperator("Filter", 10,
                (String k, String v) -> {if(Integer.parseInt(k) % 2 == 0) {
                    return true; } else { return false; }
                });

        operators.add(map);
        operators.add(filter);

        starterNode(starterNodeURI, collaboratorNodesURI, operators);
    }

    public static final void starterNode(String starterNodeURI,
                                         List<String> collaboratorNodesURI,
                                         List<Operator> operators) throws InterruptedException {
        /* INITIALIZATION */

        // Colors
        List<String> colors = Arrays.asList(ConsoleColors.RESET, ConsoleColors.RED_BRIGHT,
                ConsoleColors.BLUE_BRIGHT, ConsoleColors.YELLOW_BRIGHT,
                ConsoleColors.GREEN_BRIGHT, ConsoleColors.PURPLE_BRIGHT);

        // List of addresses of all nodes. First is starter.
        ArrayList<Address> nodesAddr = new ArrayList<>();
        nodesAddr.add(AddressFromURIString.parse(starterNodeURI));

        for(String uri : collaboratorNodesURI) {
            nodesAddr.add(AddressFromURIString.parse(uri));
        }
        int numMachines = nodesAddr.size();

        // Check functions chain
        boolean needMerge = false;
        for(Operator op : operators) {
            // Found a split, waiting for a matching merge
            if(op instanceof SplitOperator) {
                if(!needMerge) {
                    needMerge = true;
                } else { // Found two consectuive split
                    throw new RuntimeException();
                }
            }

            // Found merge
            if(op instanceof MergeOperator) {
                if(needMerge) { // Split matched
                    needMerge = false;
                } else { // Found merge first or two consecutive merge
                    throw new RuntimeException();
                }
            }

        }
        // If not all split functions are matched by a merge operator
        if(needMerge) {
            throw new RuntimeException();
        }

        /* SYSTEM AND MAIN NODES */

        // System, where actors actually live
        final Config conf = ConfigFactory.parseFile(new File("conf/starter.conf"));
        final ActorSystem sys = ActorSystem.create("sys");
        System.out.println( "System created!" );

        // Sink
        List<ActorRef> sink = createSink(sys);
        System.out.println( "Sink created!" );

        // Master (Supervisor)
        final ActorRef master = sys.actorOf(Master.props(numMachines), "master");
        System.out.println( "Master created!" );

        // Set sink as a downstream and change stage
        master.tell(new SinkMsg(sink.get(0)), ActorRef.noSender());
        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        /* OPERATORS */

        // For each operator
        int posStage = 0;
        for(Operator op : operators) {
            posStage++;
            boolean isLocal;
            needMerge = false;
            String rootName = op.name;

            // Instantiate a worker on each machine
            for(int i=0; i < numMachines; i++) {
                // Check if is the starter node and deploy locally, otherwise remotely
                if(i==0) {
                    isLocal = true;
                } else {
                    isLocal = false;
                }

                // Set color and name
                String color = colors.get(i % colors.size());
                op.name = rootName + "-" + Integer.toString(i+1);

                // Map
                if(op instanceof MapOperator) {
                    master.tell(new CreateMapMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize,
                            ((MapOperator) op).fun), ActorRef.noSender());
                }
                // FlatMap
                else if(op instanceof FlatMapOperator) {
                    master.tell(new CreateFlatMapMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize,
                            ((FlatMapOperator) op).fun), ActorRef.noSender());
                }
                // Filter
                else if(op instanceof FilterOperator) {
                    master.tell(new CreateFilterMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize,
                            ((FilterOperator) op).fun), ActorRef.noSender());
                }
                // Aggregate
                else if(op instanceof AggregateOperator) {
                    master.tell(new CreateAggMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize,
                            ((AggregateOperator) op).fun, ((AggregateOperator) op).windowSize,
                            ((AggregateOperator) op).windowSlide), ActorRef.noSender());
                }
                // Split
                else if(op instanceof SplitOperator) {
                    master.tell(new CreateSplitMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize),
                            ActorRef.noSender());
                    needMerge = true;
                }
                // Merge
                else if(op instanceof MergeOperator) {
                    master.tell(new CreateMergeMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize),
                            ActorRef.noSender());
                    needMerge = false;
                }

            }

            // Change stage only if operators are not in parallel, i.e. they are not between split and merge
            if(!needMerge) {
                master.tell(new ChangeStageMsg(), ActorRef.noSender());
            }
        }

        System.out.println(String.format("Operators created! Total number of stages: %d", operators.size()));

        /* SOURCE */

        // Source
        final ActorRef source = sys.actorOf(Source.props(), "source");
        master.tell(new SourceMsg(source), source);

    }

    private static final Source createSource(final List<ActorRef> downstream, final String filePath) {
        final Source src = new Source(downstream, filePath);
        new Thread(src).start();
        return src;
    }


    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props(), "sink"));
    }

}
