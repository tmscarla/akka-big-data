package com.gof.akka;

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
import scala.collection.immutable.Stream;

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

        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        master.tell(new CreateMergeMsg("Merge", ConsoleColors.WHITE, 3, true,new Address("akka.tcp", "sys", "host", 1234),
                10), ActorRef.noSender());

        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        master.tell(new CreateFilterMsg("Filter", ConsoleColors.YELLOW_BRIGHT, 2, true, new Address("akka.tcp", "sys", "host", 1234),
                10, (String key, String value) -> true), ActorRef.noSender());

        master.tell(new CreateMapMsg("Map", ConsoleColors.YELLOW_BRIGHT, 2,true,
                        new Address("akka.tcp", "sys", "host", 1234),
                        10,
                        (String k, String v) -> new Message(k+"pippo", v)),
                ActorRef.noSender());

        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        master.tell(new CreateSplitMsg("Split", ConsoleColors.RED_BRIGHT,1, true,new Address("akka.tcp", "sys", "host", 1234),
                10), ActorRef.noSender());

        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        master.tell(new CreateMapMsg("MapIn", ConsoleColors.BLUE_BRIGHT, 0,true,
                        new Address("akka.tcp", "sys", "host", 1234),
                        10,
                        (String k, String v) -> new Message(k+"123", v)),
                ActorRef.noSender());

        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        // Source
        final ActorRef source = sys.actorOf(Source.props(), "source");
        master.tell(new SourceMsg(source), source);


    }


    public static final void starterNode(String starterNodeURI,
                                         ArrayList<String> collaboratorNodesURI,
                                         List<Operator> operators) {
        /* INITIALIZATION */

        // Colors
        List<String> colors = Arrays.asList(ConsoleColors.WHITE_BRIGHT, ConsoleColors.RED_BRIGHT,
                                            ConsoleColors.BLUE_BRIGHT, ConsoleColors.YELLOW_BRIGHT,
                                            ConsoleColors.GREEN_BRIGHT, ConsoleColors.PURPLE_BRIGHT);

        // List of addresses of all nodes. First is starter.
        ArrayList<Address> nodesAddr = new ArrayList<Address>();
        nodesAddr.add(AddressFromURIString.parse(starterNodeURI));

        for(String uri : collaboratorNodesURI) {
            nodesAddr.add(AddressFromURIString.parse(uri));
        }
        int numMachines = nodesAddr.size();

        // Check functions chain
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
        // If not all split functions are matched by a merge operator
        if(needMerge) {
            throw new RuntimeException();
        }

        /* MAIN NODES */

        // System, where actors actually live
        final ActorSystem sys = ActorSystem.create("System");
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

            // Instantiate a worker on each machine
            for(int i=0; i < numMachines; i++) {
                // Check if is the starter node and deploy locally, otherwise remotely
                if(i==0) {
                    isLocal = true;
                } else {
                    isLocal = false;
                }

                // Set color
                String color = colors.get(i % colors.size());

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

                // Change stage only if operators are not in parallel, i.e. they are not between split and merge
                if(!needMerge) {
                    master.tell(new ChangeStageMsg(), ActorRef.noSender());
                }

            }
            System.out.println("Operators created! Total number of stages:");

            /* SOURCE */

            // Source
            final ActorRef source = sys.actorOf(Source.props(), "source");
            master.tell(new SourceMsg(source), source);

        }




    }

    public static final void collaboratorNode() {
        /* INITIALIZATION */

        // Define the system where actors actually live
        final ActorSystem sys = ActorSystem.create("System");
        System.out.println( "System created!" );


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
