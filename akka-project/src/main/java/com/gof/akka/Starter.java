package com.gof.akka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import akka.actor.*;

import akka.util.Timeout;
import com.gof.akka.messages.create.*;
import com.gof.akka.messages.source.*;
import com.gof.akka.operators.*;
import com.gof.akka.utils.ConsoleColors;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.omg.CORBA.TIMEOUT;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;


public class Starter {

    public static void main( String[] args ) throws InterruptedException, IOException {
        String starterNodeURI = "akka.tcp://sys@127.0.0.1:6000";
        List<String> collaboratorNodesURI = Arrays.asList("akka.tcp://sys@127.0.0.1:6120",
                                                          "akka.tcp://sys@127.0.0.1:6121");

        starterNode(starterNodeURI, collaboratorNodesURI, Job.jobOne.getOperators());

    }

    public static final ActorSystem starterNode(String starterNodeURI,
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

        // Convert string URIs into suitable addresses
        for(String uri : collaboratorNodesURI) {
            nodesAddr.add(AddressFromURIString.parse(uri));
        }
        int numMachines = nodesAddr.size();

        // Check functions chain
        boolean needMerge = false;
        int numStages = 0;
        for(Operator op : operators) {
            // Found a split, waiting for a matching merge
            if(op instanceof SplitOperator) {
                if(!needMerge) {
                    needMerge = true;
                    numStages += 2;
                } else { // Found two consecutive split
                    System.out.println("Error! Found two consecutive split!");
                    throw new RuntimeException();
                }
            }
            // Found merge
            else if(op instanceof MergeOperator) {
                if(needMerge) { // Split matched
                    needMerge = false;
                    numStages++;
                } else { // Found merge first or two consecutive merge
                    System.out.println("Error! Found merge first or two consecutive merge!");
                    throw new RuntimeException();
                }
            }
            // Other operators
            else {
                if(!needMerge) {
                    numStages++;
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
        final ActorSystem sys = ActorSystem.create("sys", conf);
        System.out.println(ConsoleColors.RESET + "System created on starter node!" );

        // Sink
        List<ActorRef> sink = createSink(sys);
        System.out.println(ConsoleColors.RESET + "Sink created!" );

        // Master (Supervisor)
        final ActorRef master = sys.actorOf(Master.props(numMachines), "master");
        System.out.println(ConsoleColors.RESET + "Master created!" );

        // Set sink as a downstream and change stage
        master.tell(new SinkMsg(sink.get(0)), ActorRef.noSender());
        master.tell(new ChangeStageMsg(), ActorRef.noSender());

        /* OPERATORS */

        // Instantiate operators in reverse order, updating downstream at each stage
        int posStage = numStages + 1;
        boolean needSplit = false;
        boolean countedParallelStage = false;

        // Deep copy of operators before reverse
        List<Operator> reverseOperators = new ArrayList<>();
        for(Operator op : operators) {
            reverseOperators.add(op.clone());
        }
        Collections.reverse(reverseOperators);

        // For each operator in the list
        for(Operator op : reverseOperators) {
            // Count the position of the operator in the stage
            if(!needSplit) {
                posStage--;
                countedParallelStage = false;
            } else {
                if(!countedParallelStage) {
                    posStage--;
                    countedParallelStage = true;
                } else if(op instanceof SplitOperator) {
                    posStage--;
                }
            }

            boolean isLocal;
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
                    // If is first split worker change stage
                    if(i == 0) {
                        master.tell(new ChangeStageMsg(), ActorRef.noSender());
                    }
                    master.tell(new CreateSplitMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize),
                            ActorRef.noSender());
                    needSplit = false;
                }
                // Merge
                else if(op instanceof MergeOperator) {
                    master.tell(new CreateMergeMsg(op.name, color, posStage, isLocal, nodesAddr.get(i), op.batchSize),
                            ActorRef.noSender());
                    needSplit = true;
                    // If is last merge worker change stage
                    if(i == numMachines-1) {
                        master.tell(new ChangeStageMsg(), ActorRef.noSender());
                    }
                }
            }

            // Change stage only if operators are not in parallel, i.e. they are not between split and merge
            if(!needSplit) {
                master.tell(new ChangeStageMsg(), ActorRef.noSender());
            }
        }

        System.out.println(String.format(ConsoleColors.RESET + "Operators created! Total number of stages: %d", numStages));

        /* SOURCE */

        // Source
        final ActorRef source = sys.actorOf(Source.props(), "source");
        master.tell(new SourceMsg(source), source);
        System.out.println(ConsoleColors.RESET + "Source created!");
        /*Thread.sleep(2000);
        source.tell(new ChangeModeSourceMsg(false), ActorRef.noSender());
        Thread.sleep(2000);
        source.tell(new RandSourceMsg(100, 500), ActorRef.noSender());
        Thread.sleep(2000);*/

        return sys;

    }

    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props(), "sink"));
    }

}
