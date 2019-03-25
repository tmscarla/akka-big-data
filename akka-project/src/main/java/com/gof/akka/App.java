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
        firstTry();
    }

    public static final void firstTry() {
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


    private static final Source createSource(final List<ActorRef> downstream, final String filePath) {
        final Source src = new Source(downstream, filePath);
        new Thread(src).start();
        return src;
    }


    private static final List<ActorRef> createSink(final ActorSystem sys) {
        return Collections.singletonList(sys.actorOf(Sink.props(), "sink"));
    }
}
