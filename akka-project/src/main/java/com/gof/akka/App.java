package com.gof.akka;

import akka.actor.ActorSystem;
import java.io.IOException;
import akka.actor.ActorRef;

import com.gof.akka.Sink;
import com.gof.akka.Source;
import com.gof.akka.Master;

public class App {

    public static void main( String[] args ) throws InterruptedException, IOException {
        // Define the system where actors actually live
        final ActorSystem sys = ActorSystem.create("System");
        System.out.println( "System created!" );

        // The ActorRef remains valid throughout the lifecycle of the actor even if the actor stops and restarts

        // Master (Supervisor)

        // Source

        // Sink
        ActorRef sink = sys.actorOf(Sink.props(), "sink");

    }
}
