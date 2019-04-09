package com.gof.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Identify;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Collaborator {
    public static void main( String[] args ) throws InterruptedException, IOException {

        // If address ip:port is passed as a command line argument
        if (args.length > 0) {
            String[] addr = args[0].split(":");
            String ip = addr[0];
            String port = addr[1];

            // Load standard configuration
            final Config nodeConf = ConfigFactory.parseFile(new File("conf/node.conf"));

            // Configure collaborator
            Config collab = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port +"\n"
                    + "akka.remote.netty.tcp.hostname=" + ip);

            // Merge configurations
            Config combined = collab.withFallback(nodeConf);
            Config complete = ConfigFactory.load(combined);
            collaboratorNode(complete);
        }

        // Otherwise load defaults
        else {
            String configPath = "conf/collaborator.conf";
            Config collabConfig = ConfigFactory.parseFile(new File(configPath));
            collaboratorNode(collabConfig);
        }

    }

    public static final void collaboratorNode(Config collabConfig) {

        /* INITIALIZATION */

        final ActorSystem sys = ActorSystem.create("sys", collabConfig);
        System.out.println( "System created on collaborator node!");

    }

}


