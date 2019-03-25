package com.gof.akka;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Collaborator {
    public static void main( String[] args ) throws InterruptedException, IOException {
        String configPath = "conf/collaborator2.conf";
        collaboratorNode(configPath);
    }

    public static final void collaboratorNode(String configPath) {
        /* INITIALIZATION */

        // Define system with configuration loaded from configPath
        final Config conf = ConfigFactory.parseFile(new File(configPath));
        final ActorSystem sys = ActorSystem.create("sys", conf);
        System.out.println( "System created!" );

    }

}


