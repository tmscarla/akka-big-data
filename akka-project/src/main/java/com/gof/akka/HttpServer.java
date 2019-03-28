package com.gof.akka;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;

import com.gof.akka.Starter;

import java.util.concurrent.ExecutionException;



// HttpServer definition
class HttpServer extends HttpApp {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Starting the server
        final HttpServer myHttpServer = new HttpServer();
        myHttpServer.startServer("localhost", 8080);

    }

    @Override
    protected Route routes() {
        return path("start", () ->
                // only handle GET requests
                get(() -> {
                    try {
                        Starter.main(null);
                        return complete("PONG!");
                    } catch (Exception e) {
                        return complete("Exception!");
                    }
                }));
    }
}



