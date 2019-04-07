# Akka - Big Data

## Overview
This repo contains an implementation in Java of a Big Data (batch and stream) processing engine using Akka actors. The engine accepts programs that define an arbitrary acyclic graph of operators. Each operator takes in input <Key, Value> pairs where both Key and Value are strings. 

- The framework takes care of instantiating **multiple workers** (actors) to perform each operator in parallel on different data partitions
- Input data is made available by a single **Source** node and output data is consumed by a single **Sink** node
- A **Master** node supervises all the worker actors. The Master, the Source and the Sink cannot fail, while workers can fail at any time

## Quickstart
The platform has been developed both for scaling up (on multiple processors) and for scaling out (on different physical machines). The easiest way to get started with the platform is to run one or more Collaborator nodes and a Starter (main) node in this way. Firstly, move to the akka-project folder and build the project with [Maven](https://maven.apache.org/index.html):

~~~~
cd akka-project
mvn package
~~~~
Collaborator nodes must be started before the Starter node. You can run a Collaborator node in this way:
~~~~
mvn exec:java -Dexec.mainClass="com.gof.akka.Collaborator" -Dexec.args="<IP>:<PORT>"
~~~~
If you want to run the entire platform locally, use localhost or 127.0.0.1 as IP address and choose an unused port. Once you have set up Collaborator(s), run the Starter node in this way:
~~~~
mvn exec:java -Dexec.mainClass="com.gof.akka.HttpServer" -Dexec.args="-s <IP_s>:<PORT_s> -c <IP_c1>:<PORT_c1>,<IP_c2>:<PORT_c2>"
~~~~
Obviously, the list of Collaborators addresses must match the Collaborators effectively launched.
This command will launch the Starter node and an HTTP server on the address <IP_s>:8080 as well. This server is in charge to manage the HTTP requests specified through the REST API of the platform.

# Introduction
Akka is a toolkit for building highly concurrent, distributed, and resilient message-driven applications. Akka is based on the actor model, a mathematical model of concurrent computation that treats actors as the universal primitives of concurrent computation. Actors may modify their own private state, but can only affect each other through messages (avoiding the need for any locks).

<p align="center">
<img src="https://doc.akka.io/docs/akka/current/guide/diagrams/actor_graph.png"/>
</p>

## Structure


## Collaborators

- [Matteo Moreschini](https://github.com/teomores)
- [Alessio Russo Introito](https://github.com/russointroitoa)
