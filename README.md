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

## Project Structure
Here are listed the main packages of the project:

* **Nodes**: main actors of the system such as: Sink, Source and Master
* **Operators**: operators signatures
  * Map: takes in input a <Key, Value> pair and produces a <Key, Value> pair, according to a user-defined function.
  * FlatMap: takes in input a <Key, Value> pair and produces a set of <Key, Value> pairs, according to a user-defined function.
  * Filter: takes in input a <Key, Value> pair and either propagates it downstream or drops it, according to a user-defined predicate.
  * Aggregate: accumulates n <Key, Value> pairs and produces a single <Key, Value> pair, according to a user-defined aggregate function.
  * Split: forwards the incoming <Key, Value> pairs to multiple downstream operators.
  * Merge: accepts <Key, Value> pairs from multiple operators and forwards them to the downstream operator.
* **Workers**: Actor implementation of each operator
* **Messages**: collection of all the messages exchanged between actors
* **Functions**: interfaces for operators' user-defined functions

# Workflow

## Initialization
On startup the system loads a default Job (a meaningful acyclic graph of operators) and allocates Actors on the different machines involved. From now on we will assume a system with one Starter node and two Collaborator nodes.

* **Starter node**: hosts the main actors, Source, Sink, Master, Collector and a Worker for each Operator.
* **Collaborator node**: hosts a Worker for each Operator.

In this way, for each Operator of the Job we have a corresponding worker on each machine. The actors hierarchy of a sample Job with three Workers (Map, Filter, Aggregate) is the following:

<p align="center">
 <img src="https://github.com/tmscarla/akka-big-data/blob/master/img/actors_hierarchy.png" width=90%>
</p>

Black nodes are allocated on the Starter machine, while colored Workers are allocated on Collaborators.
The **Master** node is in charge of allocating all the Workers within its context, in this way it's able to:
* Choose for local or remote deployment
* Set dinamically the downstream of each Worker
* Handle failures

## Computation: from Source to Sink
The Source node continuosly sends messages of <Key, Value> pairs to its downstream, which is the first stage of Workers of the first Operator. It can operate in two different ways:
* *Random*: crafts randomly generated messages within a specified keySize and valueSize range.
* *Read*: reads rows from a csv file with two columns ['Key', 'Value']

Operators are organized in stages. Normally, each Operator is assigned to a different stage (and its Workers consequently). If there is a Split operator, all the Operators between Split and Merge live in the same stage, i.e. they are in "parallel".
A Split operator duplicates each message for each Operator in its downstream. Each Worker chooses the proper Worker in its downstream using the hashcode of the key. 

When a message reaches the Sink, it is written permanently on a CSV file under the /data folder.

<p align="center">
 <img src="https://github.com/tmscarla/akka-big-data/blob/master/img/workflow.png" width=90%>
</p>

The figure above shows an example of a Job with the following operators in order: Map, Split, Filter, FlatMap, Merge.

The system is made of three different machines: the black one is the Starter node, the red and the blue are Collaborator nodes. For each Operator, a Worker is instantiated on each machine. Vertically aligned workers are on the same stage of the computation. 

## Streaming vs Batch mode
The engine can work into different modes:

* **Streaming**: single messages of <Key, Value> pairs are exchanged between workers. When a Worker receives a message, it processes it according to its specific Operator and then forwards it to its downstream. In this case, the computation is continuos. Each element gets processed as soon as it is available. This can lead to a lower delay but also to a lower throughput.
* **Batch**: the stream is divided into batches of data. Each operator processes a batch at a time and waits until enough data elements are available to fill a batch. Each operator has a *batchSize* attribute that quantifies the size of the buffer. It might introduce some delay.

## Fault tolerance
Akka by defaul, has the following general rules for message sends:
* at-most-once delivery
* message ordering per sender-reciver pair
Further details can be found on [this page](https://doc.akka.io/docs/akka/current/general/message-delivery-reliability.html) of the official documentation.

The platform guarantees an **exactly-once delivery**, meaning that a message sent between two workers can neither be lost nor duplicated. The delivery mechanism relies on the assumption that the message has been successfully sent, received, put in the mailbox, and then processed by the target actor.

When a Worker crashes during a message processing it throws an Exception which is handled by the Master node. The Master restarts the Worker which puts the message which caused the crash in its mailbox again. In this way the message will be re-processed by the Worker. 

In order to guarantee that the processing order is unchanged, each Worker's mailbox is structured as a priority queue. When a message is put back in the mailbox it has the highest priority in the queue. In this way we are guaranteed that it will be processed first.

## REST API
A complete set of examples of the REST API can be found [here](https://github.com/tmscarla/akka-big-data/blob/master/REST-API.txt). Below a detailed explanation of each endpoint:

* **Random source**: make the source generate continuosly random messages with a key and a value included in a *keySize* and a *valueSize* range respectively.
* **Read from CSV**: generate messages reading from a CSV file with two columns: ['Key', 'Value'].
* **Set Job**: clear all the operators instantiated if a previous job was running and then select a new job from the available ones using an id parameter.
* **Batch/Stream**: switch between batch and streaming mode.
* **Suspend/Resume**: suspend or resume source, i.e. stops or starts sending messages.
* **Statistiscs**: print a summary of the statistics of the system.


## Other Collaborators

- [Matteo Moreschini](https://github.com/teomores)
- [Alessio Russo Introito](https://github.com/russointroitoa)
