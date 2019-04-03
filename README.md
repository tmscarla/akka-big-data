# Akka - Big Data

## Overview
This repo contains an implementation in Java of a Big Data (batch and stream) processing engine using Akka actors. The engine accepts programs that define an arbitrary acyclic graph of operators. Each operator takes in input <Key, Value> pairs where both Key and Value are strings. 

- The framework takes care of instantiating **multiple workers** (actors) to perform each operator in parallel on different data partitions
- Input data is made available by a single **Source** node and output data is consumed by a single **Sink** node
- A **Master** node supervises all the worker actors. The Master, the Source and the Sink cannot fail, while workers can fail at any time

## Quickstart
The platform has been developed both for scaling up (on multiple processors) and for scaling out (on different physical machines). The easiest way to get started with the platform is to run one or more Collaborator nodes and a Starter (main) node. 

# Introduction


## Structure


## Collaborators

- [Matteo Moreschini](https://github.com/teomores)
- [Alessio Russo Introito](https://github.com/russointroitoa)
