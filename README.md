# Mars distributed storage system
This repository contains the implementation of Mars, a lightweight distributed storage system developed as part of a university distributed systems course. The system is built using Java and Gradle, and follows a ring-based architecture with support for node joins, leaves, and replication.

## Table of contents
...

## Installation
To run the project, you’ll need:
- Java 8 or later

- Gradle 6+

## Set up
As stated in the description of the repository, the project is about a ring-based architecture that supports data replication among a network of nodes.\
Such architecture enforces sequential consistency, which is based on 4 parameters:
- N: the replication factor that is, how many nodes should replicate the data.

- R: the number of nodes to reach get quorum.

- W: the number of nodes to reach update quorum.

- T: the maximum allowd time before throwing a timeout.

After setting the parameters accordingly to your test, simply type the following command to run the project:
```bash
gradle run
```

During the execution, the different actors will log their actions in a file specified in the Main class, line 39.\
Such file can be then accessed in order to debug and monitor the execution.

## Authors
Dorijan Di Zepp

Giovanni Francesco Pettinacci