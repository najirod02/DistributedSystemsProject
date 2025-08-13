# Mars distributed storage system
This repository contains the implementation of a distributed storage system developed as part of the distributed systems course. The system is built using Java and Gradle, and follows a ring-based architecture with support for node joins, leaves and replication.

<div align="center">
    <img src="https://cdn.mos.cms.futurecdn.net/scMkYWkioMGydSSpB2fAJK-1200-80.jpg" width=60%/>
</div>

## Table of contents

- [Installation](#installation)

- [Set up](#set-up)

- [Run the project](#run-the-project)

- [Log structure](#log-structure)

- [Messages list](#messages-list)

- [Authors](#authors)

## Installation
To run the project, you’ll need:
- Java 8 or later

- Gradle 6 or later

You can verify if the required dependencies are already installed, or check immediately after installation, by running:

```bash
java -version
```

```bash
gradle -v
```

The project uses Gradle’s build configuration located in: `app/build.gradle`.

If your environment uses a different Java or Akka version than the one expected by the project, you can update the `versions` and  `dependencies` in that file.

## Set up
As described in the repository overview, this project implements a ring-based architecture that supports data replication across a network of nodes.\
The architecture enforces __sequential consistency, which is controlled by the following four parameters__:

- __N__: the `replication factor` that is, how many nodes should replicate the data.

- __R__: the number of nodes to reach a read (`get`) quorum.

- __W__: the number of nodes to reach a write (`update`) quorum.

- __T__: the maximum allowed time before a `timeout` is triggered.

These parameters can be configured in the source code to adapt the replication behavior to different requirements, all accessible in the file `Node.java` starting right after the class declaration.

```java
private static final int N = 3,//replication factor
                         R = 2,//reading quorum 
                         W = 2,//writing quorum
                         T = 1000,//in millis
```

The project __does not__ enforce any validation on the quorum values. It is the user’s responsibility to ensure the correctness of these parameters by following two simple rules to maintain consistency and availability guarantees:

- Writing quorum W must be greater than half of N: `W > N / 2`.

- The sum of writing and reading quorums must be greater than the replication factor: `W + R > N`.

Failing to meet these conditions may result in compromised data consistency or availability.

Another independent parameter is the output log file name, which by default is set to `test.txt`.\
You can change the log file name by modifying this line in the file `Main.java`. For example:
```java
private static final Logger logger = new Logger(LOGGER_FILE_BASE_PATH + "/my_log.txt");
```

## Run the project
To start the project, simply run:
```bash
gradle run
```
After launching, an interactive terminal menu will appear. By entering the corresponding digit, you can execute a specific unit test.\
The available tests are designed not only to verify the basic operations—such as join, leave, update, and get—but also to explore edge cases that challenge the system’s guarantees. These include scenarios like concurrent writes to validate sequential consistency, quorum failures, timeouts, and node crashes. This ensures the architecture is tested under both normal and adverse conditions.

Each executed test will produce log entries in the file defined in the Main class (you can change it as discussed in [Set up](#set-up)).\
All logs are stored inside the `logs folder`.

These logs provide detailed insights into the actions and behaviors of all actors in the system, making them useful for both monitoring and debugging.

## Log structure
In this project, logs are the __primary source of debugging and monitoring information__.
Unlike traditional System.out printing in Java, Akka’s actor-based concurrency model often results in non-deterministic execution order, making __console output difficult to interpret__.\
Instead, all actions are timestamped and written to a dedicated log file.

Each log entry follows a structured format:
```plain
[HH:MM.SSS] <actor> <message>
```

- __Timestamp__, elapsed time since the start of the simulation, in minutes, seconds, and milliseconds.

- __Actor__, the unique identifier of the actor generating the log entry. This is typically appended with its current state (one of the enums defined in the Node class, like join, leave, etc).

- __Message__, a descriptive string detailing the event or operation. This usually indicates the type of action being executed or that has just completed.

## Messages list
Given the large number of messages exchanged between Client and Node, a dedicated document listing each message type along with a brief description of its purpose has been written.

The list is organized into categories for the Node and the Client, making it easy to navigate. This reference is especially useful when analyzing logs or extending the system, as it allows quick identification of what each message does and when it is used.

You can find the complete list here: [Messages.md](./app/src/main/java/mars/README.md)

## Authors
| **Student**                    |                 **Email**             |
|--------------------------------|---------------------------------------|
| Dorijan Di Zepp                | dorijan.dizepp@studenti.unitn.it      |
| Giovanni Francesco Pettinacci  | giovanni.pettinacci@studenti.unitn.it |