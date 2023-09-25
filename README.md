
## Introduction
[![Build Status](https://www.travis-ci.org/openmessaging/dledger.svg?branch=master)](https://www.travis-ci.org/search/dledger) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.openmessaging.storage/dledger/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Copenmessaging-storage-dledger)  [![Coverage Status](https://coveralls.io/repos/github/openmessaging/openmessaging-storage-dledger/badge.svg?branch=master)](https://coveralls.io/github/openmessaging/openmessaging-storage-dledger?branch=master) [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

A raft-based java library for building high-available, high-durable, strong-consistent commitlog, which could act as the persistent layer for distributed storage system, i.e. messaging, streaming, kv, db, etc.

Dledger has added many new features that are not described in the [original paper](https://raft.github.io/raft.pdf). It has been proven to be a true production ready product. 


## Features

* Leader election
* Preferred leader election
* [Pre-vote protocol](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
* High performance, high reliable storage support
* Parallel log replication between leader and followers
* Asynchronous replication
* State machine
* Multi-Raft
* High tolerance of symmetric network partition
* High tolerance of asymmetric network partition
* [Jepsen verification with fault injection](https://github.com/openmessaging/openmessaging-dledger-jepsen)

### New features waiting to be added ###
* Snapshot (working in progress)
* Dynamic membership & configuration change
* SSL/TLS support

## Quick Start

### Prerequisite

* 64bit JDK 1.8+

* Maven 3.2.x

### How to Build

```
mvn clean install -DskipTests
```

### Run Command Line

#### Help

> Print Help in Command Line

```shell
java -jar example/target/dledger-example.jar
```

#### Appender

**A high-available, high-durable, strong-consistent, append-only log store.**

> Start a Standalone Appender Server

```shell
java -jar example/target/dledger-example.jar appender
```

> Append Data to Appender

```shell
java -jar example/target/dledger-example.jar append -d "Hello World"
```
After this command, you have appended a log which contains "Hello World" to the appender.

> Get Data from Appender

```shell
java -jar example/target/dledger-example.jar get -i 0
```
After this command, you have got the log which contains "Hello World" from the appender.

#### RegisterModel

**A simple multi-register model**

> Start a Standalone RegisterModel Server

```shell
java -jar example/target/dledger-example.jar register
```

> Write Value for a Key

```shell
java -jar example/target/dledger-example.jar write -k 13 -v 31
```

After this command, you have written a key-value pair which is <13, 31> to the register model.

> Read Value for a Key

```shell
java -jar example/target/dledger-example.jar read -k 13
```

After this command, you have read the value 31 for the key 13 from the register model.

## Contributing
We always welcome new contributions, whether for trivial cleanups, big new features. We are always interested in adding new contributors. What we look for are series of contributions, good taste and ongoing interest in the project. If you are interested in becoming a committer, please let one of the existing committers know and they can help you walk through the process.

## License
[Apache License, Version 2.0](https://github.com/openmessaging/openmessaging-storage-dledger/blob/master/LICENSE) Copyright (C) Apache Software Foundation
 
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-storage-dledger.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-storage-dledger?ref=badge_large)












