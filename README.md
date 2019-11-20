
## Introduction
[![Build Status](https://travis-ci.org/openmessaging/openmessaging-storage-dledger.svg?branch=master)](https://travis-ci.org/openmessaging/openmessaging-storage-dledger) [![Coverage Status](https://coveralls.io/repos/github/openmessaging/openmessaging-storage-dledger/badge.svg?branch=master)](https://coveralls.io/github/openmessaging/openmessaging-storage-dledger?branch=master) [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

A raft-based java library for building high-available, high-durable, strong-consistent commitlog, which could act as the persistent layer for distributed storage system, i.e. messaging, streaming, kv, db, etc.

Dledger has added many new features that are not described in the [original paper](https://raft.github.io/raft.pdf). It has been proven to be a true production ready product. 


## Features

* Leader election
* High performance, high reliable storage support
* Asynchronous thread model
* Pipeline replication
* Parallel log replication between leader and followers
* Minority nodes failures do not affect availability
* High tolerance of symmetric network partition
* High tolerance of asymmetric network partition
* [Jepsen verification with fault injection](https://github.com/openmessaging/openmessaging-dledger-jepsen)
* Preferred leader election

### New features waiting to be added ###
* State machine 
* Snapshot
* Dynamic membership & configuration change
* Pre-vote protocol
* SSL/TLS support

## Quick Start


### Prerequisite

* 64bit JDK 1.8+;
* Maven 3.2.x

### How to Build

```
mvn clean install -DskipTests
```

### Run Command Line

```
* Get Command Usage

java -jar target/DLedger.jar

* Start DLedger Server

nohup java -jar target/DLedger.jar server &

* Append Data to DLedger

java -jar target/DLedger.jar append -d "Hello World"

* Get Data from DLedger

java -jar target/DLedger.jar get -i 0
```

## Contributing
We always welcome new contributions, whether for trivial cleanups, big new features. We are always interested in adding new contributors. What we look for are series of contributions, good taste and ongoing interest in the project. If you are interested in becoming a committer, please let one of the existing committers know and they can help you walk through the process.

## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
 













