
# Introduction
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
A raft implementation, a dead simple unified ordered storage, acts as the persistent layer of messaging and streaming system.
It introduces only two major apis:

* append(data)
* get(index)


# Quick Start


### Prerequisite

* 64bit JDK 1.8+;
* Maven 3.2.x

### Build

```
mvn clean install -DskipTests
```

### Run Command Line


##### Get Command Usage

```
java -jar target/DLedger.jar
```
##### Start DLedger Server

```
nohup java -jar target/DLedger.jar server &
```
##### Append Data to DLedger

```
java -jar target/DLedger.jar append -d "Hello World"
```
##### Get Data from DLedger

```
java -jar target/DLedger.jar get -i 0
```













