
# Introduction
A unified messaging and streaming storage.
It is append-only, and only has two simple Apis:

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


##### Get Command Uasage

```
java -jar target/DLeger.jar
```
##### Start DLeger Server

```
nohup java -jar target/DLeger.jar server &
```
##### Append Data to DLeger

```
java -jar target/DLeger.jar append -d "Hello World"
```
##### Get Data from DLeger

```
java -jar target/DLeger.jar get -i 0
```













