#!/bin/bash

cd ../;
mvn clean install -DskipTests;
mv example/target/register-dledger.jar jepsen/;
rm jepsen/dledger-jepsen.tar.gz;
chmod +x jepsen/startup.sh;
chmod +x jepsen/stop.sh;
chmod +x jepsen/stop_dropcaches.sh;
tar zcvf jepsen/dledger-jepsen.tar.gz jepsen/register-dledger.jar jepsen/startup.sh jepsen/stop.sh jepsen/stop_dropcaches.sh