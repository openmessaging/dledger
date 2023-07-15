#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# build dledger
cd ../;
mvn clean install -DskipTests;
cp example/target/dledger-example.jar jepsen/node-deploy/;
rm jepsen/dledger-jepsen.tar.gz;
chmod +x jepsen/node-deploy/startup.sh;
chmod +x jepsen/node-deploy/stop.sh;
chmod +x jepsen/node-deploy/stop_dropcaches.sh;
tar zcvf jepsen/dledger-jepsen-node.tar.gz jepsen/node-deploy/dledger-example.jar jepsen/node-deploy/startup.sh jepsen/node-deploy/stop.sh jepsen/node-deploy/stop_dropcaches.sh;

# build jepsen test
cd jepsen;
lein uberjar;
chmod +x jepsen.sh;
cd ../;
tar zcvf jepsen/dledger-jepsen-control.tar.gz jepsen/jepsen.sh jepsen/nodes jepsen/target/openmessaging-dledger-jepsen-exec.jar;