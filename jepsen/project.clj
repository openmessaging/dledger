; Licensed to the Apache Software Foundation (ASF) under one or more
; contributor license agreements.  See the NOTICE file distributed with
; this work for additional information regarding copyright ownership.
; The ASF licenses this file to You under the Apache License, Version 2.0
; (the "License"); you may not use this file except in compliance with
; the License.  You may obtain a copy of the License at
;
;     http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.


(defproject openmessaging-dledger-jepsen "0.0.1-SNAPSHOT"
  :descritopn "A jepsen test for DLedger"
  :license {:name "Apache License 2.0"}
  :main io.openmessaging.storage.dledger.jepsen.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.15-SNAPSHOT"]
                 [io.openmessaging.storage/dledger-example "0.3.3-SNAPSHOT"]]
  :aot [io.openmessaging.storage.dledger.jepsen.core]
  :source-paths ["src" "src/main/clojure"]
  :jar-name "openmessaging-dledger-jepsen.jar"
  :uberjar-name "openmessaging-dledger-jepsen-exec.jar")