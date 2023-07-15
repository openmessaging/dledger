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

(ns io.openmessaging.storage.dledger.jepsen.core
  (:gen-class)
  (:require [clojure.string :as cstr]
            [clojure.tools.logging :refer [info]]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model])
  (:import [io.openmessaging.storage.dledger.example.register.client RegisterDLedgerClient]
           [io.openmessaging.storage.dledger.common ReadMode]))

(defonce dledger-path "/root/jepsen/node-deploy")
(defonce dledger-port 20911)
(defonce dledger-start "startup.sh")
(defonce dledger-stop "stop.sh")
(defonce dledger-stop-dropcaches "stop_dropcaches.sh")
(defonce dledger-data-path "/tmp/dledgerstore")
(defonce dledger-log-path "logs/dledger")

(defonce read-mode (atom "RAFT_LOG_READ"))
(defonce enbale-snapshot (atom false))
(defonce snapshot-threshold (atom 1000))

(defn peer-id [node]
  (str node))

(defn peer-str [node]
  (str (peer-id node) "-" node ":" dledger-port))

(defn peers
  "Constructs an initial cluster string for a test, like
  \"n0-host1:20911;n1-host2:20911,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (peer-str node)))
       (cstr/join ";")))

(defn start! [test node]
  (info "Start DLedgerServer" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-start
                "--group jepsen"
                "--id"
                (peer-id node)
                "--peers"
                (peers test)
                (if @enbale-snapshot
                  (str "--enable-snapshot --snapshot-threshold " @snapshot-threshold)
                  ""))))

(defn stop! [node]
  (info "Stop DLedgerServer" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-stop)))

(defn stop_dropcaches! [node]
  (info "Stop DLedgerServer and drop caches" node)
  (c/cd dledger-path
        (c/exec :sh
                dledger-stop)))

(defn- create-client [test]
  (doto (RegisterDLedgerClient. "jepsen" (peers test))
    (.startup)))

(defn- start-client [client]
  (-> client
      :conn
      (.startup)))

(defn- shutdown-client [client]
  (-> client
      :conn
      (.shutdown)))

(defn- write-value
  "write a key-value to DLedger"
  [client key value]
  (-> client :conn
      (.write key value)))

(defn- read-value
  "read a key-value from DLedger"
  [client key]
  (-> client :conn
      (.read key @read-mode)))

(defn db
  "Regitser-Mode DLedger Server"
  []
  (reify db/DB
    (setup! [_ test node]
      (start! test node)
      (Thread/sleep 10000))

    (teardown! [_ _ node]
      (stop! node)
      (Thread/sleep 10000)
      (c/exec
       :rm
       :-rf
       dledger-data-path))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (-> this (assoc :node node) (assoc :conn (create-client test))))

  (setup! [_ _])

  (invoke! [this _ op]
    (let [[k v] (:value op)]
      (try
        (case (:f op)
          :write (let [code, (.getCode (write-value this k v))]
                   (cond
                     (= code 200) (assoc op :type :ok)
                     :else (assoc op :type :fail :error (str "write failed with code " code))))

          :read (let [res, (read-value this k)]
                  (cond
                    (= (.getCode res) 200) (assoc op :type :ok :value (independent/tuple k (.getValue res)))
                    :else (assoc op :type :fail :error (str "read failed with code " (.getCode res))))))

        (catch Exception e
          (assoc op :type :info :error e)))))

  (teardown! [_ _])

  (close! [this _]
    (shutdown-client this)))


(def nemesis-map
  {"partition-random-halves" (nemesis/partition-random-halves)
   "partition-random-nodes" (nemesis/partition-random-node)
   "partition-majorities-ring" (nemesis/partition-majorities-ring)})

(defn- parse-read-mode [read-mode-str]
  (ReadMode/valueOf read-mode-str))

(defn- parse-int [s]
  (Integer/parseInt s))
(defn- parse-boolean [b]
  (Boolean/parseBoolean b))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(def cli-opts
  "Additional command line options."
  [["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--nemesis NAME" "What nemesis should we run?"
    :default  "partition-random-halves"
    :validate [nemesis-map (cli/one-of nemesis-map)]]
   ["-i" "--interval TIME" "How long is the nemesis interval?"
    :default  15
    :parse-fn parse-int
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-int
    :validate [pos? "Must be a positive integer."]]
   ["-u" "--user USER" "SSH login user name."
    :default "root"]
   ["-p" "--passwd PWD" "SSH login user password."
    :default "passwd"]
   ["-m" "--read-mode MODE" "Read mode of DLedger."
    :default "RAFT_LOG_READ"]
   [nil "--snapshot BOOL" "Whether to enbale snapshot mode."
    :default false
    :parse-fn parse-boolean]
   [nil "--snapshot-threshold NUM" "Snapshot threshold."
    :default 1000
    :parse-fn parse-int]])

(defn dledger-test
  [opts]
  (reset! read-mode (:read-mode opts))
  (reset! enbale-snapshot (:snapshot opts))
  (reset! snapshot-threshold (:snapshot-threshold opts))
  (let [nemesis (get nemesis-map (:nemesis opts))]
    (merge tests/noop-test
           opts
           {:name      "dledger"
            :os        os/noop
            :db        (db)
            :client    (Client. nil)
            :ssh       {:username (:user opts) :password (:passwd opts) :strict-host-key-checking false}
            :nemesis   nemesis
            :checker   (checker/compose
                        {:perf (checker/perf)
                         :indep (independent/checker
                                 (checker/compose
                                  {:linear (checker/linearizable
                                            {:model (model/cas-register)})
                                   :timeline (timeline/html)}))})
            :generator  (->> (independent/concurrent-generator
                              (:concurrency opts 5)
                              (range)
                              (fn [_]
                                (->> (gen/mix [r w])
                                     (gen/stagger (/ (:rate opts)))
                                     (gen/limit (:ops opts)))))
                             (gen/nemesis
                              (gen/seq (cycle [(gen/sleep (:interval opts))
                                               {:type :info, :f :start}
                                               (gen/sleep (:interval opts))
                                               {:type :info, :f :stop}])))
                             (gen/time-limit (:time-limit opts)))})))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
      browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn dledger-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))