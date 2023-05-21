(defproject openmessaging-dledger-jepsen "0.0.1-SNAPSHOT"
  :descritopn "A jepsen test for DLedger"
  :license {:name "Apache License 2.0"}
  :main io.openmessaging.storage.dledger.jepsen.core
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.1.15-SNAPSHOT"]
                 [io.openmessaging.storage/dledger-example "0.3.3-SNAPSHOT"]]
  :source-paths ["src" "src/main/clojure"])