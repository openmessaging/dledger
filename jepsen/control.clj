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


(defcluster :dledger-node
  :clients [{:host "n0" :user "root"}
            {:host "n1" :user "root"}
            {:host "n2" :user "root"}
            {:host "n3" :user "root"}
            {:host "n4" :user "root"}])

(defcluster :dledger-control
  :clients [{:host "n5" :user "root"}])

(deftask :date "echo date on cluster"  []
  (ssh "date"))

(deftask :node-deploy []
  (ssh
   (run
    (cd "~"
        (run "rm -rf jepsen/")
        (run "rm -rf dledger-jepsen-node.tar.gz"))))
  (scp "dledger-jepsen-node.tar.gz" "~/")
  (ssh
   (run
    (cd "~"
        (run "tar zxvf dledger-jepsen-node.tar.gz")
        (run "rm -rf dledger-jepsen-node.tar.gz")))))

(deftask :control-deploy []
  (ssh
   (run
    (cd "~"
        (run "rm -rf jepsen/")
        (run "rm -rf dledger-jepsen-control.tar.gz"))))
  (scp "dledger-jepsen-control.tar.gz" "~/")
  (ssh
   (run
    (cd "~"
        (run "tar zxvf dledger-jepsen-control.tar.gz")
        (run "rm -rf dledger-jepsen-control.tar.gz")))))