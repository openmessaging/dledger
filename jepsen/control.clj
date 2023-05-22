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