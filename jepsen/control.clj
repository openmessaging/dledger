(defcluster :dledger
  :clients [{:host "n0" :user "root"}
            {:host "n1" :user "root"}
            {:host "n2" :user "root"}
            {:host "n3" :user "root"}
            {:host "n4" :user "root"}])

(deftask :date "echo date on cluster"  []
  (ssh "date"))

(deftask :deploy []
  (ssh
   (run
    (cd "~"
        (run "rm -rf jepsen/")
        (run "rm -rf dledger-jepsen.tar.gz"))))
  (scp "dledger-jepsen.tar.gz" "~/")
  (ssh
   (run
    (cd "~"
        (run "tar zxvf dledger-jepsen.tar.gz")
        (run "rm -rf dledger-jepsen.tar.gz")))))