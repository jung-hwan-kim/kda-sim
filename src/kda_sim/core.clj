(ns kda-sim.core
  (:use [clojure.java.shell])
  (:require [kda-sim.aws :as aws]
            [cheshire.core :as json]))




(defn -main
  "kda sim main"
  []
  (aws/run-kinesis-consumer "ds-inventory-raw")

  (println "*** DONE ***")
  (shutdown-agents)
  )
