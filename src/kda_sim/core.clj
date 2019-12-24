(ns kda-sim.core
  (:use [clojure.java.shell])
  (:require [kda-sim.aws :as aws]
            [kda-sim.awslogs :as awslogs]
            [cheshire.core :as json]))

(def help-doc "Options: kinesis, log")


(defn -main
  [command & args]
  (case command
    "kinesis" (if (nil? (first args))
                (do
                  (println "Running kinesis consumer on default stream i.e ds-inventory-raw")
                  (aws/run-kinesis-consumer "ds-inventory-raw"))
                (do
                  (println "Running kinesis consumer on: " (first args))
                  (aws/run-kinesis-consumer (first args))))
    "log" (let [first-arg (first args)
                second-arg (second args)
                log-group-name (or first-arg "/ds/kda")
                log-stream-name (or second-arg "analytics")]
            (awslogs/start-log log-group-name log-stream-name))
    "info" (do
             (awslogs/logs-describe-log-groups "/ds/kda")
             (awslogs/logs-describe-log-streams "/ds/kda")
             (shutdown-agents))
    (println help-doc))
  (println "*** DONE ***"))