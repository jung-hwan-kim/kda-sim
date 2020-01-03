(ns kda-sim.core
  (:use [clojure.java.shell])
  (:require [kda-sim.aws :as aws]
            [kda-sim.v :as v]
            [kda-sim.awslogs :as awslogs]
            [cheshire.core :as json]))

(def default-kinesis "ds-prototype-raw")
(defn send-rule [stream-name rule-name rule-value]
  (let [data {:eventtable "rule" :id rule-name :value rule-value :op "update"
              :created (System/currentTimeMillis)
              :updated (System/currentTimeMillis)}]
    (aws/kinesis-put stream-name [data])))


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
                log-group-name (or first-arg "/aws/kinesis-analytics/ds-kda")
                log-stream-name (or second-arg "kinesis-analytics-log-stream")]
            (awslogs/start-log! log-group-name log-stream-name))
    "v" (let [first-arg (first args)
              second-arg (second args)
              third-arg (Integer/parseInt (nth args 2))
              type-name (or first-arg "actor")
              stream-name (or second-arg "ds-inventory-raw")]
          (v/start-v! type-name stream-name (or third-arg 60)))
    "info" (do
             (awslogs/logs-describe-log-groups "/ds/kda")
             (awslogs/logs-describe-log-streams "/ds/kda")
             (shutdown-agents))
    (println help-doc))
  (println "*** DONE ***"))