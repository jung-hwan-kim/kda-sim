(ns kda-sim.main
  (:use [clojure.java.shell])
  (:require [kda-sim.aws :as aws]
            [kda-sim.v :as v]
            [kda-sim.event :as event]
            [kda-sim.awslogs :as awslogs]
            [datascript.core :as d]
            [camel-snake-kebab.core :as csk]
            [taoensso.nippy :as nippy]
            [kda-sim.data-analysis :as da]
            [cheshire.core :as json])
  (:import (jungfly.aws EventtimestampParser)
           (java.util Base64)))

(def kinesis-local "ds-prototype-raw")
(def kinesis-remote "ds-inventory-raw")

(defn encode-base64str[edn]
  (.encodeToString (Base64/getEncoder) (nippy/freeze edn)))

(defn decode-edn [base64-str]
  (nippy/thaw (.decode (Base64/getDecoder) base64-str)))

(defn ednk-read-kstate [stream-name]
  (let [edn (encode-base64str {:eventtable "EDNK" :function '(fn [kstate-obj kstate bstate]
                                                               {:k kstate})})
        event {:EVENTTABLE "EDNK" :VEHICLE_ID "0" :edn edn}]
    (aws/kinesis-put stream-name [event])))

(defn ednk-clean-kstate [stream-name]
  (let [edn (encode-base64str {:eventtable "EDNK" :function '(fn [kstate-obj kstate bstate]
                                                               (.clean kstate-obj)
                                                               {:k kstate})})
        event {:EVENTTABLE "EDNK" :VEHICLE_ID "0" :edn edn}]
    (aws/kinesis-put stream-name [event])))

(defn ednk-memory [stream-name]
  (let [edn (encode-base64str {:eventtable "EDNK" :function '(fn [kstate-obj kstate bstate]
                                                               {:freeMem  (.freeMemory (Runtime/getRuntime))
                                                                :maxMem   (.maxMemory (Runtime/getRuntime))
                                                                :totalMem  (.totalMemory (Runtime/getRuntime))
                                                                })})
        event {:EVENTTABLE "EDNK" :VEHICLE_ID "0" :edn edn}]
    (aws/kinesis-put stream-name [event])))

(defn send-rule [stream-name rule-name rule-value]
  (let [data {:eventtable "rule" :id rule-name :value rule-value :op "update"
              :created (System/currentTimeMillis)
              :updated (System/currentTimeMillis)}]
    (aws/kinesis-put stream-name [data])))

(defn send-auction [stream-name]
  (let [event (-> (event/auction-update "0")
                  (assoc :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)))
        ]
    (aws/kinesis-put stream-name [event])))

(defn send-vehicle [stream-name]
  (let [event (-> (event/vehicle-update "0")
                  (assoc :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)))]
    (aws/kinesis-put stream-name [event])))

(defn send-vehicle-additional-info [stream-name]
  (let [event (-> (event/vehicle-addtional-info-update "0")
                  (assoc :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)))]
    (aws/kinesis-put stream-name [event])))

(defn send-picture [stream-name]
  (let [event (-> (event/picture-update "0")
                  (assoc :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)))]
    (aws/kinesis-put stream-name [event])))


(defn test-kinesis[stream-name]
  (aws/kinesis-put stream-name [{:vehicleid "123" :test 1 :dur 100}]))

(defn send-heartbeat-k[stream-name vehicleId debug action]
  (let [event {:EVENTTABLE "HEARTBEAT_K"
               :VEHICLE_ID vehicleId
               :DEBUG debug
               :ACTION action
               :EVENTTIMESTAMP (EventtimestampParser/generateEventtimestampString)
               }]
    (aws/kinesis-put stream-name [event])))


(defn readfile-and-kinesis[file-name stream-name batch-size]
  (let [jsons (json/parsed-seq (clojure.java.io/reader file-name) true)]
    (loop [list jsons]
      (if (empty? list)
        "done"
        (do
          (aws/kinesis-put stream-name (take batch-size list) #(:vehicleId %))
          ;(println (count (take batch-size list)))
          (recur (drop batch-size list)))))))


(defn batchload-dir-to-kinesis[dir-path stream-name batch-size]
  (let [dir-name "../../tmp/drivin-prod"
        dir (clojure.java.io/file dir-name)
        ]
    (doseq [d (sort (.listFiles dir))]
      (if (.isDirectory d)
        (doseq [f (sort (.listFiles d))]
          (println (.getName f))
          (readfile-and-kinesis (.getPath f) stream-name batch-size)
          )))))


(def help-doc "Options: kinesis, log, v
e.g.
lein run kinesis $stream-name
lein run log $log-group $log-stream
lein run v $type $stream-name $interval-in-sec
")
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
    "kinesis-aggr" (if (nil? (first args))
                (do
                  (println "Running kinesis consumer on default stream i.e ds-inventory-raw")
                  (aws/run-kinesis-aggr-consumer "ds-inventory-raw"))
                (do
                  (println "Running kinesis consumer on: " (first args))
                  (aws/run-kinesis-aggr-consumer (first args))))
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
    "batchload" (let [dir-name (first args)
                      stream-name (or (second args) "ds-prototype-master")
                      batch-size (Integer/valueOf (or (nth args 2) "100"))]
                  (time
                    (batchload-dir-to-kinesis dir-name  stream-name batch-size)))
    (println help-doc))
  (println "*** end of sim ***"))