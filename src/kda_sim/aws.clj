(ns kda-sim.aws
  (:use [clojure.java.shell])
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (java.time LocalDate LocalDateTime ZoneOffset)))

(defn default-kinesis-listener[]
  (reify jungfly.aws.KinesisListener
    (listen [this shardId key seq data]
      (log/info "shard:" shardId)
      (log/info "key:" key)
      (log/info "seq:" seq)
      (println data)
      )))

(defn run-kinesis-consumer
  ([stream-name]
   (run-kinesis-consumer stream-name "us-east-1" (default-kinesis-listener)))
  ([stream-name region]
   (run-kinesis-consumer stream-name region (default-kinesis-listener)))
  ([stream-name region kinesis-listener]
   (let [kc (jungfly.aws.KinesisConsumer. stream-name region kinesis-listener)]
    (.run kc))))

(defn kinesis-put-record
  ([stream-name json-data]
   (kinesis-put-record stream-name "0" json-data))
  ([stream-name key json-data]
   (let [r (:out (sh "aws" "kinesis" "put-record" "--stream-name" stream-name "--partition-key" key "--data" json-data))]
     (json/parse-string r true))))


(defn kinesis-describe-stream [stream-name]
  (let [r (:out (sh "aws" "kinesis" "describe-stream" "--stream-name" stream-name))]
    (json/parse-string r true)))

(defn logs-describe-log-groups
  [log-group-name-prefix]
  (let [r (:out (sh "aws" "logs" "describe-log-groups" "--log-group-name-prefix" log-group-name-prefix))]
    (json/parse-string r true)))

(defn logs-describe-log-streams
  [log-group-name]
  (let [r (:out (sh "aws" "logs" "describe-log-streams" "--log-group-name" log-group-name))]
    (json/parse-string r true)))

(def formatter (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))

(defn to-human-readable-time [epoch-milli]
  (let [inst  (java.time.Instant/ofEpochMilli epoch-milli)
        time (java.time.LocalDateTime/ofInstant inst (java.time.ZoneId/systemDefault))]
    (.format time formatter)))

(defn logs-get-logs
  ([log-group-name log-stream-name]
   (let [start-time (- (System/currentTimeMillis) (* 60 600 1000))
         r (:out (sh "aws" "logs" "get-log-events" "--log-group-name" log-group-name "--log-stream-name" log-stream-name "--start-time" (str start-time)))]
     (println "From: " (to-human-readable-time start-time))
     (json/parse-string r true)))
  ([log-group-name log-stream-name next-token]
   (let [r (:out (sh "aws" "logs" "get-log-events" "--log-group-name" log-group-name "--log-stream-name" log-stream-name "--next-token" next-token))]
     (json/parse-string r true)))
  )

(defn show-logs[]
  (let [log-group-name "/ds/kda"
        log-stream-name "analytics"
       ]
    (let [events (:events (logs-get-logs log-group-name log-stream-name))]
      (loop [l events]
        (if (empty? l)
          "done"
          (let [event (first l)]
            (print  (to-human-readable-time (:timestamp event)))
            (print " [")
            (print  (- (:ingestionTime event) (:timestamp event)))
            (print "] ")
            (println (:message event))
            (recur (rest l))))))))