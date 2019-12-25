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
      (log/info "data:" data)
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

