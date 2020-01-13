(ns kda-sim.aws
  (:use [clojure.java.shell])
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (java.time LocalDate LocalDateTime ZoneOffset)
           (jungfly.aws KinesisProducer)))

(def producer (new KinesisProducer))
(def counter (atom 0))

(defn default-kinesis-listener[]
  (reify jungfly.aws.KinesisListener
    (listen [this shardId key seq data]
      ;(log/info "shard:" shardId)
      ;(log/info "key:" key)
      ;(log/info "seq:" seq)
      ;(log/info "data:" data)
      (println (swap! counter inc) ">" data)
      )
    (resetCount [this]
      (reset! counter 0)
      (println "resetted to zero"))
    (print [this]
      (println "Count=" @counter))
    ))

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

(defn kinesis-put-records
  ([stream-name data]
   (let [r (:out (sh "aws" "kinesis" "put-records" "--stream-name" stream-name "--data" data))]
     (json/parse-string r true))))


(defn kinesis-describe-stream [stream-name]
  (let [r (:out (sh "aws" "kinesis" "describe-stream" "--stream-name" stream-name))]
    (json/parse-string r true)))

(defn get-partitionkey[pkey record]
  (let [key (pkey record)]
    (if (nil? key)
      "0"
      (str key)
      )))

(defn transform-to-kinesis-record [pkey r]
    (-> {}
        (assoc :Data (json/generate-string r))
        (assoc :PartitionKey (get-partitionkey pkey r))))

(defn transform-to-kinesis-records [pkey vector]
  (json/generate-string (vec (map #(transform-to-kinesis-record pkey %) vector))))

(defn kinesis-put-records
  ([stream-name string-data]
   (let [r (sh "aws" "kinesis" "put-records" "--stream-name" stream-name "--records" string-data)]
     (println string-data)
     (if (= (:exit r) 0)
       (json/parse-string (:out r) true)
       (do
         (println string-data)
         r)))))


(defn transform-kineisis-data [vector pkey-fn]
  (let [t-fn (fn[r] {:Data (json/generate-string r) :PartitionKey (pkey-fn r)})]
    (map t-fn vector)
    ))

(defn kinesis-put
  ([stream-name vector pkey-fn]
  (let [data (vec (transform-kineisis-data vector pkey-fn))]
    (println data)
    (.putRecords producer stream-name data)))
  ([stream-name vector]
   (kinesis-put stream-name vector #(:vehicleid %))))

(defn -kinesis-put
  ([stream-name pkey vector]
   (let [data (transform-to-kinesis-records pkey vector)
         r (sh "aws" "kinesis" "put-records" "--stream-name" stream-name "--records" data)]
     (println data)
     (if (= (:exit r) 0)
       (json/parse-string (:out r) true)
       (do
         (println data)
         r)))))
