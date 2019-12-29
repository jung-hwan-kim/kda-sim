(ns kda-sim.v
  (:use [clojure.java.shell])
  (:require [kda-sim.aws :as aws]
            [tea-time.core :as tt]
            [kda-sim.common :as common]
            [clojure.tools.logging :as log]
            [cheshire.core :as json]))

(defn gen-v[id status]
  (json/generate-string {:id id :status status :date (common/to-human-readable-time (System/currentTimeMillis))}))

(def v-task (atom nil))
(def v (atom {:yin {:id 0 :status 0} :yang {:id 1 :status -2}}))

(defn inc2 [x]
  (+ x 2))

(defn inc-status[value]
  (-> value
      (update-in [:yin :status] inc)
      (update-in [:yang :status] inc)))

(defn rebirth[value yin?]
  (if yin?
    (-> value
        (assoc-in [:yin :status] 0)
        (update-in [:yin :id] inc2))
    (-> value
        (assoc-in [:yang :status] 0)
        (update-in [:yang :id] inc2))))

(defn change-v[value]
  (cond-> (inc-status value)
          (> (get-in value [:yin :status]) 2) (rebirth true)
          (> (get-in value [:yang :status]) 2) (rebirth false)))

(defn convert-status [status-int]
  (case status-int
    1 "add"
    2 "update"
    3 "remove"
    "unknown"))
(defn transform [raw]
  (-> raw
      (assoc :created (System/currentTimeMillis))
      (update :id str)
      ;(update :status convert-status)
      (dissoc :status)
      (assoc :eventType (convert-status (:status raw)))
      ))

(defn -to-kinesis-record [record]
  (let [r (transform record)]
    (-> {}
      (assoc :Data (json/generate-string r))
      (assoc :PartitionKey (:id r)))))

(defn to-kinesis-records [v-state]
  (let [yin (:yin v-state) yang (:yang v-state)]
    (cond-> []
            (> (:status yin) 0) (conj (-to-kinesis-record yin))
            (> (:status yang) 0) (conj (-to-kinesis-record yang))
            true (json/generate-string))))

(defn kinesis-put-records
  ([stream-name data]
   (let [r (sh "aws" "kinesis" "put-records" "--stream-name" stream-name "--records" data)]
     ;(println data)
     (if (= (:exit r) 0)
       (json/parse-string (:out r) true)
       (do (println data)
           r)))))

(defn change-and-send-v! [stream-name]
  (swap! v change-v)
  (log/info (kinesis-put-records stream-name (to-kinesis-records @v)))
  (println @v))

(defn start-v![stream-name interval]
  (reset! v {:yin {:id 0 :status 0} :yang {:id 1 :status -2}})
  (tt/start!)
  (reset! v-task (tt/every! interval 0 (bound-fn [] (change-and-send-v! stream-name)))))

(defn stop-v![]
  (tt/cancel! @v-task)
  (tt/stop!)
  (reset! v-task nil))