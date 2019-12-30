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
  (let [op (convert-status (:status raw))]
    (-> raw
        (assoc :created (System/currentTimeMillis))
        (update :id str)
        (assoc :type "actor")
        ;(update :status convert-status)
        (dissoc :status)
        (assoc :op op)
        (assoc :eventType op))))


(defn transform-to-kinesis-records [v-state]
  (let [yin (:yin v-state) yang (:yang v-state)]
    (cond-> []
            (> (:status yin) 0) (conj (aws/transform-to-kinesis-record (transform yin)))
            (> (:status yang) 0) (conj (aws/transform-to-kinesis-record (transform yang)))
            true (json/generate-string))))



(defn change-and-send-v! [stream-name]
  (swap! v change-v)
  (println @v)
  (aws/kinesis-put-records stream-name (transform-to-kinesis-records @v)))

(defn start-v![stream-name interval]
  (reset! v {:yin {:id 0 :status 0} :yang {:id 1 :status -2}})
  (tt/start!)
  (reset! v-task (tt/every! interval 0 (bound-fn [] (change-and-send-v! stream-name)))))

(defn stop-v![]
  (tt/cancel! @v-task)
  (tt/stop!)
  (reset! v-task nil))