(ns kda-sim.awslogs
  (:use [clojure.java.shell])
  (:require [clojure.tools.logging :as log]
            [tea-time.core :as tt]
            [kda-sim.common :as common]
            [cheshire.core :as json]))

(defn logs-describe-log-groups
  [log-group-name-prefix]
  (let [r (:out (sh "aws" "logs" "describe-log-groups" "--log-group-name-prefix" log-group-name-prefix))]
    (json/parse-string r true)))

(defn logs-describe-log-streams
  [log-group-name]
  (let [r (:out (sh "aws" "logs" "describe-log-streams" "--log-group-name" log-group-name))]
    (json/parse-string r true)))



(defn logs-get-logs
  ([log-group-name log-stream-name]
   (let [start-time (- (System/currentTimeMillis) (* 60 3 1000))
         r (:out (sh "aws" "logs" "get-log-events" "--log-group-name" log-group-name "--log-stream-name" log-stream-name "--start-time" (str start-time)))]
     (println "From: " (common/to-human-readable-time start-time))
     (json/parse-string r true)))
  ([log-group-name log-stream-name next-token]
   (if (nil? next-token)
     (logs-get-logs log-group-name log-stream-name)
     (let [r (:out (sh "aws" "logs" "get-log-events" "--log-group-name" log-group-name "--log-stream-name" log-stream-name "--next-token" next-token))]
       (json/parse-string r true)))))

(defn print-log-events[events]
  (loop [l events]
    (if (empty? l)
      "done"
      (let [event (first l)]
        (print  (common/to-human-readable-time (:timestamp event)))
        (print " [")
        (print  (- (:ingestionTime event) (:timestamp event)))
        (print "] ")
        (println (:message event))
        (recur (rest l))))))

(def aws-logs-token (atom nil))

(defn show-last-logs[log-group-name log-stream-name]
  (let [r (logs-get-logs log-group-name log-stream-name @aws-logs-token)
        events (:events r)
        token (:nextForwardToken r)]
    (print-log-events events)
    (reset! aws-logs-token token)
    (log/info @aws-logs-token)))

(def log-task (atom nil))

(defn start-log![log-group-name log-stream-name]
  (tt/start!)
  (reset! log-task (tt/every! 60 0 (bound-fn [] (show-last-logs log-group-name log-stream-name))))
  )

(defn stop-log![]
  (tt/cancel! @log-task)
  (tt/stop!)
  (reset! log-task nil)
  (reset! aws-logs-token nil))