(ns kda-sim.dedup-metric
  (:require [taoensso.nippy :as nippy]
            [cheshire.core :as json])
  (:import (java.time OffsetDateTime)))

(def state-map (atom {}))
(defn get-state[vehicleId]
  (get @state-map vehicleId))
(defn update-state[vehicleId new-state]
  (swap! state-map #(assoc % vehicleId new-state)))

(def duplicate-count (atom 0))
(def deduped-count (atom 0))

(def max-dupgap (atom 0))
(def total-dupgap (atom 0))

(defn dedup[event]
  (let [vehicleId (:vehicleId event)
        state (get-state vehicleId)
        eventDate (:eventDate event)
        evented (.toEpochSecond (OffsetDateTime/parse eventDate))
        content-of-event (assoc (dissoc event :eventType :eventDate :ingested :id) :evented evented)]
    (if (= (dissoc state :evented) (dissoc content-of-event :evented))
      (let [orig-d (:evented state)
            new-d (:evented content-of-event)
            dupgap (- new-d orig-d)]
        (swap! duplicate-count inc)
        (swap! total-dupgap #(+ dupgap %))
        (if (> dupgap @max-dupgap)
          (reset! max-dupgap dupgap))
        ;(println "duplicate" vehicleId)
        )
      (do
        (swap! deduped-count inc)
        (update-state vehicleId content-of-event)
        ;(println "ok" vehicleId)
        ))))

(defn dedup-file[file-name batch-size]
  (let [jsons (json/parsed-seq (clojure.java.io/reader file-name) true)]
    (loop [list jsons total-count 0]
      (if (empty? list)
        total-count
        (let [working-list (take batch-size list)]
          ;(println (count working-list))
          (doseq [event working-list]
            (dedup event))
          (recur (drop batch-size list) (+ total-count (count working-list))))))))


(defn batchload-and-dedup[dir-path batch-size]
  (reset! state-map {})
  (reset! duplicate-count 0)
  (reset! deduped-count 0)
  (reset! max-dupgap 0)
  (reset! total-dupgap 0)

  (let [dir (clojure.java.io/file dir-path)]
    (loop [directories (sort (.listFiles dir))
           total 0]
      (if (empty? directories)
        (println "Processed: Total" total)
        (let [t2 (loop [files (sort (.listFiles (first directories)))
                        t 0]
                   (if (empty? files)
                     t
                     (let [f (first files)
                           t0 (dedup-file (.getPath f) batch-size)]
                       (println (.getName f) t0)
                       (recur (rest files) (+ t0 t)))))]
          (recur (rest directories) (+ t2 total))))))
  (println "vehicles:" (count @state-map))
  (println "duplicates:" @duplicate-count)
  (println "deduped:" @deduped-count)
  (println "max dupgap:" @max-dupgap)
  (println "total dupgap:" @total-dupgap)
  )


