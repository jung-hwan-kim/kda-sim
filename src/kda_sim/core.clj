(ns kda-sim.core
  (:use [clojure.java.shell])
  (:require [cheshire.core :as json]))

(defn -main
  "kda sim main"
  []
  (println "kda-sim main")

  (println "*** DONE ***")
  (shutdown-agents)
  )
