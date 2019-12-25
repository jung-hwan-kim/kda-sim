(ns kda-sim.common)

(def formatter (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))

(defn to-human-readable-time [epoch-milli]
  (let [inst  (java.time.Instant/ofEpochMilli epoch-milli)
        time (java.time.LocalDateTime/ofInstant inst (java.time.ZoneId/systemDefault))]
    (.format time formatter)))
