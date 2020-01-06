(ns kda-sim.EventtimestampParser-test
  (:require [clojure.test :refer :all])
  (:import (jungfly.aws EventtimestampParser)))
(deftest a-test
  (testing "main"
    (let [epoch1 (System/currentTimeMillis)
          t-str (EventtimestampParser/generateEventtimestampString)
          epoch2 (EventtimestampParser/toEpochMillis t-str)
          ]
      (is (< (- epoch2 epoch1) 100)))))

