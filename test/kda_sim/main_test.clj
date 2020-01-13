(ns kda-sim.main-test
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all :as helpers]
            [kda-sim.main :refer :all]))

(deftest a-test
  (testing "main"
    (is (= 0 1))))

